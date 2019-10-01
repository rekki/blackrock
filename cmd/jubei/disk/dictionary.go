package disk

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/rekki/blackrock/pkg/depths"
	log "github.com/sirupsen/logrus"
)

type PersistedDictionary struct {
	fd       *os.File
	offset   uint32
	resolved map[string]uint32
	reverse  map[uint32]string
	sync.RWMutex
}

func NewPersistedDictionary(root string) (*PersistedDictionary, error) {
	filename := path.Join(root, "dictionary.bin")
	fd, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return nil, err
	}
	off, resolved, err := ParseFile(fd, 0)
	if err != nil {
		return nil, err
	}
	reverse := map[uint32]string{}
	for k, v := range resolved {
		reverse[v] = k
	}
	log.Infof("dictionary %s with %d size and %d elements", filename, off, len(reverse))
	return &PersistedDictionary{
		fd:       fd,
		offset:   uint32(off),
		resolved: resolved,
		reverse:  reverse,
	}, nil
}

func ParseFile(fd *os.File, off uint32) (uint32, map[string]uint32, error) {
	out := map[string]uint32{}

	for {
		header := make([]byte, 8)
		// length, data, checksum
		_, err := fd.ReadAt(header, int64(off))
		if err == io.EOF {
			break
		}

		if err != nil {
			return 0, nil, err
		}

		length := binary.LittleEndian.Uint32(header[0:])
		checksum := binary.LittleEndian.Uint32(header[4:])
		if length > 10240 {
			return 0, nil, fmt.Errorf("length too big: %d", length)
		}
		data := make([]byte, length)
		_, err = fd.ReadAt(data, int64(off+8))
		if err == io.EOF {
			break
		}

		if err != nil {
			return 0, nil, err
		}

		hash := depths.Hash(data)
		if checksum != uint32(hash) {
			return 0, nil, fmt.Errorf("checksum mismatch, got %d expected %d", hash, checksum)
		}
		out[string(data)] = uint32(off)
		off += 8 + uint32(length)
	}
	return off, out, nil
}

func (pd *PersistedDictionary) normalize(s string) string {
	s = strings.ToLower(s)
	s = strings.Trim(s, " ")
	return s
}

func (pd *PersistedDictionary) GetUniqueTerm(s string) (uint32, error) {
	// this is used by jubei in 2 threads

	s = pd.normalize(s)
	pd.RLock()
	v, ok := pd.resolved[s]
	if ok {
		pd.RUnlock()
		return v, nil
	}
	pd.RUnlock()

	pd.Lock()
	v, ok = pd.resolved[s]
	if ok {
		pd.Unlock()
		return v, nil
	}
	defer pd.Unlock()

	b := []byte(s)

	// length, checksum, data
	data := make([]byte, len(b)+4+4)
	binary.LittleEndian.PutUint32(data[0:], uint32(len(b)))

	hash := depths.Hash(b)
	binary.LittleEndian.PutUint32(data[4:], uint32(hash))

	copy(data[8:], b)

	off := atomic.AddUint32(&pd.offset, uint32(len(data)))
	off -= uint32(len(data))

	_, err := pd.fd.WriteAt(data, int64(off))
	if err != nil {
		return 0, err
	}
	pd.resolved[s] = uint32(off)
	pd.reverse[uint32(off)] = s
	return off, nil
}

func (pd *PersistedDictionary) Resolve(s string) (uint32, bool) {
	// used in readonly more from khanzo, so no locking needed
	s = pd.normalize(s)
	v, ok := pd.resolved[s]
	return v, ok
}

func (pd *PersistedDictionary) Close() {
	pd.fd.Close()
}

func (pd *PersistedDictionary) ReverseResolve(s uint32) string {
	// used in readonly more from khanzo, so no locking needed
	v, ok := pd.reverse[s]
	if ok {
		return v
	}

	return fmt.Sprintf("%d_not_found", s)
}
