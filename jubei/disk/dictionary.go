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

	"github.com/jackdoe/blackrock/depths"
	log "github.com/sirupsen/logrus"
)

type PersistedDictionary struct {
	fd       *os.File
	offset   uint64
	resolved map[string]uint64
	reverse  map[uint64]string
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
	reverse := map[uint64]string{}
	for k, v := range resolved {
		reverse[v] = k
	}
	log.Infof("dictionary %s with %d size and %d elements", filename, off, len(reverse))
	return &PersistedDictionary{
		fd:       fd,
		offset:   uint64(off),
		resolved: resolved,
		reverse:  reverse,
	}, nil
}

func ParseFile(fd *os.File, off uint64) (uint64, map[string]uint64, error) {
	out := map[string]uint64{}

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
		out[string(data)] = uint64(off)
		off += 8 + uint64(length)
	}
	return off, out, nil
}

func (pd *PersistedDictionary) normalize(s string) string {
	s = strings.ToLower(s)
	s = strings.Trim(s, " ")
	return s
}

func (pd *PersistedDictionary) GetUniqueTerm(s string) (uint64, error) {
	pd.RLock()
	s = pd.normalize(s)
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

	off := atomic.AddUint64(&pd.offset, uint64(len(data)))
	off -= uint64(len(data))

	_, err := pd.fd.WriteAt(data, int64(off))
	if err != nil {
		return 0, err
	}
	pd.resolved[s] = uint64(off)
	pd.reverse[uint64(off)] = s
	return off, nil
}

func (pd *PersistedDictionary) Resolve(s string) (uint64, bool) {
	s = pd.normalize(s)
	v, ok := pd.resolved[s]
	return v, ok
}

func (pd *PersistedDictionary) Close() {
	pd.fd.Close()
}
func (pd *PersistedDictionary) ReverseResolve(s uint64) string {
	v, ok := pd.reverse[s]
	if ok {
		return v
	}

	return fmt.Sprintf("%d_not_found", s)
}
