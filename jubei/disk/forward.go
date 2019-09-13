package disk

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"sync/atomic"

	"github.com/golang/snappy"
	"github.com/jackdoe/blackrock/depths"
	log "github.com/sirupsen/logrus"
)

const PAD = 64

type ForwardWriter struct {
	forward *os.File
	offset  uint32
}

func NewForwardWriter(root string, name string) (*ForwardWriter, error) {
	os.MkdirAll(root, 0700)
	filename := path.Join(root, fmt.Sprintf("%s.bin", name))
	fd, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return nil, err
	}

	off, err := fd.Seek(0, os.SEEK_END)
	if err != nil {
		return nil, err
	}
	log.Infof("forward %s with %d offset", filename, off)
	return &ForwardWriter{
		forward: fd,
		offset:  uint32((off + PAD - 1) / PAD),
	}, nil
}

var EBADSLT = errors.New("checksum mismatch")
var EBADRQC = errors.New("bad length")

func (fw *ForwardWriter) Scan(offset uint32, cb func(uint32, []byte) error) error {
	for {
		data, next, err := fw.Read(offset)
		if err == io.EOF {
			return nil
		}
		if err == EBADSLT {
			offset++
			continue
		}
		if err != nil {
			return err
		}
		err = cb(next, data)
		if err != nil {
			return err
		}
		offset = next
	}
}

func (fw *ForwardWriter) Read(offset uint32) ([]byte, uint32, error) {
	header := make([]byte, 8)
	_, err := fw.forward.ReadAt(header, int64(offset*PAD))
	if err != nil {
		return nil, 0, err
	}

	metadataLen := binary.LittleEndian.Uint32(header)
	nextOffset := (offset + ((uint32(len(header))+(uint32(metadataLen)))+PAD-1)/PAD)
	if metadataLen > 1024000 {
		return nil, 0, EBADRQC
	}
	readInto := make([]byte, metadataLen)
	_, err = fw.forward.ReadAt(readInto, int64(offset*PAD)+int64(len(header)))
	if err != nil {
		return nil, 0, err
	}
	checksumHeader := binary.LittleEndian.Uint32(header[4:])
	computedChecksumData := uint32(depths.Hash(readInto))

	if checksumHeader != computedChecksumData {
		return nil, 0, EBADSLT
	}
	decompressed, err := snappy.Decode(nil, readInto)
	if err != nil {
		return nil, 0, err
	}

	return decompressed, nextOffset, nil
}

func (fw *ForwardWriter) Close() error {
	return fw.forward.Close()
}

func (fw *ForwardWriter) Size() (uint32, error) {
	s, err := fw.forward.Stat()
	if err != nil {
		return 0, err
	}
	return uint32(s.Size()), nil
}

func (fw *ForwardWriter) Sync() error {
	return fw.forward.Sync()
}

func (fw *ForwardWriter) Offset() uint32 {
	return fw.offset
}

func (fw *ForwardWriter) Append(encoded []byte) (uint32, error) {
	compressed := snappy.Encode(nil, encoded)
	blobSize := 8 + len(compressed)
	blob := make([]byte, blobSize)
	copy(blob[8:], compressed)
	binary.LittleEndian.PutUint32(blob[0:], uint32(len(compressed)))
	binary.LittleEndian.PutUint32(blob[4:], uint32(depths.Hash(compressed)))

	padded := ((uint32(blobSize) + PAD - 1) / PAD)

	current := atomic.AddUint32(&fw.offset, padded)

	current -= uint32(padded)
	_, err := fw.forward.WriteAt(blob, int64(current*PAD))
	if err != nil {
		return 0, err
	}
	return uint32(current), nil
}
