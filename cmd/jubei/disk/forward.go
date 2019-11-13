package disk

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"sync/atomic"

	"github.com/rekki/blackrock/pkg/depths"
	log "github.com/sirupsen/logrus"
)

const PAD = 64

type ForwardWriter struct {
	forward *os.File
	buffer  []byte
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

var MAGIC = []byte{0xb, 0xe, 0xe, 0xf}

func (fw *ForwardWriter) Read(offset uint32) ([]byte, uint32, error) {
	header := make([]byte, 16)
	_, err := fw.forward.ReadAt(header, int64(offset*PAD))
	if err != nil {
		return nil, 0, err
	}

	if !bytes.Equal(header[8:12], MAGIC) {
		return nil, 0, EBADSLT
	}

	computedChecksumHeader := uint32(depths.Hash(header[:12]))
	checksumHeader := binary.LittleEndian.Uint32(header[12:])
	if checksumHeader != computedChecksumHeader {
		return nil, 0, EBADSLT
	}

	metadataLen := binary.LittleEndian.Uint32(header)
	nextOffset := (offset + ((uint32(len(header))+(uint32(metadataLen)))+PAD-1)/PAD)
	readInto := make([]byte, metadataLen)
	_, err = fw.forward.ReadAt(readInto, int64(offset*PAD)+int64(len(header)))
	if err != nil {
		return nil, 0, err
	}
	checksumHeaderData := binary.LittleEndian.Uint32(header[4:])
	computedChecksumData := uint32(depths.Hash(readInto))

	if checksumHeaderData != computedChecksumData {
		return nil, 0, EBADSLT
	}
	return readInto, nextOffset, nil
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
	blobSize := 16 + len(encoded)
	blob := make([]byte, blobSize)
	copy(blob[16:], encoded)
	binary.LittleEndian.PutUint32(blob[0:], uint32(len(encoded)))
	binary.LittleEndian.PutUint32(blob[4:], uint32(depths.Hash(encoded)))
	copy(blob[8:], MAGIC)
	binary.LittleEndian.PutUint32(blob[12:], uint32(depths.Hash(blob[:12])))

	padded := ((uint32(blobSize) + PAD - 1) / PAD)

	current := atomic.AddUint32(&fw.offset, padded)
	current -= uint32(padded)

	_, err := fw.forward.WriteAt(blob, int64(current*PAD))
	if err != nil {
		return 0, err
	}
	return uint32(current), nil
}
