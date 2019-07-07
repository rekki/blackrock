package disk

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"sync/atomic"

	"github.com/jackdoe/blackrock/depths"
	log "github.com/sirupsen/logrus"
)

type ForwardWriter struct {
	forward *os.File
	offset  uint64
}

func NewForwardWriter(root string, name string) (*ForwardWriter, error) {
	filename := path.Join(root, fmt.Sprintf("%s.bin", name))
	fd, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return nil, err
	}

	off, err := fd.Seek(0, os.SEEK_END)
	if err != nil {
		return nil, err
	}

	log.Infof("forward %s with %d size", filename, off)
	return &ForwardWriter{
		forward: fd,
		offset:  uint64(off),
	}, nil
}

var EBADSLT = errors.New("checksum mismatch")

func (fw *ForwardWriter) Scan(offset uint64, readData bool, cb func(uint64, uint64, []byte) error) error {
	for {
		maker, data, next, err := fw.Read(offset, readData)
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
		err = cb(offset, maker, data)
		if err != nil {
			return err
		}
		offset = next
	}
}

func (fw *ForwardWriter) Read(offset uint64, readData bool) (uint64, []byte, uint64, error) {
	header := make([]byte, 20)
	_, err := fw.forward.ReadAt(header, int64(offset))
	if err != nil {
		return 0, nil, 0, err
	}

	maker := binary.LittleEndian.Uint64(header[0:])
	metadataLen := binary.LittleEndian.Uint32(header[8:])
	nextOffset := offset + uint64(len(header)) + (uint64(metadataLen))

	checksumHeader := binary.LittleEndian.Uint32(header[12:])
	computedChecksumHeader := uint32(depths.Hash(header[:12]))

	if checksumHeader != computedChecksumHeader {
		return 0, nil, 0, EBADSLT
	}
	if !readData {
		return maker, nil, nextOffset, nil
	}

	readInto := make([]byte, metadataLen)
	_, err = fw.forward.ReadAt(readInto, int64(offset)+int64(len(header)))
	if err != nil {
		return 0, nil, 0, err
	}
	checksumData := binary.LittleEndian.Uint32(header[16:])
	computedChecksumData := uint32(depths.Hash(readInto))
	if checksumData != computedChecksumData {
		return 0, nil, 0, EBADSLT
	}
	return maker, readInto, nextOffset, nil
}

func (fw *ForwardWriter) Close() {
	fw.forward.Close()
}

func (fw *ForwardWriter) Size() (uint64, error) {
	s, err := fw.forward.Stat()
	if err != nil {
		return 0, err
	}
	return uint64(s.Size()), nil
}

func (fw *ForwardWriter) Append(maker uint64, encoded []byte) (uint64, error) {
	// maker, len, checksum of the header, checksum of the data
	blobSize := 8 + 4 + 4 + 4 + len(encoded)
	blob := make([]byte, blobSize)
	copy(blob[20:], encoded)
	binary.LittleEndian.PutUint64(blob[0:], maker)
	binary.LittleEndian.PutUint32(blob[8:], uint32(len(encoded)))
	binary.LittleEndian.PutUint32(blob[12:], uint32(depths.Hash(blob[:12])))
	binary.LittleEndian.PutUint32(blob[16:], uint32(depths.Hash(encoded)))
	current := atomic.AddUint64(&fw.offset, uint64(blobSize))
	current -= uint64(blobSize)

	_, err := fw.forward.WriteAt(blob, int64(current))
	if err != nil {
		return 0, err
	}

	return current, nil
}
