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

func (fw *ForwardWriter) Scan(offset uint64, readData bool, cb func(uint64, uint64, uint64, []byte) error) error {
	for {
		id, ftype, data, next, err := fw.Read(offset, readData)
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
		err = cb(offset, id, ftype, data)
		if err != nil {
			return err
		}
		offset = next
	}
}

func (fw *ForwardWriter) Read(offset uint64, readData bool) (uint64, uint64, []byte, uint64, error) {
	header := make([]byte, 28)
	_, err := fw.forward.ReadAt(header, int64(offset))
	if err != nil {
		return 0, 0, nil, 0, err
	}

	id := binary.LittleEndian.Uint64(header[0:])
	ftype := binary.LittleEndian.Uint64(header[8:])
	metadataLen := binary.LittleEndian.Uint32(header[16:])
	nextOffset := offset + uint64(len(header)) + (uint64(metadataLen))

	checksumHeader := binary.LittleEndian.Uint32(header[20:])
	computedChecksumHeader := uint32(depths.Hash(header[:20]))

	if checksumHeader != computedChecksumHeader {
		return 0, 0, nil, 0, EBADSLT
	}
	if !readData {
		return id, ftype, nil, nextOffset, nil
	}

	readInto := make([]byte, metadataLen)
	_, err = fw.forward.ReadAt(readInto, int64(offset)+int64(len(header)))
	if err != nil {
		return 0, 0, nil, 0, err
	}
	checksumData := binary.LittleEndian.Uint32(header[24:])
	computedChecksumData := uint32(depths.Hash(readInto))
	if checksumData != computedChecksumData {
		return 0, 0, nil, 0, EBADSLT
	}
	return id, ftype, readInto, nextOffset, nil
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

func (fw *ForwardWriter) Sync() {
	fw.forward.Sync()
}

func (fw *ForwardWriter) Offset() uint64 {
	return fw.offset
}

func (fw *ForwardWriter) Append(id, t uint64, encoded []byte) (uint64, error) {
	// id, len, checksum of the header, checksum of the data
	blobSize := 8 + 8 + 4 + 4 + 4 + len(encoded)
	blob := make([]byte, blobSize)
	copy(blob[28:], encoded)
	binary.LittleEndian.PutUint64(blob[0:], id)
	binary.LittleEndian.PutUint64(blob[8:], t)
	binary.LittleEndian.PutUint32(blob[16:], uint32(len(encoded)))
	binary.LittleEndian.PutUint32(blob[20:], uint32(depths.Hash(blob[:20])))
	binary.LittleEndian.PutUint32(blob[24:], uint32(depths.Hash(encoded)))
	current := atomic.AddUint64(&fw.offset, uint64(blobSize))
	current -= uint64(blobSize)

	_, err := fw.forward.WriteAt(blob, int64(current))
	if err != nil {
		return 0, err
	}

	return current, nil
}
