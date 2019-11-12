package main

import (
	"encoding/binary"
	"fmt"
	"os"

	"github.com/rekki/blackrock/pkg/depths"
	log "github.com/sirupsen/logrus"
)

type OffsetWriter struct {
	fd *os.File
	l  *log.Entry
}

func NewOffsetWriter(fn string, l *log.Entry) (*OffsetWriter, error) {
	state, err := os.OpenFile(fn, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}

	return &OffsetWriter{
		fd: state,
		l:  l.WithField("OffsetWriter", true),
	}, nil
}

func (ow *OffsetWriter) Close() {
	ow.fd.Close()
}

func (ow *OffsetWriter) ReadOrDefault(def int64) (int64, error) {
	storedOffset := make([]byte, 16)
	var offset int64
	_, err := ow.fd.ReadAt(storedOffset, 0)
	if err != nil {
		ow.l.WithError(err).Warnf("failed to read bytes, setting offset to %d, err: %s", def, err)
		offset = def
	} else {
		offset = int64(binary.LittleEndian.Uint64(storedOffset))
		sum := binary.LittleEndian.Uint64(storedOffset[8:])
		expected := depths.Hash(storedOffset[:8])
		if expected != sum {
			return 0, fmt.Errorf("bad checksum expected: %d, got %d, bytes: %v", expected, sum, storedOffset)
		}
	}
	return offset, nil
}

func (ow *OffsetWriter) SetOffset(offset int64) error {
	storedOffset := make([]byte, 16)
	binary.LittleEndian.PutUint64(storedOffset[0:], uint64(offset))
	offsetBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(offsetBytes, uint64(offset))
	binary.LittleEndian.PutUint64(storedOffset[8:], depths.Hash(offsetBytes))
	_, err := ow.fd.WriteAt(storedOffset, 0)
	return err
}
