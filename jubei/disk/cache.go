package disk

import (
	"os"
	"sync"
)

type CachedFile struct {
	v    map[int64][]byte
	file *os.File
	sync.RWMutex
	size int
}

func NewCachedFile(filename string, size int) (*CachedFile, error) {
	forward, err := os.OpenFile(filename, os.O_RDONLY|os.O_CREATE, 0600)
	if err != nil {
		return nil, err
	}

	return &CachedFile{size: size, v: map[int64][]byte{}, file: forward}, nil
}

func (c CachedFile) Size() int {
	c.RLock()
	defer c.RUnlock()
	return len(c.v)
}

func (c CachedFile) Get(off int64, size int64) ([]byte, error) {
	// quick hack because amazon EBS is slow af
	c.RLock()
	v, ok := c.v[off]
	if ok {
		c.RUnlock()
		return v, nil
	}
	c.RUnlock()
	data := make([]byte, size)
	_, err := c.file.ReadAt(data, off)
	if err != nil {
		return nil, err
	}

	c.Lock()
	if len(c.v) > c.size {
		c.v = map[int64][]byte{}
	}
	c.v[off] = data
	c.Unlock()

	return data, nil
}
