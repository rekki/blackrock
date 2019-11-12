package disk

import (
	"encoding/binary"
	"io/ioutil"
	"os"
	"path"

	"github.com/rekki/blackrock/pkg/depths"
	log "github.com/sirupsen/logrus"
)

type InvertedWriter struct {
	todo map[string]map[string][]byte
	root string
}

func NewInvertedWriter(root string) *InvertedWriter {
	return &InvertedWriter{
		root: root,
		todo: map[string]map[string][]byte{},
	}
}

func (fw *InvertedWriter) Size(root, tagKey, tagValue string) int32 {
	dir, filename := depths.PathForTag(root, tagKey, tagValue)
	fn := path.Join(dir, filename)
	file, err := os.OpenFile(fn, os.O_RDONLY, 0600)
	if os.IsNotExist(err) {
		return 0
	}
	fi, err := file.Stat()
	if err != nil {
		return 0
	}
	total := fi.Size() / int64(8)
	return int32(total)
}

func InvertedReadRaw(root string, maxDocuments int32, tagKey, tagValue string) []uint64 {
	dir, filename := depths.PathForTag(root, tagKey, tagValue)
	fn := path.Join(dir, filename)
	log.Printf("reading %v", fn)
	file, err := os.OpenFile(fn, os.O_RDONLY, 0600)
	if os.IsNotExist(err) {
		log.Infof("missing file %s, returning empty", fn)
		return []uint64{}
	} else if err != nil {
		log.Warnf("failed to open file %s, error: %s", fn, err.Error())
		return []uint64{}
	}
	defer file.Close()

	fi, err := file.Stat()
	if err != nil {
		log.Warnf("failed to read file stats: %s, error: %s", fn, err.Error())
		return []uint64{}
	}
	// read to the closest multiple of 8
	log.Infof("reading %s, size: %d", fn, fi.Size())
	total := int32(fi.Size() / int64(8))
	seek := int32(0)
	if maxDocuments > 0 && total > maxDocuments {
		seek = (total - maxDocuments) * 8
	}
	_, err = file.Seek(int64(seek), 0)
	if err != nil {
		log.Warnf("failed to seek in %s, error: %s", fn, err.Error())
		return []uint64{}
	}
	log.Infof("seek %d, total %d max requested: %d", seek, total, maxDocuments)

	postings, err := ioutil.ReadAll(file)
	if err != nil {
		log.Warnf("failed to read file: %s, error: %s", fn, err.Error())
		return []uint64{}
	}
	return depths.BytesToUints64(postings)
}

func (fw *InvertedWriter) Append(docId int32, t int32, tagKey, tagValue string) {
	data := make([]byte, 8)
	binary.LittleEndian.PutUint64(data, uint64(docId)<<32|uint64(t))

	pv, ok := fw.todo[tagKey]
	if !ok {
		pv = map[string][]byte{}
		fw.todo[tagKey] = pv
	}

	pv[tagValue] = append(pv[tagValue], data...)
}

func (fw *InvertedWriter) Flush() error {
	made := map[string]bool{}

	for tk, pv := range fw.todo {
		for tv, data := range pv {
			dir, fn := depths.PathForTag(fw.root, tk, tv)
			if _, ok := made[dir]; !ok {
				os.MkdirAll(dir, 0700)
				made[dir] = true
			}
			fd, err := os.OpenFile(path.Join(dir, fn), os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
			if err != nil {
				return err
			}
			_, err = fd.Write(data)
			if err != nil {
				return err
			}
		}
	}

	fw.todo = map[string]map[string][]byte{}
	return nil
}
