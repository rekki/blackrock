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
	todo map[string][]byte
	made map[string]bool
}

func NewInvertedWriter() (*InvertedWriter, error) {
	return &InvertedWriter{
		todo: map[string][]byte{},
		made: map[string]bool{},
	}, nil
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

func (fw *InvertedWriter) Append(root string, docId int32, t int32, tagKey, tagValue string) error {
	dir, fn := depths.PathForTag(root, tagKey, tagValue)
	filename := path.Join(dir, fn)
	if _, ok := fw.made[dir]; !ok {
		os.MkdirAll(dir, 0700)
		fw.made[dir] = true
	}

	data := make([]byte, 8)
	binary.LittleEndian.PutUint64(data, uint64(docId)<<32|uint64(t))
	fw.todo[filename] = append(fw.todo[filename], data...)
	return nil
}

func (fw *InvertedWriter) Flush() error {
	max := 0
	for filename, data := range fw.todo {
		log.Infof("openning %s", filename)
		fd, err := os.OpenFile(filename, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
		if err != nil {
			return err
		}
		_, err = fd.Write(data)
		if err != nil {
			return err
		}

		fd.Close()
		n := len(data) / 8
		if max < n {
			max = n
		}

		delete(fw.todo, filename)
	}
	log.Warnf("writing %d documents", max)
	return nil
}
