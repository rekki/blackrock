package disk

import (
	"encoding/binary"
	"io/ioutil"
	"os"
	"path"

	"github.com/jackdoe/blackrock/depths"
	log "github.com/sirupsen/logrus"
)

type InvertedWriter struct {
	descriptors        map[string]*os.File // FIXME(jackdoe): use lru
	maxOpenDescriptors int
}

func NewInvertedWriter(maxOpenDescriptors int) (*InvertedWriter, error) {
	return &InvertedWriter{
		maxOpenDescriptors: maxOpenDescriptors,
		descriptors:        map[string]*os.File{},
	}, nil
}
func (fw *InvertedWriter) Close() {
	for k, v := range fw.descriptors {
		v.Close()
		delete(fw.descriptors, k)
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
	total := fi.Size() / int64(4)
	return int32(total)
}

func InvertedReadRaw(root string, maxDocuments int32, tagKey, tagValue string) []int32 {
	dir, filename := depths.PathForTag(root, tagKey, tagValue)
	fn := path.Join(dir, filename)
	file, err := os.OpenFile(fn, os.O_RDONLY, 0600)
	if os.IsNotExist(err) {
		log.Infof("missing file %s, returning empty", fn)
		return []int32{}
	}
	fi, err := file.Stat()
	if err != nil {
		log.Warnf("failed to read file stats: %s, error: %s", fn, err.Error())
		return []int32{}
	}
	// read to the closest multiple of 4
	log.Infof("reading %s, size: %d", fn, fi.Size())
	total := int32(fi.Size() / int64(4))
	seek := int32(0)
	if maxDocuments > 0 && total > maxDocuments {
		seek = (total - maxDocuments) * 4
	}
	file.Seek(int64(seek), 0)
	log.Infof("seek %d, total %d max requested: %d", seek, total, maxDocuments)

	postings, err := ioutil.ReadAll(file)
	if err != nil {
		log.Warnf("failed to read file: %s, error: %s", fn, err.Error())
		return []int32{}
	}
	return depths.BytesToInts(postings)
}

func (fw *InvertedWriter) Append(root string, docId int32, tagKey, tagValue string) error {
	dir, fn := depths.PathForTag(root, tagKey, tagValue)
	filename := path.Join(dir, fn)
	f, ok := fw.descriptors[filename]
	if !ok {
		if len(fw.descriptors) > fw.maxOpenDescriptors {
			log.Warnf("clearing descriptor cache len: %d", len(fw.descriptors))
			for dk, fd := range fw.descriptors {
				fd.Close()
				delete(fw.descriptors, dk)
			}
		}

		log.Infof("openning %s", filename)
		os.MkdirAll(dir, 0700)
		fd, err := os.OpenFile(filename, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
		if err != nil {
			return err
		}
		f = fd
		fw.descriptors[filename] = fd
	}
	log.Infof("writing document id %d at %s", docId, filename)
	data := make([]byte, 4)

	binary.LittleEndian.PutUint32(data, uint32(docId))
	_, err := f.Write(data)
	if err != nil {
		return err
	}
	return nil
}
