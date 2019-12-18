package index

import (
	"fmt"
	"os"
	"path"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gogo/protobuf/proto"
	spec "github.com/rekki/blackrock/pkg/blackrock_io"
	"github.com/rekki/blackrock/pkg/logger"
	pen "github.com/rekki/go-pen"
	"github.com/tinylib/msgp/msgp"
)

const INVERTED_INDEX_FILE_NAME = "inverted.current.v5"

//go:generate msgp -tests=false
type Segment struct {
	Path      string
	Postings  map[string]map[string][]int32
	Offset    uint32
	TotalDocs int
	reader    *pen.Reader
	writer    *pen.Writer
	dirty     bool

	loadedAt    time.Time
	searchedAt  time.Time
	writtenAt   time.Time
	cache       sync.Map
	enableCache bool
}

func NewSegment(root string, enableCache bool) (*Segment, error) {
	return &Segment{Postings: map[string]map[string][]int32{}, Path: root, Offset: 0, enableCache: enableCache}, nil
}

func (s *Segment) Catchup() error {
	cnt := 0

	start := s.Offset
	err := s.reader.Scan(s.Offset, func(data []byte, current, next uint32) error {
		meta := spec.SearchableMetadata{}
		err := proto.Unmarshal(data, &meta)
		if err != nil {
			return err
		}

		for _, kv := range meta.Search {
			s.Add(kv.Key, kv.Value, int32(current))
		}

		s.Add(meta.ForeignType, meta.ForeignId, int32(current))
		s.Add("event_type", meta.EventType, int32(current))
		s.Add("blackrock", "match_all", int32(current))

		s.Offset = next
		s.TotalDocs++
		s.dirty = true

		cnt++

		return nil
	})

	logger.Log.Infof("%s catchup: %d, startOffset: %d, endOffset: %d", s.Path, cnt, start, s.Offset)
	return err
}

func (s *Segment) Ingest(envelope *spec.Envelope) error {
	encoded, err := proto.Marshal(envelope.Metadata)
	if err != nil {
		return err
	}

	did, next, err := s.writer.Append(encoded)
	if err != nil {
		return err
	}
	meta := envelope.Metadata

	for _, kv := range meta.Search {
		s.Add(kv.Key, kv.Value, int32(did))
	}

	s.Add(meta.ForeignType, meta.ForeignId, int32(did))
	s.Add("event_type", meta.EventType, int32(did))
	s.Add("blackrock", "match_all", int32(did))

	s.Offset = next
	s.TotalDocs++

	// this is under write lock
	s.dirty = true

	return nil
}

func (s *Segment) Add(k, v string, did int32) {
	pk, ok := s.Postings[k]
	if !ok {
		pk = map[string][]int32{}
		s.Postings[k] = pk
	}

	postings := pk[v]
	if len(postings) == 0 {
		postings = []int32{did}
		pk[v] = postings
	} else {
		if postings[len(postings)-1] != did {
			postings = append(postings, did)
			pk[v] = postings
		}
	}
}

func (s *Segment) ReadForward(did int32) ([]byte, error) {
	if s.enableCache {
		v, ok := s.cache.Load(did)
		if ok {
			return v.([]byte), nil
		}

		data, _, err := s.reader.Read(uint32(did))
		s.cache.Store(did, data)
		return data, err
	} else {
		data, _, err := s.reader.Read(uint32(did))
		return data, err
	}
}

func (s *Segment) ReadForwardDecode(did int32, m proto.Message) error {
	data, err := s.ReadForward(did)
	if err != nil {
		return err
	}

	err = proto.Unmarshal(data, m)
	if err != nil {
		return err
	}

	return err
}

func (s *Segment) LoadFromDisk() error {
	fn := path.Join(s.Path, INVERTED_INDEX_FILE_NAME)
	fo, err := os.OpenFile(fn, os.O_RDONLY, 0600)
	if err != nil {
		return err
	}
	defer fo.Close()
	t0 := time.Now()

	reader := msgp.NewReader(fo)
	err = s.DecodeMsg(reader)
	if err != nil {
		return err
	}

	logger.Log.Infof("[done] loading %v, took: %v", fn, time.Since(t0))
	return nil
}

func (s *Segment) OpenForwardIndex() error {
	err := os.MkdirAll(s.Path, 0700)
	if err != nil {
		return err
	}

	fn := path.Join(s.Path, "main.bin")
	writer, err := pen.NewWriter(fn)
	if err != nil {
		return err
	}

	reader, err := pen.NewReader(fn, 0)
	if err != nil {
		writer.Close()
		return err
	}

	s.reader = reader
	s.writer = writer
	return nil
}
func (s *Segment) Close() {
	if s.writer != nil {
		_ = s.writer.Sync()
		_ = s.writer.Close()
		_ = s.reader.Close()
	}
}

var tempIncrement = uint64(0)

func (s *Segment) DumpToDisk() error {
	tempId := atomic.AddUint64(&tempIncrement, 1)
	t0 := time.Now()
	tmp := path.Join(s.Path, fmt.Sprintf("%s.%d.tmp", INVERTED_INDEX_FILE_NAME, tempId))
	fo, err := os.OpenFile(tmp, os.O_TRUNC|os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return err
	}
	defer fo.Close()
	writer := msgp.NewWriter(fo)
	err = s.EncodeMsg(writer)
	if err != nil {
		return err
	}

	err = writer.Flush()
	if err != nil {
		return err
	}
	current := path.Join(s.Path, INVERTED_INDEX_FILE_NAME)
	err = os.Rename(tmp, current)
	if err != nil {
		return err
	}
	logger.Log.Infof("[done] %s storing took %v", s.Path, time.Since(t0))
	return nil
}
