package index

import (
	"os"
	"path"
	"sync"

	"github.com/gogo/protobuf/proto"
	spec "github.com/rekki/blackrock/pkg/blackrock_io"
	pen "github.com/rekki/go-pen"
	dsl "github.com/rekki/go-query/util/index"
)

func init() {
	dsl.DefaultAnalyzer = dsl.IDAnalyzer
}

type Segment struct {
	dir         *dsl.DirIndex
	root        string
	whitelist   map[string]bool
	reader      *pen.Reader
	writer      *pen.Writer
	cache       sync.Map
	enableCache bool
}

func NewSegment(root string, fdc dsl.FileDescriptorCache, enableCache bool, whitelist map[string]bool) (*Segment, error) {
	s := &Segment{root: root, dir: dsl.NewDirIndex(path.Join(root, "inv"), fdc, nil), enableCache: enableCache, whitelist: whitelist}
	err := s.OpenForwardIndex()
	if err != nil {
		return nil, err
	}
	return s, nil
}

type Indexable struct {
	data map[string][]string
	id   int32
}

func (x *Indexable) IndexableFields() map[string][]string {
	return x.data
}

func (x *Indexable) DocumentID() int32 {
	return x.id
}

func (s *Segment) Ingest(envelope *spec.Envelope) error {
	encoded, err := proto.Marshal(envelope.Metadata)
	if err != nil {
		return err
	}

	did, _, err := s.writer.Append(encoded)
	if err != nil {
		return err
	}
	meta := envelope.Metadata

	x := Indexable{
		data: map[string][]string{},
		id:   int32(did),
	}
	for _, kv := range meta.Search {
		if len(kv.Key) == 0 || len(kv.Value) == 0 {
			continue
		}
		if s.whitelist == nil || len(s.whitelist) == 0 || s.whitelist[kv.Key] {
			x.data[kv.Key] = append(x.data[kv.Key], kv.Value)
		}
	}

	x.data[meta.ForeignType] = []string{meta.ForeignId}
	x.data["event_type"] = []string{meta.EventType}
	x.data["blackrock"] = []string{"match_all"}

	return s.dir.Index(dsl.DocumentWithID(&x))
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

func (s *Segment) OpenForwardIndex() error {
	err := os.MkdirAll(s.root, 0700)
	if err != nil {
		return err
	}

	fn := path.Join(s.root, "main.bin")
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
		s.dir.Close()
	}
}
