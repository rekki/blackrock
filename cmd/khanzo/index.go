package main

import (
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gogo/protobuf/proto"
	iq "github.com/jackdoe/go-query"
	"github.com/rekki/blackrock/cmd/jubei/disk"
	"github.com/rekki/blackrock/cmd/orgrim/spec"
	"github.com/rekki/blackrock/pkg/depths"
	log "github.com/sirupsen/logrus"
	"github.com/tinylib/msgp/msgp"
)

var GIANT = sync.RWMutex{}

const INVERTED_INDEX_FILE_NAME = "inverted.current.v5"

//go:generate msgp -tests=false
type Segment struct {
	Path      string
	Postings  map[string]map[string][]int32
	Offset    uint32
	TotalDocs int
	fw        *disk.ForwardWriter
	dirty     bool
	flushedAt time.Time
}

func (s *Segment) ReadForward(did int32) ([]byte, error) {
	data, _, err := s.fw.Read(uint32(did))
	return data, err
}

func (s *Segment) ReadForwardDecode(did int32, m proto.Message) error {
	data, _, err := s.fw.Read(uint32(did))
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

	log.Warnf("[done] loading %v, took: %v", fn, time.Since(t0))
	return nil
}

func (s *Segment) OpenForwardIndex() error {
	fw, err := disk.NewForwardWriter(s.Path, "main")
	if err != nil {
		return err
	}
	s.fw = fw
	return nil
}

func (s *Segment) DumpToDisk() error {
	if !s.dirty {
		return nil
	}

	t0 := time.Now()
	tmp := path.Join(s.Path, fmt.Sprintf("%s.tmp", INVERTED_INDEX_FILE_NAME))
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
	log.Warnf("[done] %s storing took %v", s.Path, time.Since(t0))
	s.dirty = false
	s.flushedAt = time.Now()
	return nil
}

func (s *Segment) Refresh() error {
	storedOffset := s.Offset
	did := uint32(storedOffset)
	t0 := time.Now()
	cnt := 0
	temporary := &Segment{Postings: map[string]map[string][]int32{}, Offset: s.Offset, TotalDocs: s.TotalDocs}

	err := s.fw.Scan(uint32(storedOffset), func(offset uint32, data []byte) error {
		meta := spec.SearchableMetadata{}
		err := proto.Unmarshal(data, &meta)
		if err != nil {
			return err
		}

		temporary.Add(meta.ForeignType, meta.ForeignId, int32(did))
		temporary.Add("event_type", meta.EventType, int32(did))

		for _, kv := range meta.Search {
			temporary.Add(kv.Key, kv.Value, int32(did))
		}
		for ex := range meta.Track {
			temporary.Add("__experiment", ex, int32(did))
		}

		did = offset
		temporary.Offset = did
		temporary.TotalDocs++
		cnt++
		return nil
	})

	if err != nil {
		return fmt.Errorf("error scanning, startOffset: %d, currentOffset: %d, err: %s", storedOffset, did, err)
	}
	if cnt > 0 {

		GIANT.Lock()
		s.dirty = true
		s.Merge(temporary)
		GIANT.Unlock()

		log.Warnf("[done] %s, Offset: %d, #docs: %d, took: %v for %d events", s.Path, storedOffset, s.TotalDocs, time.Since(t0), cnt)
	}
	return nil
}

func (s *Segment) Merge(o *Segment) {
	s.Offset = o.Offset
	s.TotalDocs = o.TotalDocs
	for k, pv := range o.Postings {
		for v, p := range pv {
			s.AddMany(k, v, p)
		}
	}
}
func (s *Segment) Add(k, v string, did int32) {
	pk, ok := s.Postings[k]
	if !ok {
		pk = map[string][]int32{}
		s.Postings[k] = pk
	}
	pk[v] = append(pk[v], did)
}

func (s *Segment) AddMany(k, v string, did []int32) {
	pk, ok := s.Postings[k]
	if !ok {
		pk = map[string][]int32{}
		s.Postings[k] = pk
	}
	pk[v] = append(pk[v], did...)
}

type MemOnlyIndex struct {
	Root     string
	Segments map[string]*Segment
}

func NewMemOnlyIndex(Root string) *MemOnlyIndex {
	m := &MemOnlyIndex{Root: Root, Segments: map[string]*Segment{}}
	return m
}
func (m *MemOnlyIndex) PrintStats() {
	GIANT.RLock()
	defer GIANT.RUnlock()
	keys := []string{}
	for sid := range m.Segments {
		keys = append(keys, sid)
	}
	sort.Strings(keys)
	for _, sid := range keys {
		v := m.Segments[sid]
		terms := 0
		types := 0
		size := 0
		for _, pk := range v.Postings {
			types++
			for _, postings := range pk {
				terms++
				size += 8 + (len(postings) * 4)
			}
		}
		log.Warnf("segment %v, #docs: %d, invsize: %02fMB, terms: %d, types: %d, dirty: %v", sid, v.TotalDocs, float32(size)/1024/1024, terms, types, v.dirty)
	}
}

func (m *MemOnlyIndex) toSegmentId(sid int64) string {
	// because of msgp
	return fmt.Sprintf("%d", sid)
}

func (m *MemOnlyIndex) ListSegments() ([]int64, error) {
	days, err := ioutil.ReadDir(path.Join(m.Root))
	if err != nil {
		return nil, err
	}
	todo := []int64{}
	for _, day := range days {
		if !day.IsDir() || !depths.IsDigit(day.Name()) {
			continue
		}

		n, err := strconv.Atoi(day.Name())
		if err != nil {
			log.Warnf("skipping %s", day.Name())
			continue
		}
		ts := n * 3600 * 24
		t := time.Unix(int64(ts), 0).UTC()
		todo = append(todo, depths.SegmentFromNs(t.UnixNano()))
	}
	sort.Slice(todo, func(i, j int) bool {
		return todo[i] < todo[j]
	})
	return todo, nil
}

func (m *MemOnlyIndex) DumpToDisk() error {
	GIANT.RLock()
	defer GIANT.RUnlock()
	for _, s := range m.Segments {
		err := s.DumpToDisk()
		if err != nil {
			return err
		}
	}

	return nil
}

func (m *MemOnlyIndex) Refresh(store bool) error {
	todo, err := m.ListSegments()
	if err != nil {
		return err
	}
	wait := make(chan bool)

	maxReaders := runtime.GOMAXPROCS(0)
	var sem = make(chan bool, maxReaders)
	for _, sid := range todo {
		sem <- true
		go func(segmentId int64) {
			s, err := m.LoadSingleSegment(segmentId)
			if err != nil {
				panic(err)
			}
			if store {
				err := s.DumpToDisk()
				if err != nil {
					panic(err)
				}
			}

			<-sem
			wait <- true
		}(sid)
	}

	for range todo {
		<-wait
	}
	return nil
}

func (m *MemOnlyIndex) LoadSingleSegment(sid int64) (*Segment, error) {
	GIANT.RLock()
	segment, ok := m.Segments[m.toSegmentId(sid)]
	GIANT.RUnlock()

	if !ok {
		p := path.Join(m.Root, fmt.Sprintf("%d", sid))
		segment = &Segment{Postings: map[string]map[string][]int32{}, Path: p, Offset: 0}
		err := segment.LoadFromDisk()
		if err != nil && !os.IsNotExist(err) {
			log.WithError(err).Warnf("error loading from disk, continuing anyway")
		}

		err = segment.OpenForwardIndex()
		if err != nil {
			return nil, err
		}

		GIANT.Lock()
		m.Segments[m.toSegmentId(sid)] = segment
		GIANT.Unlock()
	}

	return segment, segment.Refresh()
}

func (m *MemOnlyIndex) NewTermQuery(sid int64, tagKey string, tagValue string) iq.Query {
	tagKey = depths.Cleanup(strings.ToLower(tagKey))
	tagValue = depths.Cleanup(strings.ToLower(tagValue))

	s := fmt.Sprintf("%s:%s", tagKey, tagValue)

	GIANT.RLock()
	segment, ok := m.Segments[m.toSegmentId(sid)]
	if !ok {
		// XXX: load segment on demand
		GIANT.RUnlock()
		return iq.Term(segment.TotalDocs, s, []int32{})
	}

	pk, ok := segment.Postings[tagKey]
	if !ok {
		GIANT.RUnlock()
		return iq.Term(segment.TotalDocs, s, []int32{})
	}
	pv, ok := pk[tagValue]
	if !ok {
		GIANT.RUnlock()
		return iq.Term(segment.TotalDocs, s, []int32{})
	}
	GIANT.RUnlock()
	return iq.Term(segment.TotalDocs, s, pv)
}

func (m *MemOnlyIndex) ForEach(qr *spec.SearchQueryRequest, limit uint32, cb func(*Segment, int32, float32) error) error {
	dates := depths.ExpandYYYYMMDD(qr.From, qr.To)
	for _, date := range dates {
		sid := depths.SegmentFromNs(date.UnixNano())

		query, err := fromQuery(qr.Query, func(k, v string) iq.Query {
			return m.NewTermQuery(sid, k, v)
		})
		if err != nil {
			return err
		}
		GIANT.RLock()
		segment, ok := m.Segments[m.toSegmentId(sid)]
		GIANT.RUnlock()
		if !ok {
			continue
		}

		for query.Next() != iq.NO_MORE {
			did := query.GetDocId()
			score := query.Score()
			err = cb(segment, did, score)
			if err != nil {
				return err
			}
			if limit > 0 {
				limit--
				if limit == 0 {
					break
				}
			}
		}
	}

	return nil
}
