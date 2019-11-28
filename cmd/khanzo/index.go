package main

import (
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"runtime"
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

//go:generate msgp -tests=false
type Segment struct {
	Path     string
	Postings map[string]map[string][]int32
	Offset   uint32
	fw       *disk.ForwardWriter
}

func (s *Segment) Merge(o *Segment) {
	s.Offset = o.Offset
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
	t0 := time.Now()
	err := m.LoadFromDisk()
	if err != nil {
		log.WithError(err).Warnf("failed to load, starting from scratch")
	} else {
		log.Warnf("loaded from disk, took: %v", time.Since(t0))
		m.PrintStats()
	}

	return m
}
func (m *MemOnlyIndex) PrintStats() {
	GIANT.RLock()
	defer GIANT.RUnlock()

	for sid, v := range m.Segments {
		terms := 0
		types := 0
		size := 0
		for _, pk := range v.Postings {
			types++
			for _, postings := range pk {
				terms++
				size += len(postings) * 4
			}
		}
		log.Warnf("segment %v, sizeMB: %d, terms: %d, types: %d", sid, size/1024/1024, terms, types)
	}
}
func (m *MemOnlyIndex) LoadFromDisk() error {
	fo, err := os.OpenFile(path.Join(m.Root, "inverted.current"), os.O_RDONLY, 0600)
	if err != nil {
		return err
	}
	defer fo.Close()

	reader := msgp.NewReader(fo)

	err = m.DecodeMsg(reader)
	if err != nil {
		for _, s := range m.Segments {
			s.fw, err = disk.NewForwardWriter(s.Path, "main")
			if err != nil {
				return err
			}
		}
	}
	return err
}
func (m *MemOnlyIndex) toSegmentId(sid int64) string {
	// because of msgp
	return fmt.Sprintf("%d", sid)
}
func (m *MemOnlyIndex) DumpToDisk() error {
	GIANT.RLock()
	defer GIANT.RUnlock()
	t0 := time.Now()
	tmp := path.Join(m.Root, "inverted.tmp")
	fo, err := os.OpenFile(tmp, os.O_TRUNC|os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return err
	}
	defer fo.Close()
	writer := msgp.NewWriter(fo)
	err = m.EncodeMsg(writer)
	if err != nil {
		return err
	}

	err = writer.Flush()
	if err != nil {
		return err
	}
	current := path.Join(m.Root, "inverted.current")
	err = os.Rename(tmp, current)
	if err != nil {
		return err
	}
	log.Warnf("storing took %v", time.Since(t0))
	return nil
}

func (m *MemOnlyIndex) Refresh() error {
	days, err := ioutil.ReadDir(path.Join(m.Root))
	if err != nil {
		return err
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

	wait := make(chan error)

	maxReaders := runtime.GOMAXPROCS(0)
	var sem = make(chan bool, maxReaders)
	for _, sid := range todo {
		sem <- true
		go func(sid int64) {
			err := m.LoadSingleSegment(sid)
			<-sem
			wait <- err
		}(sid)
	}

	for range todo {
		err := <-wait
		if err != nil {
			// XXX: abandon everything, we panic anyway
			return err
		}
	}
	return nil
}

func (m *MemOnlyIndex) LoadSingleSegment(sid int64) error {
	p := path.Join(m.Root, fmt.Sprintf("%d", sid))
	segment := &Segment{Postings: map[string]map[string][]int32{}, Path: p}

	GIANT.RLock()
	oldSegment, ok := m.Segments[m.toSegmentId(sid)]
	GIANT.RUnlock()

	if ok {
		segment.Offset = oldSegment.Offset
		segment.fw = oldSegment.fw
	}

	if segment.fw == nil {
		fw, err := disk.NewForwardWriter(p, "main")
		if err != nil {
			return err
		}
		segment.fw = fw
	}

	storedOffset := segment.Offset
	did := uint32(storedOffset)
	t0 := time.Now()
	cnt := 0

	err := segment.fw.Scan(uint32(storedOffset), func(Offset uint32, data []byte) error {
		meta := spec.CondenseMetadata{}
		err := proto.Unmarshal(data, &meta)
		if err != nil {
			return err
		}

		segment.Add(meta.ForeignType, meta.ForeignId, int32(did))
		segment.Add("event_type", meta.EventType, int32(did))

		for _, kv := range meta.Search {
			segment.Add(kv.Key, kv.Value, int32(did))
		}
		for ex := range meta.Track {
			segment.Add("__experiment", ex, int32(did))
		}

		did = Offset
		segment.Offset = did
		cnt++
		return nil
	})
	if cnt > 0 && storedOffset != 0 {
		log.Warnf("loading path: %s, Offset: %d, took: %v for %d events", p, storedOffset, time.Since(t0), cnt)
	}
	if err != nil {
		return fmt.Errorf("error scanning, startOffset: %d, currentOffset: %d, err: %s", storedOffset, did, err)
	}

	// merge
	GIANT.Lock()
	oldSegment, ok = m.Segments[m.toSegmentId(sid)]
	if !ok {
		m.Segments[m.toSegmentId(sid)] = segment
	} else {
		oldSegment.Merge(segment)
	}
	GIANT.Unlock()
	return nil
}

var errNotFound = errors.New("not found")

func (m *MemOnlyIndex) ReadForward(sid int64, did int32) ([]byte, error) {
	GIANT.RLock()
	segment, ok := m.Segments[m.toSegmentId(sid)]
	if !ok {
		GIANT.RUnlock()
		return nil, errNotFound
	}
	GIANT.RUnlock()

	data, _, err := segment.fw.Read(uint32(did))
	return data, err
}

func (m *MemOnlyIndex) ReadAndDecodeForward(sid int64, did int32) (*spec.Metadata, error) {
	data, err := m.ReadForward(sid, did)
	if err != nil {
		return nil, err
	}
	out := &spec.Metadata{}
	err = proto.Unmarshal(data, out)
	if err != nil {
		return nil, err
	}

	return out, nil
}

func (m *MemOnlyIndex) NewTermQuery(sid int64, tagKey string, tagValue string) iq.Query {
	tagKey = depths.Cleanup(strings.ToLower(tagKey))
	tagValue = depths.Cleanup(strings.ToLower(tagValue))

	s := fmt.Sprintf("%s:%s", tagKey, tagValue)

	GIANT.RLock()
	segment, ok := m.Segments[m.toSegmentId(sid)]
	if !ok {
		GIANT.RUnlock()
		return iq.Term(s, []int32{})
	}

	pk, ok := segment.Postings[tagKey]
	if !ok {
		GIANT.RUnlock()
		return iq.Term(s, []int32{})
	}
	pv, ok := pk[tagValue]
	if !ok {
		GIANT.RUnlock()
		return iq.Term(s, []int32{})
	}
	GIANT.RUnlock()
	return iq.Term(s, pv)
}

func (m *MemOnlyIndex) ForEach(qr *spec.SearchQueryRequest, limit uint32, cb func(int64, int32, float32)) error {
	dates := depths.ExpandYYYYMMDD(qr.From, qr.To)
	for _, date := range dates {
		sid := depths.SegmentFromNs(date.UnixNano())
		query, err := fromQuery(qr.Query, func(k, v string) iq.Query {
			return m.NewTermQuery(sid, k, v)
		})
		if err != nil {
			return err
		}
		for query.Next() != iq.NO_MORE {
			did := query.GetDocId()
			score := query.Score()
			cb(sid, did, score)
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

type matching struct {
	did   int32
	score float32
	m     *spec.Metadata
}

func (m *MemOnlyIndex) ForEachDecodeParallel(parallel int, qr *spec.SearchQueryRequest, limit int32, cb func(int32, *spec.Metadata, float32)) error {
	dates := depths.ExpandYYYYMMDD(qr.From, qr.To)
	l := log.WithField("limit", limit)
	for _, date := range dates {
		sid := depths.SegmentFromNs(date.UnixNano())
		query, err := fromQuery(qr.Query, func(k, v string) iq.Query {
			return m.NewTermQuery(sid, k, v)
		})
		if err != nil {
			return err
		}
		l.Warnf("running query {%v} in segment: %s, with %d workers", query.String(), date, parallel)

		work := make(chan matching)
		doneWorker := make(chan bool)
		doneQuery := make(chan bool)
		doneConsumer := make(chan bool)
		completed := make(chan matching)

		for i := 0; i < parallel; i++ {
			go func(sid int64) {
				for w := range work {
					m, err := m.ReadAndDecodeForward(sid, w.did)
					if err != nil {
						l.Warnf("failed to decode Offset %d, err: %s", w.did, err)
						continue
					}
					w.m = m
					completed <- w
				}
				doneWorker <- true
			}(sid)
		}

		stopped := false
		go func() {
			for query.Next() != iq.NO_MORE {
				did := query.GetDocId()
				score := query.Score()
				work <- matching{did: did, score: score, m: nil}
				if limit > 0 {
					limit--
					if limit == 0 {
						stopped = true
						break
					}
				}
			}
			doneQuery <- true
		}()

		go func() {
			for matching := range completed {
				cb(matching.did, matching.m, matching.score)
			}
			doneConsumer <- true
		}()

		<-doneQuery
		close(work)

		for i := 0; i < parallel; i++ {
			<-doneWorker
		}

		close(completed)

		<-doneConsumer
		if stopped {
			break
		}
	}

	return nil
}
