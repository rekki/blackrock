package main

import (
	"errors"
	"fmt"
	"io/ioutil"
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
)

type Segment struct {
	postings map[uint64][]int32
	fw       *disk.ForwardWriter
	offset   uint32
	sync.RWMutex
}

func (s *Segment) Merge(o *Segment) {
	s.offset = o.offset
	for kv, p := range o.postings {
		s.AddMany(kv, p)
	}
}
func (s *Segment) Add(kv uint64, did int32) {
	s.postings[kv] = append(s.postings[kv], did)
}

func (s *Segment) AddMany(kv uint64, did []int32) {
	s.postings[kv] = append(s.postings[kv], did...)
}

type MemOnlyIndex struct {
	root     string
	segments map[int64]*Segment
	words    map[string]uint32
	sync.RWMutex
}

func (m *MemOnlyIndex) kv(k string, v string) uint64 {
	ik, ok := m.words[k]
	if !ok {
		return 0
	}
	iv, ok := m.words[v]
	if !ok {
		return 0
	}

	return uint64(ik)<<32 | uint64(iv)
}

func (m *MemOnlyIndex) kvOrNew(k string, v string) uint64 {
	ik, ok := m.words[k]
	if !ok {
		// XXX: must start at 1, 0 is NOT_FOUND
		ik = uint32(len(m.words) + 1)
		m.words[k] = ik
	}
	iv, ok := m.words[v]
	if !ok {
		iv = uint32(len(m.words) + 1)
		m.words[v] = iv
	}

	return uint64(ik)<<32 | uint64(iv)
}

func NewMemOnlyIndex(root string) *MemOnlyIndex {
	return &MemOnlyIndex{root: root, segments: map[int64]*Segment{}, words: map[string]uint32{}}
}

func (m *MemOnlyIndex) Refresh() error {
	days, err := ioutil.ReadDir(path.Join(m.root))
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
	p := path.Join(m.root, fmt.Sprintf("%d", sid))
	segment := &Segment{postings: map[uint64][]int32{}}

	m.RLock()
	oldSegment, ok := m.segments[sid]
	m.RUnlock()

	if ok {
		segment.offset = oldSegment.offset
	}

	if segment.fw == nil {
		fw, err := disk.NewForwardWriter(p, "main")
		if err != nil {
			return err
		}
		segment.fw = fw
	}

	storedOffset := segment.offset
	did := uint32(storedOffset)
	t0 := time.Now()
	cnt := 0

	err := segment.fw.Scan(uint32(storedOffset), func(offset uint32, data []byte) error {
		meta := spec.CondenseMetadata{}
		err := proto.Unmarshal(data, &meta)
		if err != nil {
			return err
		}

		m.Lock()
		segment.Add(m.kvOrNew(meta.ForeignType, meta.ForeignId), int32(did))
		segment.Add(m.kvOrNew("event_type", meta.EventType), int32(did))

		for _, kv := range meta.Search {
			segment.Add(m.kvOrNew(kv.Key, kv.Value), int32(did))
		}
		for ex := range meta.Track {
			segment.Add(m.kvOrNew("__experiment", ex), int32(did))
		}
		m.Unlock()

		did = offset
		segment.offset = did
		cnt++
		return nil
	})

	log.Warnf("loading path: %s, offset: %d, took: %v for %d events", p, storedOffset, time.Since(t0), cnt)
	if err != nil {
		return fmt.Errorf("error scanning, startOffset: %d, currentOffset: %d, err: %s", storedOffset, did, err)
	}

	// merge
	m.Lock()
	oldSegment, ok = m.segments[sid]
	if !ok {
		m.segments[sid] = segment
	} else {
		oldSegment.Lock()
		oldSegment.Merge(segment)
		oldSegment.Unlock()
	}
	m.Unlock()
	return nil
}

var errNotFound = errors.New("not found")

func (m *MemOnlyIndex) ReadForward(sid int64, did int32) ([]byte, error) {
	m.RLock()
	segment, ok := m.segments[sid]
	if !ok {
		m.RUnlock()
		return nil, errNotFound
	}
	m.RUnlock()

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

	m.RLock()
	segment, ok := m.segments[sid]
	if !ok {
		m.RUnlock()
		return iq.Term(s, []int32{})
	}
	kv := m.kv(tagKey, tagValue)
	m.RUnlock()

	segment.RLock()
	// dont defer RUnlock because iq.Term(s, pv) takes time
	postings, ok := segment.postings[kv]
	if !ok {
		segment.RUnlock()
		return iq.Term(s, []int32{})
	}
	segment.RUnlock()
	return iq.Term(s, postings)
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
						l.Warnf("failed to decode offset %d, err: %s", w.did, err)
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
