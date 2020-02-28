package index

import (
	"errors"
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

	spec "github.com/rekki/blackrock/pkg/blackrock_io"

	"github.com/rekki/blackrock/pkg/logger"

	"github.com/rekki/blackrock/pkg/depths"
	. "github.com/rekki/blackrock/pkg/logger"
	iq "github.com/rekki/go-query"
	dsl "github.com/rekki/go-query/util/index"
)

type SearchIndex struct {
	root               string
	Segments           map[string]*Segment
	whitelist          map[string]bool
	SegmentStep        int64
	enableSegmentCache bool
	sync.RWMutex
}

func NewSearchIndex(root string, storeInterval, segmentStep int64, enableSegmentCache bool, whitelist map[string]bool) *SearchIndex {
	root = path.Join(root, fmt.Sprintf("%d", segmentStep))

	err := os.MkdirAll(root, 0700)
	if err != nil {
		Log.Fatal(err)
	}

	m := &SearchIndex{root: root, Segments: map[string]*Segment{}, SegmentStep: segmentStep, enableSegmentCache: enableSegmentCache, whitelist: whitelist}

	if storeInterval > 0 {
		go func() {
			store := time.Duration(storeInterval) * time.Second
			for {
				time.Sleep(store)

				err = m.DumpToDisk()
				if err != nil {
					Log.Fatal(err)
				}
				m.PrintStats()
			}
		}()
	}

	return m
}

func (m *SearchIndex) Ingest(envelope *spec.Envelope) error {
	err := PrepareEnvelope(envelope)
	if err != nil {
		return err
	}

	err = m.holdWrite(envelope.Metadata.CreatedAtNs, func(segment *Segment) error {
		segment.writtenAt = time.Now()
		return segment.Ingest(envelope)
	})
	return err
}

func (m *SearchIndex) PrintStats() {
	m.RLock()
	defer m.RUnlock()

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
		Log.Infof("segment %v, #docs: %d, invsize: %02fMB, terms: %d, types: %d, dirty: %v", sid, v.TotalDocs, float32(size)/1024/1024, terms, types, v.dirty)
	}
}

func (m *SearchIndex) Close() {
	// NB: used only for testing, does not close the goroutines
	m.Lock()
	defer m.Unlock()

	for k, s := range m.Segments {
		s.Close()
		delete(m.Segments, k)
	}
}
func (m *SearchIndex) toSegmentId(ns int64) string {
	s := ns / 1000000000
	d := s / m.SegmentStep

	return fmt.Sprintf("%d", d)
}

func (m *SearchIndex) ListSegments() ([]int64, error) {
	days, err := ioutil.ReadDir(m.root)
	if err != nil {
		return nil, err
	}
	todo := []int64{}
	for _, day := range days {
		if !day.IsDir() {
			Log.Infof("skipping: %s", day.Name())
			continue
		}
		id, err := strconv.ParseInt(day.Name(), 10, 64)
		if err != nil {
			Log.Infof("skipping: %s, cant parse step", day.Name())
			continue
		}
		todo = append(todo, id*m.SegmentStep*1000000000)
	}
	sort.Slice(todo, func(i, j int) bool {
		return todo[i] < todo[j]
	})
	return todo, nil
}

func (m *SearchIndex) DumpToDisk() error {
	c := map[string]*Segment{}

	m.Lock()
	for k, s := range m.Segments {
		c[k] = s
	}
	m.Unlock()

	for _, s := range c {
		m.Lock()
		if !s.dirty {
			m.Unlock()
			continue
		}

		s.dirty = false
		m.Unlock()

		m.RLock()
		// NB: if this fails we should panic?(and we do in the caller, but its not obvious)
		err := s.DumpToDisk()
		m.RUnlock()
		if err != nil {
			return err
		}
	}

	return nil
}

func (m *SearchIndex) LoadAtBoot(n int) error {
	todo, err := m.ListSegments()
	if err != nil {
		return err
	}
	wait := make(chan bool)

	if n > 0 && len(todo) > n {
		todo = todo[:n]
	}
	maxReaders := runtime.GOMAXPROCS(0)
	var sem = make(chan bool, maxReaders)
	for _, sid := range todo {
		sem <- true
		go func(segmentId int64) {
			err := m.holdWrite(segmentId, func(segment *Segment) error {
				return nil
			})
			if err != nil {
				panic(err)
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

func (m *SearchIndex) LookupSingleSegment(ns int64) *Segment {
	segmentId := m.toSegmentId(ns)
	m.RLock()
	defer m.RUnlock()

	return m.Segments[segmentId]
}

func (m *SearchIndex) loadSegmentFromDisk(segmentId string) (*Segment, error) {
	p := path.Join(m.root, segmentId)
	segment, err := NewSegment(p, m.enableSegmentCache, m.whitelist)
	if err != nil {
		return nil, err
	}
	err = segment.LoadFromDisk()
	if err != nil && !os.IsNotExist(err) {
		logger.Log.Warnf("error loading from disk, continuing anyway, err: %s", err.Error())
	}

	segment.loadedAt = time.Now()
	err = segment.OpenForwardIndex()
	if err != nil {
		return nil, err
	}

	err = segment.Catchup()
	if err != nil {
		segment.Close()
		return nil, err
	}
	return segment, nil
}

func (m *SearchIndex) newTermQuery(segment *Segment, tagKey string, tagValue string) iq.Query {
	tagKey = depths.Cleanup(strings.ToLower(tagKey))
	tagValue = depths.Cleanup(strings.ToLower(tagValue))
	termString := fmt.Sprintf("%s:%s", tagKey, tagValue)

	pk, ok := segment.Postings[tagKey]
	if !ok {
		logger.Log.Infof("%s missing field: %s", segment.Path, tagKey)
		return iq.Term(segment.TotalDocs, termString, []int32{})
	}

	pv, ok := pk[tagValue]
	if !ok {
		logger.Log.Infof("%s missing value: %s", segment.Path, tagValue)
		return iq.Term(segment.TotalDocs, termString, []int32{})
	}

	logger.Log.Infof("%s found %s, postings: %d", segment.Path, termString, len(pv))
	return iq.Term(segment.TotalDocs, termString, pv)
}

func (m *SearchIndex) holdRead(step int64, cb func(s *Segment) error) error {
	segmentId := m.toSegmentId(step)

	m.RLock()

	var err error

	segment, ok := m.Segments[segmentId]
	if !ok {
		m.RUnlock()

		// RACE (multiple load)

		segment, err = m.loadSegmentFromDisk(segmentId)
		if err != nil {
			return err
		}

		m.Lock()

		overriden, ok := m.Segments[segmentId]
		if ok {
			segment.Close()
			segment = overriden
		} else {
			err = segment.Catchup()
			if err != nil {
				m.Unlock()
				return err
			}
			m.Segments[segmentId] = segment
		}

		m.Unlock()

		m.RLock()
	}

	err = cb(segment)
	m.RUnlock()
	return err
}

func (m *SearchIndex) holdWrite(step int64, cb func(s *Segment) error) error {
	segmentId := m.toSegmentId(step)
	m.Lock()
	defer m.Unlock()
	var err error
	segment, ok := m.Segments[segmentId]
	if !ok {
		segment, err = m.loadSegmentFromDisk(segmentId)
		if err != nil {
			return err
		}
		m.Segments[segmentId] = segment
	}

	return cb(segment)
}

var errBadRequest = errors.New("missing Query")

func (m *SearchIndex) ForEach(qr *spec.SearchQueryRequest, limit uint32, cb func(*Segment, int32, float32) error) error {
	steps := m.ExpandFromTo(qr.FromSecond, qr.ToSecond)
	if qr.Query == nil {
		return errBadRequest
	}

	for _, step := range steps {
		err := m.holdRead(step, func(segment *Segment) error {
			segment.searchedAt = time.Now()

			query, err := dsl.Parse(qr.Query, func(k, v string) iq.Query {
				return m.newTermQuery(segment, k, v)
			})
			if err != nil {
				return err
			}

			// no need to lock the segment after that because its used only to get data from the forward index
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
						return nil
					}
				}
			}
			return nil
		})
		if err != nil {
			return err
		}
	}

	return nil
}

func (m *SearchIndex) ExpandFromTo(from uint32, to uint32) []int64 {
	if to == 0 {
		to = uint32(time.Now().Unix())
	}

	if from == 0 {
		from = uint32(time.Now().Unix()) - (3600 * 24)
	}

	out := []int64{}
	f := int64((int64(from) / m.SegmentStep) * m.SegmentStep)
	t := int64((int64(to) / m.SegmentStep) * m.SegmentStep)
	for i := f; i <= t; i += m.SegmentStep {
		out = append(out, i*1000000000)
	}
	return out
}
