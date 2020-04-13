package index

import (
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"sort"
	"strconv"
	"sync"
	"time"

	spec "github.com/rekki/blackrock/pkg/blackrock_io"

	. "github.com/rekki/blackrock/pkg/logger"
	iq "github.com/rekki/go-query"
	dsl "github.com/rekki/go-query-index"
)

type SearchIndex struct {
	root               string
	Segments           map[string]*Segment
	whitelist          map[string]bool
	SegmentStep        int64
	enableSegmentCache bool
	fdCache            dsl.FileDescriptorCache
	sync.RWMutex
}

func NewSearchIndex(root string, nOpenFD int, segmentStep int64, enableSegmentCache bool, whitelist map[string]bool) *SearchIndex {
	root = path.Join(root, fmt.Sprintf("%d", segmentStep))

	err := os.MkdirAll(root, 0700)
	if err != nil {
		Log.Fatal(err)
	}

	fdc := dsl.NewFDCache(nOpenFD)
	m := &SearchIndex{root: root, fdCache: fdc, Segments: map[string]*Segment{}, SegmentStep: segmentStep, enableSegmentCache: enableSegmentCache, whitelist: whitelist}

	return m
}

func (m *SearchIndex) Ingest(envelope *spec.Envelope) error {
	err := PrepareEnvelope(envelope)
	if err != nil {
		return err
	}

	err = m.holdWrite(envelope.Metadata.CreatedAtNs, func(segment *Segment) error {
		return segment.Ingest(envelope)
	})
	return err
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

func (m *SearchIndex) LookupSingleSegment(ns int64) *Segment {
	segmentId := m.toSegmentId(ns)
	m.RLock()
	defer m.RUnlock()

	return m.Segments[segmentId]
}

func (m *SearchIndex) loadSegmentFromDisk(segmentId string) (*Segment, error) {
	p := path.Join(m.root, segmentId)
	segment, err := NewSegment(p, m.fdCache, m.enableSegmentCache, m.whitelist)
	if err != nil {
		return nil, err
	}
	return segment, nil
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
			query, err := dsl.Parse(qr.Query, func(k, v string) iq.Query {
				if len(k) == 0 || len(v) == 0 {
					return iq.Term(1, k+":"+v, []int32{})
				}
				queries := segment.dir.Terms(k, v)
				if len(queries) == 1 {
					return queries[0]
				} else {
					return iq.Or(queries...)
				}
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
