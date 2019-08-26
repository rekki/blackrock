package main

import (
	"sort"

	"github.com/jackdoe/blackrock/orgrim/spec"
)

type ChartPoint struct {
	ForeignLink map[string]map[string]uint32 `json:"per_user"`
	Count       uint32                       `json:"count"`
	CreatedAtNs int64                        `json:"bucket_ns"`
	EventType   string                       `json:"event_type"`
}

type Chart struct {
	PerTimePerType map[int64]map[string]*ChartPoint
	TimeBucketNs   int64
}

func NewChart(timebucketns int64) *Chart {
	return &Chart{
		PerTimePerType: map[int64]map[string]*ChartPoint{},
		TimeBucketNs:   timebucketns,
	}
}

func (c *Chart) Points() []*ChartPoint {
	out := []*ChartPoint{}

	for _, v := range c.PerTimePerType {
		for _, vv := range v {
			out = append(out, vv)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[j].CreatedAtNs == out[i].CreatedAtNs {
			return out[i].EventType < out[j].EventType
		}
		return out[j].CreatedAtNs < out[i].CreatedAtNs
	})

	return out
}

func (c *Chart) Add(m *spec.Metadata) {
	bucket := m.CreatedAtNs / c.TimeBucketNs
	perType, ok := c.PerTimePerType[bucket]
	if !ok {
		perType = map[string]*ChartPoint{}
		c.PerTimePerType[bucket] = perType
	}

	point, ok := perType[m.EventType]
	if !ok {
		point = &ChartPoint{
			ForeignLink: map[string]map[string]uint32{},
			CreatedAtNs: (m.CreatedAtNs / c.TimeBucketNs) * c.TimeBucketNs,
			EventType:   m.EventType,
		}
		perType[m.EventType] = point
	}

	ft, ok := point.ForeignLink[m.ForeignType]
	if !ok {
		ft = map[string]uint32{}
		point.ForeignLink[m.ForeignType] = ft
	}
	ft[m.ForeignId]++
	point.Count++
}
