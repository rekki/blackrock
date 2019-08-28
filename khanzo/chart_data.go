package main

import (
	"time"

	"github.com/jackdoe/blackrock/orgrim/spec"
)

type ChartPoint struct {
	ForeignLink map[string]map[string]uint32 `json:"-"`
	Count       uint32                       `json:"count"`
	CountUnique uint32                       `json:"count_unique"`
	CreatedAt   uint32                       `json:"bucket"`
	EventType   string                       `json:"event_type"`
}

type Chart struct {
	PerTimePerType map[uint32]map[string]*ChartPoint `json:"per_time_per_type"`
	TimeBucket     uint32                            `json:"time_bucket_sec"`
	TimeStart      uint32                            `json:"time_start"`
	TimeEnd        uint32                            `json:"time_end"`
}

func NewChart(timebucket uint32, dates []time.Time) *Chart {
	perBucket := map[uint32]map[string]*ChartPoint{}

	return &Chart{
		PerTimePerType: perBucket,
		TimeBucket:     timebucket,
		TimeStart:      uint32(dates[0].Unix()),
		TimeEnd:        uint32(dates[len(dates)-1].AddDate(0, 0, 1).Unix()),
	}
}

func (c *Chart) Add(m *spec.Metadata) {
	bucket := (uint32(m.CreatedAtNs/1000000000) / c.TimeBucket) * c.TimeBucket
	perType, ok := c.PerTimePerType[bucket]
	if !ok {
		perType = map[string]*ChartPoint{}
		c.PerTimePerType[bucket] = perType
	}

	point, ok := perType[m.EventType]
	if !ok {
		point = &ChartPoint{
			ForeignLink: map[string]map[string]uint32{},
			CreatedAt:   bucket,
			EventType:   m.EventType,
		}
		perType[m.EventType] = point
	}

	ft, ok := point.ForeignLink[m.ForeignType]
	if !ok {
		ft = map[string]uint32{}
		point.ForeignLink[m.ForeignType] = ft
	}
	v, ok := ft[m.ForeignId]
	if !ok {
		point.CountUnique++
		v = 1
	} else {
		v++
	}
	ft[m.ForeignId] = v
	point.Count++
}
