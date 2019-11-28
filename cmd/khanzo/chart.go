package main

import (
	"time"

	"github.com/rekki/blackrock/cmd/orgrim/spec"
)

type FKV struct {
	ForeignId   string
	TimeBucket  uint32
	EventType   string
	ForeignType string
}
type Chart struct {
	out *spec.Chart
	fkv map[FKV]bool
}

func NewChart(timebucket uint32, dates []time.Time) *Chart {
	return &Chart{
		out: &spec.Chart{
			Buckets:       map[uint32]*spec.ChartBucketPerTime{},
			TimeBucketSec: timebucket,
			TimeStart:     (uint32(dates[0].Unix()) / timebucket) * timebucket,
			TimeEnd:       (uint32(dates[len(dates)-1].AddDate(0, 0, 1).Unix()) / timebucket) * timebucket,
		},
		fkv: map[FKV]bool{},
	}
}

func (c *Chart) Add(m *spec.CountableMetadata) {
	bucket := (uint32(m.CreatedAtNs/1000000000) / c.out.TimeBucketSec) * c.out.TimeBucketSec
	perTime, ok := c.out.Buckets[bucket]
	if !ok {
		perTime = &spec.ChartBucketPerTime{PerType: map[string]*spec.PointPerEventType{}}
		c.out.Buckets[bucket] = perTime
	}

	point, ok := perTime.PerType[m.EventType]
	if !ok {
		point = &spec.PointPerEventType{
			EventType: m.EventType,
		}
		perTime.PerType[m.EventType] = point
	}

	fk := FKV{m.ForeignId, bucket, m.EventType, m.ForeignType}
	userFoundInSameBucket := c.fkv[fk]
	if !userFoundInSameBucket {
		point.CountUnique++
		c.fkv[fk] = true
	}
	point.Count++
}
