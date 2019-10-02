package main

import (
	"sort"
	"strings"
	"time"

	"github.com/guptarohit/asciigraph"
	"github.com/rekki/blackrock/cmd/khanzo/chart"
	"github.com/rekki/blackrock/cmd/orgrim/spec"
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
		TimeStart:      (uint32(dates[0].Unix()) / timebucket) * timebucket,
		TimeEnd:        (uint32(dates[len(dates)-1].AddDate(0, 0, 1).Unix()) / timebucket) * timebucket,
	}
}

func (c *Chart) String(n int) string {
	top := map[string]uint32{}

	for _, v := range c.PerTimePerType {
		for _, vv := range v {
			top[vv.EventType]++
		}
	}

	topKeys := []string{}
	for k := range top {
		topKeys = append(topKeys, k)
	}

	sort.Slice(topKeys, func(i, j int) bool {
		return top[topKeys[j]] < top[topKeys[i]]
	})

	if len(topKeys) > n {
		topKeys = topKeys[:n]
	}
	out := []string{}
	for _, k := range topKeys {
		data := []float64{}

		for i := c.TimeStart; i < c.TimeEnd; i += c.TimeBucket {
			pb, ok := c.PerTimePerType[i]
			if !ok {
				data = append(data, 0)
			} else {
				pt, ok := pb[k]
				if !ok {
					data = append(data, 0)
				} else {
					data = append(data, float64(pt.Count))
				}
			}

		}

		graph := asciigraph.Plot(data, asciigraph.Caption(k), asciigraph.Height(10), asciigraph.Width(72))
		out = append(out, graph)
	}

	return chart.Banner("CHART") + strings.Join(out, "\n\n")
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
