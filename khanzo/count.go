package main

import (
	"fmt"
	"html/template"
	"net/http"
	"sort"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/jackdoe/blackrock/jubei/disk"
	"github.com/jackdoe/blackrock/khanzo/chart"
	"github.com/jackdoe/blackrock/orgrim/spec"
)

type ByKV []*spec.KV

func (a ByKV) Len() int      { return len(a) }
func (a ByKV) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a ByKV) Less(i, j int) bool {
	if a[i].Key == a[j].Key {
		return a[i].Value < a[j].Value
	}
	return a[i].Key < a[j].Key
}

type Counter struct {
	Search       map[uint64]map[string]uint32 `json:"search"`
	Count        map[uint64]map[string]uint32 `json:"count"`
	Foreign      map[uint64]map[string]uint32 `json:"foreign"`
	EventTypes   map[uint64]uint32            `json:"event_types"`
	Sample       []Hit                        `json:"sample"`
	Total        uint64                       `json:"total"`
	pd           *disk.PersistedDictionary
	contextCache *ContextCache
	sampleSize   int
}

func NewCounter(pd *disk.PersistedDictionary, contextCache *ContextCache, sampleSize int) *Counter {
	return &Counter{
		Search:       map[uint64]map[string]uint32{},
		Count:        map[uint64]map[string]uint32{},
		Foreign:      map[uint64]map[string]uint32{},
		EventTypes:   map[uint64]uint32{},
		Sample:       []Hit{},
		Total:        0,
		sampleSize:   sampleSize,
		pd:           pd,
		contextCache: contextCache,
	}
}

func (c *Counter) Prettify() *CountedResult {
	out := &CountedResult{
		contextCache: c.contextCache,
		pd:           c.pd,
	}
	out.Search = statsForMapMap(c.pd, c.Search)
	out.Count = statsForMapMap(c.pd, c.Count)

	out.Foreign = statsForMapMap(c.pd, c.Foreign)

	resolved := map[string]uint32{}
	for k, v := range c.EventTypes {
		resolved[c.pd.ReverseResolve(k)] = v
	}
	out.EventTypes = statsForMap("event_type", resolved)
	out.Sample = c.Sample
	out.TotalCount = int64(c.Total)
	return out
}

func (c *Counter) Add(offset int64, foreignId, foreignType uint64, p *spec.PersistedMetadata) {
	for i := 0; i < len(p.SearchKeys); i++ {
		k := p.SearchKeys[i]
		v := p.SearchValues[i]
		m, ok := c.Search[k]
		if !ok {
			m = map[string]uint32{}
			c.Search[k] = m
		}
		m[v]++
	}

	for i := 0; i < len(p.CountKeys); i++ {
		k := p.CountKeys[i]
		v := p.CountValues[i]
		m, ok := c.Count[k]
		if !ok {
			m = map[string]uint32{}
			c.Count[k] = m
		}
		m[v]++
	}
	m, ok := c.Foreign[foreignType]
	if !ok {
		m = map[string]uint32{}
		c.Foreign[foreignType] = m
	}
	m[p.ForeignId]++
	c.EventTypes[p.EventType]++
	c.Total++

	if len(c.Sample) < c.sampleSize {
		hit := toHit(c.contextCache, c.pd, offset, foreignId, foreignType, p)
		c.Sample = append(c.Sample, hit)
	}
}

type PerValue struct {
	Value string `json:"value" yaml:"value"`
	Count int64  `json:"count"`
}

type PerKey struct {
	Key        string     `json:"key"`
	Values     []PerValue `json:"values"`
	TotalCount int64      `json:"total"`
}

type CountedResult struct {
	EventTypes   *PerKey   `json:"event_types"`
	Foreign      []*PerKey `json:"foreign"`
	Search       []*PerKey `json:"search"`
	Count        []*PerKey `json:"count"`
	Sample       []Hit     `json:"sample"`
	TotalCount   int64     `json:"total"`
	contextCache *ContextCache
	pd           *disk.PersistedDictionary
}

func (cr *CountedResult) VW(c *gin.Context) {
	c.JSON(400, gin.H{"error": "scan does not support vw output"})
}

func (cr *CountedResult) String(c *gin.Context) {
	t := cr.prettyCategoryStats()
	c.String(200, t)
}

type Breadcrumb struct {
	Base  string
	Exact string
}

func (cr *CountedResult) HTML(c *gin.Context) {
	url := strings.TrimRight(c.Request.URL.Path, "/")
	splitted := strings.Split(url, "/")
	crumbs := []Breadcrumb{}
	for i := 0; i < len(splitted[3:]); i++ {
		v := splitted[i+3]
		p := strings.Join(splitted[:i+3], "/")
		if len(v) > 0 {
			crumbs = append(crumbs, Breadcrumb{Base: p, Exact: v})
		}
	}
	if url == "/scan/html" {
		url = "/scan/html/"
	}

	c.HTML(http.StatusOK, "/html/t/index.tmpl", map[string]interface{}{"Crumbs": crumbs, "Stats": cr, "BaseUrl": url, "QueryString": template.URL(c.Request.URL.RawQuery)})
}

func (cr *CountedResult) prettyCategoryStats() string {
	makers := prettyStats(cr.contextCache, "FOREIGN", cr.Foreign)
	types := prettyStats(nil, "EVENT_TYPES", []*PerKey{cr.EventTypes})
	properties := prettyStats(nil, "COUNT", cr.Count)
	tags := prettyStats(cr.contextCache, "SEARCH", cr.Search)
	out := fmt.Sprintf("%s%s%s%s\n", makers, types, tags, properties)
	out += chart.Banner("SAMPLE")
	for _, h := range cr.Sample {
		out += fmt.Sprintf("%s\n", h.String())
	}
	return out
}

func statsForMap(key string, values map[string]uint32) *PerKey {
	tk := &PerKey{
		Key: key,
	}
	for value, count := range values {
		tk.TotalCount += int64(count)
		tk.Values = append(tk.Values, PerValue{Value: value, Count: int64(count)})
	}
	sort.Slice(tk.Values, func(i, j int) bool {
		if tk.Values[j].Count == tk.Values[i].Count {
			return tk.Values[i].Value < tk.Values[j].Value
		}
		return tk.Values[j].Count < tk.Values[i].Count
	})

	return tk
}

func statsForMapMap(pd *disk.PersistedDictionary, input map[uint64]map[string]uint32) []*PerKey {
	out := []*PerKey{}
	for key, values := range input {
		out = append(out, statsForMap(pd.ReverseResolve(key), values))
	}
	sort.Slice(out, func(i, j int) bool {
		if out[j].TotalCount == out[i].TotalCount {
			return out[i].Key < out[j].Key
		}

		return out[j].TotalCount < out[i].TotalCount
	})
	return out
}

func prettyStats(c *ContextCache, title string, stats []*PerKey) string {
	if stats == nil {
		return ""
	}

	sort.Slice(stats, func(i, j int) bool {
		return stats[j].TotalCount < stats[i].TotalCount
	})

	out := []string{}
	pad := "    "
	width := 80 - len(pad)
	total := int64(0)
	for _, t := range stats {
		for _, v := range t.Values {
			total += v.Count
		}
	}

	for _, t := range stats {
		if t == nil {
			continue
		}
		x := []float64{}
		y := []string{}
		sort.Slice(t.Values, func(i, j int) bool {
			return t.Values[j].Count < t.Values[i].Count
		})

		for _, v := range t.Values {
			x = append(x, float64(v.Count))
			y = append(y, v.Value)
		}
		percent := float64(100) * float64(t.TotalCount) / float64(total)
		out = append(out, fmt.Sprintf("« %s (%d) » total: %d, %.2f%%\n%s", t.Key, len(y), t.TotalCount, percent, chart.HorizontalBar(x, y, '▒', width, pad, 50)))
	}

	return fmt.Sprintf("%s%s", chart.Banner(title), strings.Join(out, "\n\n--------\n\n"))
}
