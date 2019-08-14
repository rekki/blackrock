package main

import (
	"fmt"
	"html/template"
	"net/http"
	"sort"
	"strings"

	"github.com/gin-gonic/gin"
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
	Search     map[string]map[string]uint32 `json:"search"`
	Count      map[string]map[string]uint32 `json:"count"`
	Foreign    map[string]map[string]uint32 `json:"foreign"`
	EventTypes map[string]uint32            `json:"event_types"`
	Sample     []Hit                        `json:"sample"`
	Total      uint64                       `json:"total"`
}

func NewCounter() *Counter {
	return &Counter{
		Search:     map[string]map[string]uint32{},
		Count:      map[string]map[string]uint32{},
		Foreign:    map[string]map[string]uint32{},
		EventTypes: map[string]uint32{},
		Sample:     []Hit{},
		Total:      0,
	}
}

func (c *Counter) Prettify() *CountedResult {
	out := &CountedResult{}
	out.Search = statsForMapMap(c.Search)
	out.Count = statsForMapMap(c.Count)
	out.Foreign = statsForMapMap(c.Foreign)

	out.EventTypes = statsForMap("event_type", c.EventTypes)
	out.Sample = c.Sample
	out.TotalCount = int64(c.Total)
	return out
}

func (c *Counter) Add(p *spec.Metadata) {
	for _, kv := range p.Search {
		k := kv.Key
		v := kv.Value
		m, ok := c.Search[k]
		if !ok {
			m = map[string]uint32{}
			c.Search[k] = m
		}
		m[v]++
	}

	for _, kv := range p.Count {
		k := kv.Key
		v := kv.Value

		m, ok := c.Count[k]
		if !ok {
			m = map[string]uint32{}
			c.Count[k] = m
		}
		m[v]++
	}
	m, ok := c.Foreign[p.ForeignType]
	if !ok {
		m = map[string]uint32{}
		c.Foreign[p.ForeignType] = m
	}
	m[p.ForeignId]++
	c.EventTypes[p.EventType]++
	c.Total++
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
	EventTypes *PerKey   `json:"event_types"`
	Foreign    []*PerKey `json:"foreign"`
	Search     []*PerKey `json:"search"`
	Count      []*PerKey `json:"count"`
	Sample     []Hit     `json:"sample"`
	TotalCount int64     `json:"total"`
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
	url := c.Request.URL.Path
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
	makers := prettyStats("FOREIGN", cr.Foreign)
	types := prettyStats("EVENT_TYPES", []*PerKey{cr.EventTypes})
	properties := prettyStats("COUNT", cr.Count)
	tags := prettyStats("SEARCH", cr.Search)
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

func statsForMapMap(input map[string]map[string]uint32) []*PerKey {
	out := []*PerKey{}
	for key, values := range input {
		out = append(out, statsForMap(key, values))
	}
	sort.Slice(out, func(i, j int) bool {
		if out[j].TotalCount == out[i].TotalCount {
			return out[i].Key < out[j].Key
		}

		return out[j].TotalCount < out[i].TotalCount
	})
	return out
}

func prettyStats(title string, stats []*PerKey) string {
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
