package main

import (
	"fmt"
	"html/template"
	"net/http"
	"sort"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/jackdoe/blackrock/khanzo/chart"
	"github.com/jackdoe/blackrock/khanzo/stat"
	"github.com/jackdoe/blackrock/orgrim/spec"
)

type ConvertedCache map[uint32]map[string]map[string]uint32

func NewConvertedCache() *ConvertedCache {
	return &ConvertedCache{}
}

type ConvertedPerVariant struct {
	ConvertingUsers    uint32
	NotConvertingUsers uint32
	Users              uint32
	Convertions        uint32
}
type ConfidenceSignificant struct {
	Confidence  float64
	Significant bool
}

func (x *ConvertedCache) Confidence(pv []ConvertedPerVariant) ConfidenceSignificant {
	sv := []stat.Variant{}
	for _, v := range pv {
		sv = append(sv, stat.Variant{
			Visits:      v.ConvertingUsers + v.NotConvertingUsers,
			Convertions: v.ConvertingUsers,
		})
	}

	_, confidence, significant := stat.P(sv)
	return ConfidenceSignificant{Confidence: confidence, Significant: significant}
}
func (x *ConvertedCache) TotalConvertingUsers() []ConvertedPerVariant {
	c := *x
	maxVariant := uint32(0)
	for variant, _ := range c {
		if variant > maxVariant {
			maxVariant = variant
		}
	}
	out := make([]ConvertedPerVariant, maxVariant+1)

	for vid, variant := range c {
		sum := uint32(0)
		usersC := uint32(0)
		usersNC := uint32(0)
		for _, pid := range variant {
			for _, u := range pid {
				if u == 0 {
					usersNC++
				} else {
					usersC++
				}
				sum += u
			}
		}
		out[vid] = ConvertedPerVariant{ConvertingUsers: usersC, NotConvertingUsers: usersNC, Convertions: sum, Users: usersC + usersNC}
	}

	return out
}

func (c ConvertedCache) SetConverted(n uint32, variant uint32, ftype, fid string) {
	pv, ok := c[variant]
	if !ok {
		pv = map[string]map[string]uint32{}
		c[variant] = pv
	}

	v, ok := pv[ftype]
	if !ok {
		v = map[string]uint32{}
		pv[ftype] = v
	}
	v[fid] += n
}

type CountPerValue struct {
	CountEventsFromConverterVariant []uint32
	Count                           uint32
	Key                             string
}

func (c *CountPerValue) Add(variant uint32, converted bool) {
	if converted {
		if variant >= uint32(len(c.CountEventsFromConverterVariant)) {
			x := make([]uint32, variant+1)
			for i, v := range c.CountEventsFromConverterVariant {
				x[i] = v
			}
			c.CountEventsFromConverterVariant = x
		}
		c.CountEventsFromConverterVariant[variant]++
	}
	c.Count++
}

type CountPerKey struct {
	PerValue                 map[string]*CountPerValue
	Count                    uint32
	CountEventsFromConverter uint32
	Key                      string
}

func (c *CountPerKey) Sorted() []*CountPerValue {
	out := []*CountPerValue{}
	for _, v := range c.PerValue {
		out = append(out, v)
	}

	sort.Slice(out, func(i, j int) bool {
		if out[j].Count == out[i].Count {
			return out[i].Key < out[j].Key
		}
		return out[j].Count < out[i].Count
	})
	return out
}

func (c *CountPerKey) Add(value string, variant uint32, converted bool) {
	c.Count++
	if converted {
		c.CountEventsFromConverter++
	}
	pv, ok := c.PerValue[value]
	if !ok {
		pv = &CountPerValue{Key: value}
		c.PerValue[value] = pv
	}
	pv.Add(variant, converted)
}

func NewCountPerKey(s string) *CountPerKey {
	return &CountPerKey{Key: s, PerValue: map[string]*CountPerValue{}}
}

type Counter struct {
	Search                        map[string]*CountPerKey `json:"search"`
	Count                         map[string]*CountPerKey `json:"count"`
	Foreign                       map[string]*CountPerKey `json:"foreign"`
	EventTypes                    *CountPerKey            `json:"event_types"`
	Sample                        map[uint32][]Hit        `json:"sample"`
	TotalCount                    uint32                  `json:"total"`
	TotalCountEventsFromConverter uint32                  `json:"total_events_from_converter"`
	ConvertedCache                *ConvertedCache         `json:"convertions"`
	Chart                         *Chart
	Whitelist                     map[string]bool
	Sections                      map[string]uint32 `json:"sections"`
}

func NewCounter(conv *ConvertedCache, Whitelist map[string]bool, chart *Chart) *Counter {
	return &Counter{
		Search:         map[string]*CountPerKey{},
		Count:          map[string]*CountPerKey{},
		Foreign:        map[string]*CountPerKey{},
		EventTypes:     NewCountPerKey("event_type"),
		Sample:         map[uint32][]Hit{},
		ConvertedCache: conv,
		Chart:          chart,
		TotalCount:     0,
		Whitelist:      Whitelist,
		Sections:       map[string]uint32{},
	}
}
func (c *Counter) IsWhitelisted(s string) bool {
	if c.Whitelist == nil {
		return false
	}
	return c.Whitelist[s]
}

type SortedSection struct {
	Key   string
	Count uint32
}

func (c *Counter) SortedSections() []SortedSection {
	out := []SortedSection{}
	for k, v := range c.Sections {
		out = append(out, SortedSection{Key: k, Count: v})
	}

	sort.Slice(out, func(i, j int) bool {
		return out[i].Key < out[j].Key
	})

	return out
}

func (c *Counter) SortedKeys(what map[string]*CountPerKey) []*CountPerKey {
	out := []*CountPerKey{}
	for _, v := range what {
		out = append(out, v)
	}

	sort.Slice(out, func(i, j int) bool {
		if out[j].Count == out[i].Count {
			return out[i].Key < out[j].Key
		}
		return out[j].Count < out[j].Count
	})
	return out
}

func (c *Counter) Add(converted bool, variant uint32, p *spec.Metadata) {
	if c.ConvertedCache != nil {
		c.TotalCountEventsFromConverter++
	}
	c.TotalCount++
	for _, kv := range p.Search {
		k := kv.Key
		v := kv.Value
		if k == "ip" {
			continue
		}
		c.Sections[k]++
		if !c.IsWhitelisted(k) {
			continue
		}

		xm, ok := c.Search[k]
		if !ok {
			xm = NewCountPerKey(k)
			c.Search[k] = xm
		}
		xm.Add(v, variant, converted)
	}

	for _, kv := range p.Count {
		k := kv.Key
		v := kv.Value
		c.Sections[k]++
		if !c.IsWhitelisted(k) {
			continue
		}

		xm, ok := c.Count[k]
		if !ok {
			xm = NewCountPerKey(k)
			c.Count[k] = xm
		}
		xm.Add(v, 0, false)
	}
	{
		xm, ok := c.Foreign[p.ForeignType]
		if !ok {
			xm = NewCountPerKey(p.ForeignType)
			c.Foreign[p.ForeignType] = xm
		}
		xm.Add(p.ForeignId, 0, false)
	}

	c.EventTypes.Add(p.EventType, 0, false)
	if c.Chart != nil {
		c.Chart.Add(p)
	}
}

func (c *Counter) String(context *gin.Context) {
	makers := prettyStats("FOREIGN", c.SortedKeys(c.Foreign))
	types := prettyStats("EVENT_TYPES", []*CountPerKey{c.EventTypes})
	properties := prettyStats("COUNT", c.SortedKeys(c.Count))
	tags := prettyStats("SEARCH", c.SortedKeys(c.Search))
	out := fmt.Sprintf("%s%s%s%s\n", makers, types, tags, properties)
	out += chart.Banner("SAMPLE")
	for _, samples := range c.Sample {
		for _, h := range samples {
			out += fmt.Sprintf("%s\n", h.String())
		}
	}
	context.String(200, out)
}

func prettyStats(title string, stats []*CountPerKey) string {
	if stats == nil {
		return ""
	}

	out := []string{}
	pad := "    "
	width := 80 - len(pad)
	total := uint32(0)
	for _, t := range stats {
		for _, v := range t.PerValue {
			total += v.Count
		}
	}

	for _, t := range stats {
		if t == nil {
			continue
		}
		x := []float64{}
		y := []string{}
		sorted := t.Sorted()
		for _, v := range sorted {
			x = append(x, float64(v.Count))
			y = append(y, v.Key)
		}
		percent := float64(100) * float64(t.Count) / float64(total)
		out = append(out, fmt.Sprintf("« %s (%d) » total: %d, %.2f%%\n%s", t.Key, len(y), t.Count, percent, chart.HorizontalBar(x, y, '▒', width, pad, 50)))
	}

	return fmt.Sprintf("%s%s", chart.Banner(title), strings.Join(out, "\n\n--------\n\n"))
}

type Breadcrumb struct {
	Base  string
	Exact string
}
type Breadcrumbs []Breadcrumb

func (base Breadcrumbs) RemoveQuery(kv string) string {
	out := []string{}
	for _, crumb := range base {
		if crumb.Exact != kv {
			out = append(out, crumb.Exact)
		}
	}
	return "/scan/html/" + strings.Join(out, "/")
}

func (base Breadcrumbs) NegateQuery(kv string) string {
	out := []string{}
	toggle := "-"
	if strings.HasPrefix(kv, "-") {
		toggle = ""
	}
	for _, crumb := range base {
		if crumb.Exact == kv {
			out = append(out, toggle+strings.TrimLeft(crumb.Exact, "-"))
		} else {
			out = append(out, crumb.Exact)
		}
	}
	return "/scan/html/" + strings.Join(out, "/")
}

func NewBreadcrumb(url string) Breadcrumbs {
	splitted := strings.Split(url, "/")
	crumbs := []Breadcrumb{}
	for i := 0; i < len(splitted[3:]); i++ {
		v := splitted[i+3]
		p := strings.Join(splitted[:i+3], "/")
		if len(v) > 0 {
			crumbs = append(crumbs, Breadcrumb{Base: p, Exact: v})
		}
	}
	return Breadcrumbs(crumbs)
}

func (c *Counter) HTML(context *gin.Context) {
	url := context.Request.URL.Path
	crumbs := NewBreadcrumb(url)
	if url == "/scan/html" {
		url = "/scan/html/"
	}

	context.HTML(http.StatusOK, "/html/t/index.tmpl", map[string]interface{}{"Crumbs": crumbs, "Stats": c, "BaseUrl": url, "QueryString": template.URL(context.Request.URL.RawQuery)})
}
