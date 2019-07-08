package main

import (
	"flag"
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"os"
	"path"
	"runtime"
	"sort"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gogo/protobuf/proto"
	"github.com/jackdoe/blackrock/jubei/disk"
	"github.com/jackdoe/blackrock/khanzo/chart"
	"github.com/jackdoe/blackrock/orgrim/spec"
	auth "github.com/jackdoe/gin-basic-auth-dynamic"
	log "github.com/sirupsen/logrus"
)

type QueryRequest struct {
	Query            interface{} `json:"query"`
	Size             int         `json:"size"`
	DecodeMetadata   bool        `json:"decode_metadata"`
	ScanMaxDocuments int64       `json:"scan_max_documents"`
}
type KafkaOffset struct {
	Offset    uint64 `json:"offset,omitempty"`
	Partition uint32 `json:"partition,omitempty"`
}

type Hit struct {
	Score       float32        `json:"score,omitempty"`
	ID          int64          `json:"id,omitempty"`
	Maker       string         `json:"maker,omitempty"`
	Metadata    *spec.Metadata `json:"metadata,omitempty"`
	KafkaOffset *KafkaOffset   `json:"kafka,omitempty"`
}

func (h Hit) String(link bool) string {
	out := []string{}
	m := h.Metadata
	t := time.Unix(m.CreatedAtNs/1000000000, 0)
	if !link {
		out = append(out, fmt.Sprintf("%s\n%s\n%s", m.Maker, m.Type, t.Format(time.UnixDate)))
	} else {
		out = append(out, fmt.Sprintf("<a href='/scan/html/maker:%s'>%s</a>", m.Maker, m.Maker))
		out = append(out, fmt.Sprintf("<a href='/scan/html/maker:%s/%s:%s'>%s</a>", m.Maker, "type", m.Type, m.Type))
		out = append(out, fmt.Sprintf("%s", t.Format(time.UnixDate)))
	}
	keys := []string{}
	for k, _ := range m.Tags {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		v := m.Tags[k]
		if !link {
			out = append(out, fmt.Sprintf("  %-30s: %s", k, v))
		} else {
			out = append(out, fmt.Sprintf("  %-30s: <a href='/scan/html/maker:%s/%s:%s'>%s</a>", k, m.Maker, k, v, v))
		}
	}
	keys = []string{}
	for k, _ := range m.Properties {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		v := m.Properties[k]
		out = append(out, fmt.Sprintf("  %-30s: %s", k, v))
	}

	return strings.Join(out, "\n") + "\n\n"
}

type QueryResponse struct {
	Total int64 `json:"total"`
	Hits  []Hit `json:"hits"`
}

func (qr *QueryResponse) String(c *gin.Context) {
	c.YAML(200, qr)
}

func (qr *QueryResponse) HTML(c *gin.Context) {
	c.YAML(200, qr)
}

func NewTermQuery(inverted *disk.InvertedWriter, dictionary *disk.PersistedDictionary, maxDocuments int64, tagKey string, tagValue string) Query {
	s := fmt.Sprintf("%s:%s", tagKey, tagValue)
	tk, ok := dictionary.Resolve(tagKey)
	if !ok {
		log.Warnf("error reading key for %s", tagKey)
		return NewTerm(s, []int64{})
	}

	return NewTerm(s, inverted.Read(maxDocuments, tk, tagValue))
}

func getScoredHit(forward *disk.ForwardWriter, dictionary *disk.PersistedDictionary, did int64, decodeMetadata bool) (Hit, error) {
	maker, data, _, err := forward.Read(uint64(did), decodeMetadata)
	if err != nil {
		return Hit{}, err
	}
	if decodeMetadata {
		var p spec.PersistedMetadata
		err := proto.Unmarshal(data, &p)
		if err != nil {
			return Hit{}, err
		}
		return toHit(dictionary, did, maker, nil), nil
	}
	return toHit(dictionary, did, maker, nil), nil
}

func toHit(dictionary *disk.PersistedDictionary, did int64, maker uint64, p *spec.PersistedMetadata) Hit {
	hit := Hit{
		ID: did,
	}
	hit.Maker = dictionary.ReverseResolve(maker)
	if p == nil {
		return hit
	}
	pretty := &spec.Metadata{
		Maker:      hit.Maker,
		Type:       dictionary.ReverseResolve(p.Type),
		Tags:       map[string]string{},
		Properties: map[string]string{},
	}
	hit.KafkaOffset = &KafkaOffset{Offset: p.Offset, Partition: p.Partition}
	for k, v := range p.Tags {
		tk := dictionary.ReverseResolve(k)
		pretty.Tags[tk] = v
	}
	for k, v := range p.Properties {
		tk := dictionary.ReverseResolve(k)
		pretty.Properties[tk] = v
	}

	hit.Metadata = pretty
	return hit
}

type Renderable interface {
	String(c *gin.Context)
	HTML(c *gin.Context)
}

func Render(c *gin.Context, x Renderable) {
	format := c.Param("format")
	if format == "" {
		format = c.Query("format")
	}
	if format == "json" {
		c.JSON(200, x)
	} else if format == "yaml" {
		c.YAML(200, x)
	} else if format == "html" {
		x.HTML(c)
	} else {
		x.String(c)
	}
}

type Counter struct {
	Tags       map[uint64]map[string]uint32 `json:"tags"`
	Properties map[uint64]map[string]uint32 `json:"properties"`
	Makers     map[uint64]uint32            `json:"makers"`
	Types      map[uint64]uint32            `json:"types"`
	Sample     []Hit                        `json:"sample"`
	Total      uint64                       `json:"total"`
	pd         *disk.PersistedDictionary
}

func NewCounter(pd *disk.PersistedDictionary) *Counter {
	return &Counter{
		Tags:       map[uint64]map[string]uint32{},
		Properties: map[uint64]map[string]uint32{},
		Makers:     map[uint64]uint32{},
		Types:      map[uint64]uint32{},
		Sample:     []Hit{},
		Total:      0,
		pd:         pd,
	}
}

func (c *Counter) Prettify() *CountedResult {
	out := &CountedResult{}
	out.Tags = statsForMapMap(c.pd, c.Tags)
	out.Properties = statsForMapMap(c.pd, c.Properties)

	resolved := map[string]uint32{}
	for k, v := range c.Makers {
		resolved[c.pd.ReverseResolve(k)] = v
	}
	out.Makers = statsForMap("maker", resolved)
	resolved = map[string]uint32{}
	for k, v := range c.Types {
		resolved[c.pd.ReverseResolve(k)] = v
	}
	out.Types = statsForMap("type", resolved)
	out.Sample = c.Sample
	out.TotalCount = int64(c.Total)
	return out
}

func (c *Counter) Add(offset int64, maker uint64, p *spec.PersistedMetadata) {
	for k, v := range p.Tags {
		m, ok := c.Tags[k]
		if !ok {
			m = map[string]uint32{}
			c.Tags[k] = m
		}
		m[v]++
	}
	for k, v := range p.Properties {
		m, ok := c.Properties[k]
		if !ok {
			m = map[string]uint32{}
			c.Properties[k] = m
		}
		m[v]++
	}

	c.Makers[maker]++
	c.Types[p.Type]++
	c.Total++
	if len(c.Sample) < 100 {
		hit := toHit(c.pd, offset, maker, p)
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
	Makers     *PerKey   `json:"makers"`
	Types      *PerKey   `json:"types"`
	Tags       []*PerKey `json:"tags"`
	Properties []*PerKey `json:"properties"`
	Sample     []Hit     `json:"sample"`
	TotalCount int64     `json:"total"`
}

func (cr *CountedResult) String(c *gin.Context) {
	t := cr.prettyCategoryStats(false, strings.TrimRight(c.Request.URL.Path, "/"))
	c.String(200, t)
}

func (cr *CountedResult) HTML(c *gin.Context) {
	url := strings.TrimRight(c.Request.URL.Path, "/")
	t := cr.prettyCategoryStats(true, url)
	splitted := strings.Split(url, "/")

	crumbs := []string{`<a href="/scan/html/">/</a>`}

	for i := 0; i < len(splitted[3:]); i++ {
		v := splitted[i+3]
		p := strings.Join(splitted[:i+3], "/")
		crumbs = append(crumbs, fmt.Sprintf(`%s[<a href="/scan/html/%s">@</a> <a href="%s/%s">=</a>]`, v, v, p, v))
	}
	cs := strings.Join(crumbs, ", ")
	c.Data(200, "text/html; charset=utf8", []byte(fmt.Sprintf("<pre>%s\n%s</pre>", cs, t)))
}

func (cr CountedResult) prettyCategoryStats(link bool, base string) string {
	makers := prettyStats("MAKERS", []*PerKey{cr.Makers}, link, base)
	types := prettyStats("TYPES", []*PerKey{cr.Types}, link, base)
	properties := prettyStats("PROPERTIES", cr.Properties, false, base)
	tags := prettyStats("TAGS", cr.Tags, link, base)
	out := fmt.Sprintf("%s%s%s%s\n", makers, types, tags, properties)
	out += chart.Banner("SAMPLE")
	for _, h := range cr.Sample {
		out += fmt.Sprintf("%s\n", h.String(link))
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
		return out[j].TotalCount < out[i].TotalCount
	})
	return out
}

func prettyStats(title string, stats []*PerKey, link bool, base string) string {
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
		y := []chart.Label{}
		sort.Slice(t.Values, func(i, j int) bool {
			return t.Values[j].Count < t.Values[i].Count
		})

		for _, v := range t.Values {
			x = append(x, float64(v.Count))
			linkified := v.Value
			if link {
				linkified = fmt.Sprintf("<a href='%s/%s:%s'>%s</a>", base, t.Key, v.Value, v.Value)
			}

			y = append(y, chart.Label{Display: linkified, Len: len(v.Value)})
		}
		percent := float64(100) * float64(t.TotalCount) / float64(total)
		out = append(out, fmt.Sprintf("« %s » total: %d, %.2f%%\n%s", t.Key, t.TotalCount, percent, chart.HorizontalBar(x, y, '▒', width, pad, 50)))
	}

	return fmt.Sprintf("%s%s", chart.Banner(title), strings.Join(out, "\n\n--------\n\n"))
}

func main() {
	var proot = flag.String("root", "/blackrock", "root directory for the files (root/topic/partition)")
	var dataTopic = flag.String("topic-data", "blackrock-data", "topic for the data")
	var basicAuth = flag.String("basic-auth", "blackrock:orgrim", "basic user and password, leave empty for no auth")
	var verbose = flag.Bool("verbose", false, "print info level logs to stdout")
	var bind = flag.String("bind", ":9002", "bind to")
	flag.Parse()
	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()

	root := path.Join(*proot, *dataTopic)
	os.MkdirAll(root, 0700)
	forward, err := disk.NewForwardWriter(root, "main")
	if err != nil {
		log.Fatal(err)
	}

	inverted, err := disk.NewInvertedWriter(root, 0)
	if err != nil {
		log.Fatal(err)
	}

	dictionary, err := disk.NewPersistedDictionary(root)
	if err != nil {
		log.Fatal(err)
	}
	dictionary.Close()

	if *verbose {
		log.SetLevel(log.InfoLevel)
	} else {
		gin.SetMode(gin.ReleaseMode)
		log.SetLevel(log.WarnLevel)
	}

	r := gin.Default()
	r.GET("/health", func(c *gin.Context) {
		c.String(200, "OK")
	})
	if *basicAuth != "" {
		splitted := strings.Split(*basicAuth, ":")
		if len(splitted) != 2 {
			log.Fatalf("expected -basic-auth user:pass, got %s", *basicAuth)
		}
		u := splitted[0]
		p := splitted[1]

		r.Use(auth.BasicAuth(func(context *gin.Context, realm, user, pass string) auth.AuthResult {
			ok := user == u && pass == p
			return auth.AuthResult{Success: ok, Text: "not authorized"}
		}))

	}
	go func() {
		for {
			tmp, err := disk.NewPersistedDictionary(root)
			if err != nil {
				log.Fatal(err)
			}
			tmp.Close()
			dictionary = tmp
			runtime.GC()
			time.Sleep(1 * time.Minute)
		}

	}()

	search := func(qr QueryRequest) (*QueryResponse, error) {
		query, err := fromJson(qr.Query, func(k, v string) Query {
			return NewTermQuery(inverted, dictionary, qr.ScanMaxDocuments, k, v)
		})

		if err != nil {
			return nil, err
		}
		out := &QueryResponse{
			Hits:  []Hit{},
			Total: 0,
		}

		for query.Next() != NO_MORE {
			did := query.GetDocId()
			out.Total++
			if qr.Size == 0 {
				continue
			}

			score := query.Score()
			doInsert := false
			var hit Hit
			if len(out.Hits) < qr.Size {
				hit, err = getScoredHit(forward, dictionary, did, qr.DecodeMetadata)
				hit.Score = score
				if err != nil {
					// possibly corrupt forward index, igore, error is already printed
					continue
				}
				out.Hits = append(out.Hits, hit)
				doInsert = true
			} else if out.Hits[len(out.Hits)-1].Score < hit.Score {
				doInsert = true
				hit, err = getScoredHit(forward, dictionary, did, qr.DecodeMetadata)
				hit.Score = score
				if err != nil {
					// possibly corrupt forward index, igore, error is already printed
					continue
				}
			}
			if doInsert {
				for i := 0; i < len(out.Hits); i++ {
					if out.Hits[i].Score < hit.Score {
						copy(out.Hits[i+1:], out.Hits[i:])
						out.Hits[i] = hit
						break
					}
				}
			}
		}
		return out, nil
	}

	r.POST("/search/:format", func(c *gin.Context) {
		var qr QueryRequest
		if err := c.ShouldBindJSON(&qr); err != nil {
			c.JSON(400, gin.H{"error": err.Error()})
			return
		}

		out, err := search(qr)
		if err != nil {
			c.JSON(400, gin.H{"error": err.Error()})
			return
		}
		Render(c, out)
	})

	r.GET("/scan/:format/*query", func(c *gin.Context) {
		counter := NewCounter(dictionary)
		var p spec.PersistedMetadata
		queryPath := strings.Trim(c.Param("query"), "/")

		if queryPath != "" {
			query, err := fromString(strings.Replace(queryPath, "/", " AND ", -1), func(k, v string) Query {
				return NewTermQuery(inverted, dictionary, 100000, k, v)
			})
			if err != nil {
				c.JSON(400, gin.H{"error": err.Error()})
				return
			}
			for query.Next() != NO_MORE {
				did := query.GetDocId()
				maker, data, offset, err := forward.Read(uint64(did), true)
				if err != nil {
					c.JSON(400, gin.H{"error": err.Error()})
					return
				}

				err = proto.Unmarshal(data, &p)
				if err != nil {
					c.JSON(400, gin.H{"error": err.Error()})
					return
				}
				counter.Add(int64(offset), maker, &p)
			}
		} else {
			back := uint64(0)
			size, err := forward.Size()
			if err != nil {
				c.JSON(400, gin.H{"error": err.Error()})
				return
			}

			max := uint64(10000000)
			if size > max {
				back = size - uint64(max)
			}

			err = forward.Scan(back, true, func(offset uint64, maker uint64, data []byte) error {
				err := proto.Unmarshal(data, &p)
				if err != nil {
					return err
				}
				counter.Add(int64(offset), maker, &p)
				return nil
			})

			if err != nil {
				c.JSON(400, gin.H{"error": err.Error()})
				return
			}
		}
		Render(c, counter.Prettify())
	})

	log.Panic(r.Run(*bind))
}
