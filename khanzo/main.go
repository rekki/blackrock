package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"html/template"
	"io/ioutil"
	"net/http"
	_ "net/http/pprof"
	"os"
	"reflect"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gogo/protobuf/proto"
	"github.com/jackdoe/blackrock/depths"
	"github.com/jackdoe/blackrock/jubei/disk"
	"github.com/jackdoe/blackrock/khanzo/chart"
	"github.com/jackdoe/blackrock/orgrim/spec"
	auth "github.com/jackdoe/gin-basic-auth-dynamic"
	log "github.com/sirupsen/logrus"

	"gopkg.in/yaml.v2"
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
	Score       float32         `json:"score,omitempty"`
	ID          int64           `json:"id,omitempty"`
	ForeignId   string          `json:"foreign_id,omitempty"`
	ForeignType string          `json:"foreign_type,omitempty"`
	Metadata    *spec.Metadata  `json:"metadata,omitempty"`
	KafkaOffset *KafkaOffset    `json:"kafka,omitempty"`
	Context     []*spec.Context `json:"context,omitempty"`
}

type ByKV []*spec.KV

func (a ByKV) Len() int      { return len(a) }
func (a ByKV) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a ByKV) Less(i, j int) bool {
	if a[i].Key == a[j].Key {
		return a[i].Value < a[j].Value
	}
	return a[i].Key < a[j].Key
}

type ByCtxName []*spec.Context

func (a ByCtxName) Len() int      { return len(a) }
func (a ByCtxName) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a ByCtxName) Less(i, j int) bool {
	if a[i].ForeignType != a[j].ForeignType {
		return a[i].ForeignType < a[j].ForeignType
	}
	return a[i].ForeignId < a[j].ForeignId
}

func (h Hit) String() string {
	out := []string{}
	m := h.Metadata
	t := time.Unix(m.CreatedAtNs/1000000000, 0)
	out = append(out, fmt.Sprintf("%s:%s\ntype:%s\n%s", m.ForeignType, m.ForeignId, m.EventType, t.Format(time.UnixDate)))
	for _, kv := range m.Tags {
		out = append(out, fmt.Sprintf("  %-30s: %s", kv.Key, kv.Value))
	}
	for _, kv := range m.Properties {
		out = append(out, fmt.Sprintf("  %-30s: %s", kv.Key, kv.Value))
	}

	for _, ctx := range h.Context {
		out = append(out, fmt.Sprintf("  %-30s:", fmt.Sprintf("@%s:%s", ctx.ForeignType, ctx.ForeignId)))
		for _, kv := range ctx.Properties {
			out = append(out, fmt.Sprintf("    %-28s: %s", kv.Key, kv.Value))
		}
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

func (qr *QueryResponse) VW(c *gin.Context) {
	labels := map[string]int{}

	for i := -1; i < 10; i++ {
		l := c.Query(fmt.Sprintf("label_%d", i))
		if l != "" {
			labels[l] = i
		}
	}
	if len(labels) == 0 {
		c.JSON(400, gin.H{"error": "no labels found, use ?label_1=some_event_type"})
		return
	}
	w := c.Writer
	for _, hit := range qr.Hits {
		m := hit.Metadata
		if m == nil {
			continue
		}
		label, ok := labels[m.EventType]
		if !ok {
			continue
		}

		w.Write([]byte(fmt.Sprintf("%d |%s %s ", label, hit.ForeignType, depths.CleanupVW(hit.ForeignId))))
		for _, kv := range m.Tags {
			w.Write([]byte(fmt.Sprintf("|%s %s ", kv.Key, depths.CleanupVW(kv.Value))))
		}
		for _, kv := range m.Properties {
			w.Write([]byte(fmt.Sprintf("|%s %s ", kv.Key, depths.CleanupVW(kv.Value))))
		}
		for _, ctx := range hit.Context {
			for _, kv := range ctx.Properties {
				w.Write([]byte(fmt.Sprintf("|%s_%s %s ", ctx.ForeignType, kv.Key, depths.CleanupVW(kv.Value))))
			}
		}
		w.Write([]byte{'\n'})
	}

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
	if maxDocuments == 0 {
		maxDocuments = 1000000
	}
	if maxDocuments == -1 {
		maxDocuments = 0
	}
	return NewTerm(s, inverted.Read(maxDocuments, tk, tagValue))
}

func getScoredHit(contextCache *ContextCache, forward *disk.ForwardWriter, dictionary *disk.PersistedDictionary, did int64, decodeMetadata bool) (Hit, error) {
	foreignId, foreignType, data, _, err := forward.Read(uint64(did), decodeMetadata)
	if err != nil {
		return Hit{}, err
	}
	if decodeMetadata {
		var p spec.PersistedMetadata
		err := proto.Unmarshal(data, &p)
		if err != nil {
			return Hit{}, err
		}
		return toHit(contextCache, dictionary, did, foreignId, foreignType, &p), nil
	}
	return toHit(contextCache, dictionary, did, foreignId, foreignType, nil), nil
}

func toContextDeep(seen map[uint64]map[string]bool, contextCache *ContextCache, dictionary *disk.PersistedDictionary, p *spec.PersistedContext) []*spec.Context {
	out := []*spec.Context{toContext(dictionary, p)}
	for i := 0; i < len(p.PropertyKeys); i++ {
		k := p.PropertyKeys[i]
		v := p.PropertyValues[i]

		m, ok := seen[k]
		if !ok {
			m = map[string]bool{}
			seen[k] = m
		}
		_, ok = m[v]
		if ok {
			continue
		}
		m[v] = true
		if px, ok := contextCache.Lookup(k, v, p.CreatedAtNs); ok {
			out = append(out, toContextDeep(seen, contextCache, dictionary, px)...)
		}
	}

	return out
}
func toContext(dictionary *disk.PersistedDictionary, p *spec.PersistedContext) *spec.Context {
	out := &spec.Context{
		CreatedAtNs: p.CreatedAtNs,
		ForeignId:   p.ForeignId,
		ForeignType: dictionary.ReverseResolve(p.ForeignType),
	}
	for i := 0; i < len(p.PropertyKeys); i++ {
		tk := dictionary.ReverseResolve(p.PropertyKeys[i])
		out.Properties = append(out.Properties, &spec.KV{Key: tk, Value: p.PropertyValues[i]})
	}
	sort.Sort(ByKV(out.Properties))
	return out
}

func toHit(contextCache *ContextCache, dictionary *disk.PersistedDictionary, did int64, foreignId, foreignType uint64, p *spec.PersistedMetadata) Hit {
	hit := Hit{
		ID: did,
	}
	hit.ForeignId = dictionary.ReverseResolve(foreignId)
	hit.ForeignType = dictionary.ReverseResolve(foreignType)
	if p == nil {
		return hit
	}
	pretty := &spec.Metadata{
		ForeignId:   hit.ForeignId,
		ForeignType: hit.ForeignType,
		EventType:   dictionary.ReverseResolve(p.EventType),
		Tags:        []*spec.KV{},
		Properties:  []*spec.KV{},
		CreatedAtNs: p.CreatedAtNs,
	}
	hit.KafkaOffset = &KafkaOffset{Offset: p.Offset, Partition: p.Partition}
	seen := map[uint64]map[string]bool{}
	for i := 0; i < len(p.TagKeys); i++ {
		k := p.TagKeys[i]
		v := p.TagValues[i]
		tk := dictionary.ReverseResolve(k)

		pretty.Tags = append(pretty.Tags, &spec.KV{Key: tk, Value: v})

		if px, ok := contextCache.Lookup(k, v, p.CreatedAtNs); ok {
			hit.Context = append(hit.Context, toContextDeep(seen, contextCache, dictionary, px)...)
		}
	}

	for i := 0; i < len(p.PropertyKeys); i++ {
		tk := dictionary.ReverseResolve(p.PropertyKeys[i])
		pretty.Properties = append(pretty.Properties, &spec.KV{Key: tk, Value: p.PropertyValues[i]})
	}

	sort.Sort(ByCtxName(hit.Context))
	sort.Sort(ByKV(pretty.Tags))
	sort.Sort(ByKV(pretty.Properties))

	hit.Metadata = pretty
	return hit
}

type Renderable interface {
	String(c *gin.Context)
	VW(c *gin.Context)
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
	} else if format == "vw" {
		x.VW(c)
	} else {
		x.String(c)
	}
}

type Counter struct {
	Tags         map[uint64]map[string]uint32 `json:"tags"`
	Properties   map[uint64]map[string]uint32 `json:"properties"`
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
		Tags:         map[uint64]map[string]uint32{},
		Properties:   map[uint64]map[string]uint32{},
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
	out := &CountedResult{}
	out.Tags = statsForMapMap(c.pd, c.Tags)
	out.Foreign = statsForMapMap(c.pd, c.Foreign)
	out.Properties = statsForMapMap(c.pd, c.Properties)

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
	for i := 0; i < len(p.TagKeys); i++ {
		k := p.TagKeys[i]
		v := p.TagValues[i]
		m, ok := c.Tags[k]
		if !ok {
			m = map[string]uint32{}
			c.Tags[k] = m
		}
		m[v]++
	}

	for i := 0; i < len(p.PropertyKeys); i++ {
		k := p.PropertyKeys[i]
		v := p.PropertyValues[i]
		m, ok := c.Properties[k]
		if !ok {
			m = map[string]uint32{}
			c.Properties[k] = m
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
	EventTypes *PerKey   `json:"event_types"`
	Foreign    []*PerKey `json:"foreign"`
	Tags       []*PerKey `json:"tags"`
	Properties []*PerKey `json:"properties"`
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

	c.HTML(http.StatusOK, "/html/t/index.tmpl", map[string]interface{}{"Crumbs": crumbs, "Stats": cr, "BaseUrl": url})
}

func (cr CountedResult) prettyCategoryStats() string {
	makers := prettyStats("FOREIGN", cr.Foreign)
	types := prettyStats("EVENT_TYPES", []*PerKey{cr.EventTypes})
	properties := prettyStats("PROPERTIES", cr.Properties)
	tags := prettyStats("TAGS", cr.Tags)
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
		out = append(out, fmt.Sprintf("« %s » total: %d, %.2f%%\n%s", t.Key, t.TotalCount, percent, chart.HorizontalBar(x, y, '▒', width, pad, 50)))
	}

	return fmt.Sprintf("%s%s", chart.Banner(title), strings.Join(out, "\n\n--------\n\n"))
}

func intOrDefault(s string, n int) int {
	v, err := strconv.ParseInt(s, 10, 32)
	if err != nil {
		return n
	}

	return int(v)
}

func main() {
	var proot = flag.String("root", "/blackrock/data-topic", "root directory for the files root/topic")
	var basicAuth = flag.String("basic-auth", "", "basic auth user and password, leave empty for no auth [just for testing, better hide it behind nginx]")
	var verbose = flag.Bool("verbose", false, "print info level logs to stdout")
	var bind = flag.String("bind", ":9002", "bind to")
	flag.Parse()
	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()
	root := *proot
	os.MkdirAll(root, 0700)
	forward, err := disk.NewForwardWriter(root, "main")
	if err != nil {
		log.Fatal(err)
	}

	forwardContext, err := disk.NewForwardWriter(root, "context")
	if err != nil {
		log.Fatal(err)
	}
	contextCache := NewContextCache(forwardContext)

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

	go func() {
		for {
			tmp, err := disk.NewPersistedDictionary(root)
			if err != nil {
				log.Fatal(err)
			}
			tmp.Close()
			dictionary = tmp

			contextCache.Scan()
			runtime.GC()

			time.Sleep(1 * time.Minute)
		}
	}()

	r := gin.Default()

	t, err := loadTemplate()
	if err != nil {
		log.Panic(err)
	}
	r.SetHTMLTemplate(t)
	r.StaticFS("/public/", Assets)
	r.GET("/health", func(c *gin.Context) {
		c.String(200, "OK")
	})

	r.GET("/favicon.ico", func(c *gin.Context) {
		f := Assets.Files["/html/img/favicon.ico"]
		h, err := ioutil.ReadAll(f)
		if err != nil {
			c.JSON(400, gin.H{"error": err.Error()})
			return
		}
		c.Data(200, "image/png", h)
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

	search := func(qr QueryRequest) (*QueryResponse, error) {
		query, err := fromJson(qr.Query, func(k, v string) Query {
			return NewTermQuery(inverted, dictionary, qr.ScanMaxDocuments, k, strings.ToLower(v))
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
				hit, err = getScoredHit(contextCache, forward, dictionary, did, qr.DecodeMetadata)
				hit.Score = score
				if err != nil {
					// possibly corrupt forward index, igore, error is already printed
					continue
				}
				out.Hits = append(out.Hits, hit)
				doInsert = true
			} else if out.Hits[len(out.Hits)-1].Score < hit.Score {
				doInsert = true
				hit, err = getScoredHit(contextCache, forward, dictionary, did, qr.DecodeMetadata)
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

	r.GET("/", func(c *gin.Context) {
		c.Redirect(302, "/scan/html/")
	})

	r.GET("/scan/:format/*query", func(c *gin.Context) {
		sampleSize := intOrDefault(c.Query("sample_size"), 200)
		counter := NewCounter(dictionary, contextCache, sampleSize)
		var p spec.PersistedMetadata
		queryPath := strings.Trim(c.Param("query"), "/")

		if queryPath != "" {
			query, err := fromString(strings.Replace(queryPath, "/", " AND ", -1), func(k, v string) Query {
				return NewTermQuery(inverted, dictionary, 100000, k, strings.ToLower(v))
			})
			if err != nil {
				c.JSON(400, gin.H{"error": err.Error()})
				return
			}
			for query.Next() != NO_MORE {
				did := query.GetDocId()
				foreignId, foreignType, data, offset, err := forward.Read(uint64(did), true)
				if err != nil {
					c.JSON(400, gin.H{"error": err.Error()})
					return
				}

				err = proto.Unmarshal(data, &p)
				if err != nil {
					c.JSON(400, gin.H{"error": err.Error()})
					return
				}
				counter.Add(int64(offset), foreignId, foreignType, &p)
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

			err = forward.Scan(back, true, func(offset uint64, foreignId, foreignType uint64, data []byte) error {
				err := proto.Unmarshal(data, &p)
				if err != nil {
					return err
				}
				counter.Add(int64(offset), foreignId, foreignType, &p)
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

func dumpObj(src interface{}) string {
	data, err := yaml.Marshal(src)
	if err != nil {
		log.Fatalf("marshaling to JSON failed: %s", err.Error())
	}
	var out bytes.Buffer
	err = json.Indent(&out, data, "", "  ")
	if err != nil {
		log.Fatalf("failed to dump object: %s", err.Error())
	}
	return string(out.Bytes())
}

func loadTemplate() (*template.Template, error) {
	t := template.New("").Funcs(template.FuncMap{
		"banner": func(b string) string {
			return chart.BannerLeft(b)
		},
		"time": func(b int64) string {
			t := time.Unix(b/1000000000, 0)
			return t.Format(time.UnixDate)
		},
		"pretty": func(b interface{}) string {
			return dumpObj(b)
		},
		"format": func(value int64) string {
			return fmt.Sprintf("%8s", chart.Fit(float64(value)))
		},
		"replace": func(a, b, c string) string {
			return strings.Replace(a, b, c, -1)
		},
		"minus": func(a, b int) int {
			return a - b
		},
		"percent": func(value ...interface{}) string {
			a := float64(value[0].(int64))
			b := float64(value[1].(int64))

			return fmt.Sprintf("%.2f", (100 * (b / a)))
		},
		"dict": func(values ...interface{}) (map[string]interface{}, error) {
			if len(values) == 0 {
				return nil, errors.New("invalid dict call")
			}

			dict := make(map[string]interface{})

			for i := 0; i < len(values); i++ {
				key, isset := values[i].(string)
				if !isset {
					if reflect.TypeOf(values[i]).Kind() == reflect.Map {
						m := values[i].(map[string]interface{})
						for i, v := range m {
							dict[i] = v
						}
					} else {
						return nil, errors.New("dict values must be maps")
					}
				} else {
					i++
					if i == len(values) {
						return nil, errors.New("specify the key for non array values")
					}
					dict[key] = values[i]
				}

			}
			return dict, nil
		},
		"safeHTML": func(b string) template.HTML {
			return template.HTML(b)
		}})
	for name, file := range Assets.Files {
		if file.IsDir() || !strings.HasSuffix(name, ".tmpl") {
			continue
		}
		h, err := ioutil.ReadAll(file)
		if err != nil {
			return nil, err
		}
		t, err = t.New(name).Parse(string(h))
		if err != nil {
			return nil, err
		}
	}
	return t, nil
}
