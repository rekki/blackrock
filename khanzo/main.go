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

	crumbs := []string{`<a href="/scan/html/">/scan/html/</a>`}

	for i := 0; i < len(splitted[3:]); i++ {
		v := splitted[i+3]
		p := strings.Join(splitted[:i+3], "/")
		crumbs = append(crumbs, fmt.Sprintf(`<a href="%s/%s">%s</a> <a href="/scan/html/%s">=</a>`, p, v, v, v))
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
	var basicAuth = flag.String("basic-auth", "", "basic auth user and password, leave empty for no auth [just for testing, better hide it behind nginx]")
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
	r.GET("/favicon.ico", func(c *gin.Context) {
		v := []byte{
			137, 80, 78, 71, 13, 10, 26, 10, 0, 0, 0, 13, 73, 72, 68, 82, 0, 0, 0, 32, 0, 0, 0, 32, 8, 4, 0, 0, 0, 217, 115, 178, 127, 0, 0, 0, 4, 103, 65, 77, 65,
			0, 0, 177, 143, 11, 252, 97, 5, 0, 0, 0, 32, 99, 72, 82, 77, 0, 0, 122, 38, 0, 0, 128, 132, 0, 0, 250, 0, 0, 0, 128, 232, 0, 0, 117, 48, 0, 0, 234, 96,
			0, 0, 58, 152, 0, 0, 23, 112, 156, 186, 81, 60, 0, 0, 0, 2, 98, 75, 71, 68, 0, 0, 170, 141, 35, 50, 0, 0, 0, 9, 112, 72, 89, 115, 0, 0, 14, 196, 0, 0,
			14, 196, 1, 149, 43, 14, 27, 0, 0, 0, 7, 116, 73, 77, 69, 7, 227, 7, 8, 15, 0, 14, 199, 116, 93, 73, 0, 0, 2, 71, 73, 68, 65, 84, 72, 199, 149, 148, 79, 72,
			20, 81, 28, 199, 63, 59, 46, 187, 173, 89, 108, 109, 33, 164, 25, 101, 106, 118, 232, 208, 22, 100, 145, 199, 61, 36, 233, 165, 115, 199, 192, 139, 93, 42, 233, 18, 129, 24, 33, 118,
			144, 234, 168, 72, 176, 29, 58, 8, 122, 147, 2, 83, 15, 10, 65, 44, 66, 88, 81, 65, 203, 22, 225, 50, 107, 210, 154, 88, 56, 175, 131, 111, 126, 59, 179, 59, 187, 51, 125, 79,
			243, 190, 127, 126, 243, 222, 251, 189, 247, 234, 176, 177, 159, 48, 127, 9, 134, 6, 246, 176, 237, 38, 6, 49, 25, 10, 24, 135, 17, 54, 120, 72, 220, 30, 166, 40, 160, 80, 20, 57,
			18, 40, 126, 146, 109, 20, 138, 2, 169, 93, 226, 0, 38, 10, 133, 226, 69, 160, 2, 51, 218, 189, 206, 33, 155, 186, 165, 41, 69, 159, 111, 252, 154, 120, 239, 148, 200, 40, 239, 53,
			185, 230, 179, 140, 102, 153, 237, 59, 162, 78, 33, 201, 31, 45, 204, 19, 174, 26, 55, 120, 165, 93, 59, 92, 44, 23, 135, 101, 106, 213, 187, 113, 87, 60, 35, 149, 98, 132, 140, 84,
			79, 121, 198, 123, 216, 209, 142, 12, 49, 47, 67, 39, 27, 218, 96, 114, 170, 66, 237, 96, 93, 118, 191, 181, 218, 20, 123, 229, 31, 95, 104, 116, 41, 7, 249, 160, 21, 171, 118, 167,
			238, 201, 42, 223, 176, 87, 216, 24, 139, 194, 63, 168, 217, 37, 66, 76, 137, 117, 138, 58, 0, 194, 76, 11, 55, 173, 185, 26, 216, 39, 155, 169, 152, 196, 0, 110, 203, 120, 137, 122,
			191, 56, 192, 97, 86, 37, 50, 78, 136, 6, 230, 81, 40, 62, 150, 142, 174, 31, 142, 145, 149, 18, 163, 64, 61, 47, 201, 211, 22, 52, 14, 208, 198, 15, 41, 241, 20, 131, 24, 167,
			255, 39, 14, 112, 70, 95, 114, 133, 226, 153, 255, 214, 121, 225, 50, 69, 41, 145, 174, 113, 67, 60, 59, 241, 156, 36, 112, 73, 78, 159, 98, 198, 113, 46, 170, 192, 110, 209, 81, 50,
			40, 10, 156, 7, 206, 178, 38, 37, 222, 210, 92, 230, 116, 161, 155, 28, 125, 192, 57, 190, 235, 192, 79, 186, 128, 78, 114, 82, 226, 27, 73, 160, 151, 28, 221, 149, 5, 174, 96, 97,
			210, 207, 166, 216, 87, 245, 165, 57, 206, 103, 225, 138, 244, 99, 98, 209, 227, 53, 135, 71, 98, 83, 40, 22, 29, 199, 38, 193, 107, 151, 54, 234, 21, 63, 193, 130, 195, 50, 78, 196,
			165, 70, 153, 116, 168, 203, 116, 184, 195, 33, 110, 240, 75, 100, 139, 251, 158, 155, 124, 83, 46, 187, 226, 55, 131, 24, 182, 208, 197, 178, 107, 130, 249, 178, 183, 192, 70, 35, 121, 151,
			111, 137, 11, 0, 19, 88, 14, 114, 142, 52, 138, 217, 82, 117, 129, 193, 44, 138, 52, 115, 14, 183, 197, 4, 140, 201, 112, 147, 1, 66, 196, 88, 33, 75, 75, 69, 129, 22, 178, 172,
			16, 195, 96, 192, 209, 169, 49, 104, 98, 11, 133, 98, 129, 118, 109, 109, 37, 225, 185, 132, 132, 188, 133, 237, 250, 133, 218, 162, 9, 224, 9, 95, 185, 78, 200, 239, 160, 150, 225, 42,
			159, 120, 188, 251, 25, 47, 107, 89, 80, 68, 136, 195, 63, 63, 16, 56, 41, 61, 15, 134, 113, 0, 0, 0, 37, 116, 69, 88, 116, 100, 97, 116, 101, 58, 99, 114, 101, 97, 116, 101,
			0, 50, 48, 49, 57, 45, 48, 55, 45, 48, 56, 84, 49, 51, 58, 48, 48, 58, 49, 52, 43, 48, 50, 58, 48, 48, 95, 22, 104, 181, 0, 0, 0, 37, 116, 69, 88, 116, 100, 97,
			116, 101, 58, 109, 111, 100, 105, 102, 121, 0, 50, 48, 49, 57, 45, 48, 55, 45, 48, 56, 84, 49, 51, 58, 48, 48, 58, 49, 52, 43, 48, 50, 58, 48, 48, 46, 75, 208, 9, 0,
			0, 0, 25, 116, 69, 88, 116, 83, 111, 102, 116, 119, 97, 114, 101, 0, 119, 119, 119, 46, 105, 110, 107, 115, 99, 97, 112, 101, 46, 111, 114, 103, 155, 238, 60, 26, 0, 0, 0, 0,
			73, 69, 78, 68, 174, 66, 96, 130,
		}

		c.Data(200, "image/png", v)

	})
	r.GET("/", func(c *gin.Context) {
		c.Redirect(302, "/scan/html/")
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
