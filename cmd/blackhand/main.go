package main

import (
	"errors"
	"flag"
	"fmt"
	"html/template"
	"io/ioutil"
	"net/http"
	_ "net/http/pprof"
	"net/url"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
	"github.com/rekki/blackrock/cmd/blackhand/chart"
	"github.com/rekki/blackrock/cmd/khanzo/client"
	"github.com/rekki/blackrock/cmd/orgrim/spec"
	"github.com/rekki/blackrock/pkg/depths"
	log "github.com/sirupsen/logrus"
)

func intOrDefault(s string, n int) int {
	v, err := strconv.ParseInt(s, 10, 32)
	if err != nil {
		return n
	}

	return int(v)
}

func yyyymmdd(t time.Time) string {
	year, month, day := t.Date()
	return fmt.Sprintf("%d-%02d-%02d", year, month, day)
}

func getWhitelist(query []string) map[string]bool {
	out := map[string]bool{
		"year-month-day":  true,
		"env":             true,
		"product":         true,
		"experiment":      true,
		"sizeWH":          true,
		"geoip_city":      true,
		"geoip_country":   true,
		"ua_is_mobile":    true,
		"ua_is_bot":       true,
		"ua_browser_name": true,
		"event_type":      true,
		"foreign_id":      true,
	}

	for _, v := range query {
		out[v] = true
	}
	return out
}

func main() {
	var purl = flag.String("url", "http://localhost:9002", "khanzo url")
	var verbose = flag.Bool("verbose", false, "print info level logs to stdout")
	var bind = flag.String("bind", ":9003", "bind to")
	var home = flag.String("home", "env:live", "home query")
	flag.Parse()
	if *verbose {
		log.SetLevel(log.InfoLevel)
	} else {
		gin.SetMode(gin.ReleaseMode)
		log.SetLevel(log.WarnLevel)
	}

	kh := client.NewClient(*purl, nil)
	r := gin.Default()
	r.Use(cors.Default())
	r.Use(gin.Recovery())

	t, err := loadTemplate()
	if err != nil {
		log.Panic(err)
	}
	r.SetHTMLTemplate(t)
	r.StaticFS("/public/", Assets)
	r.StaticFS("/external/", AssetsVendor)

	r.GET("/health", func(c *gin.Context) {
		c.String(200, "OK")
	})

	r.GET("/favicon.ico", func(c *gin.Context) {
		name := "/vendor/img/favicon.ico"
		f, ok := AssetsVendor.Files[name]
		if !ok {
			c.JSON(400, gin.H{"error": "not found"})
			return
		}
		h, err := ioutil.ReadAll(f)

		if err != nil {
			c.JSON(400, gin.H{"error": err.Error()})
			return
		}
		c.Data(200, "image/png", h)
	})

	r.GET("/", func(c *gin.Context) {
		c.Redirect(302, "/query/"+*home)
	})

	r.GET("/scan/html/*query", func(c *gin.Context) {
		c.Redirect(302, "/query/"+c.Param("query"))
	})

	r.GET("/query/*query", func(c *gin.Context) {
		from := c.Query("from")
		to := c.Query("to")

		if from == "" || to == "" || c.Query("bucket") == "" {
			c.Redirect(302, fmt.Sprintf("%s?from=%s&to=%s&bucket=hour", c.Request.URL.Path, yyyymmdd(time.Now().UTC().AddDate(0, 0, -1)), yyyymmdd(time.Now().UTC())))
			return
		}
		qr := &spec.SearchQueryRequest{From: from, To: to, Limit: 200}
		query := &spec.Query{}
		query.Type = spec.Query_AND
		not := &spec.Query{
			Type: spec.Query_OR,
		}

		for _, splitted := range strings.Split(c.Param("query"), "/") {
			if splitted == "" {
				continue
			}
			kv := strings.SplitN(splitted, ":", 2)

			if len(kv) != 2 {
				c.JSON(400, gin.H{"error": fmt.Sprintf("bad term: %v", kv)})
				return
			}
			if strings.HasPrefix(kv[0], "-") {
				not.Sub = append(not.Sub, &spec.Query{Type: spec.Query_TERM, Tag: strings.TrimPrefix(kv[0], "-"), Value: kv[1]})
			} else {
				query.Sub = append(query.Sub, &spec.Query{Type: spec.Query_TERM, Tag: kv[0], Value: kv[1]})
			}
		}

		if len(not.Sub) > 0 {
			query.Not = not
		}
		if len(query.Sub) == 0 {
			query.Sub = append(query.Sub, &spec.Query{Type: spec.Query_TERM, Tag: "env", Value: "live"})
		}
		qr.Query = query

		qa := &spec.AggregateRequest{Query: qr}
		qa.Fields = getWhitelist(c.QueryArray("whitelist"))
		qa.SampleLimit = 200
		agg, err := kh.Aggregate(qa)
		if err != nil {
			c.JSON(400, gin.H{"error": err.Error()})
			return
		}

		url := c.Request.URL.Path
		if url == "/query" {
			url = "/query/"
		}

		crumbs := NewBreadcrumb(url)
		page := &Page{
			Crumbs:      crumbs,
			Agg:         agg,
			Sample:      agg.Sample,
			QueryString: template.URL(c.Request.URL.RawQuery),
			BaseUrl:     url,
			Query:       qa,
		}
		c.HTML(http.StatusOK, "/html/t/index.tmpl", page)
	})

	log.Panic(r.Run(*bind))
}

type Page struct {
	Crumbs      Breadcrumbs
	Agg         *spec.Aggregate
	Sample      []*spec.Hit
	BaseUrl     string
	QueryString template.URL
	Query       *spec.AggregateRequest
}

type CountedString struct {
	Key      string
	Count    uint32
	Selected bool
}

func (p *Page) SortedSections() []*CountedString {
	out := []*CountedString{}
	for k, v := range p.Agg.Possible {
		out = append(out, &CountedString{Key: k, Count: v, Selected: p.Query.Fields[k]})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return out
}

func loadTemplate() (*template.Template, error) {
	t := template.New("").Funcs(template.FuncMap{
		"time": func(b int64) string {
			t := time.Unix(int64(b)/1000000000, 0)
			return t.Format(time.UnixDate)
		},
		"pretty": func(b interface{}) string {
			return depths.DumpObjNoIndent(b)
		},
		"json": func(b interface{}) template.JS {
			return template.JS(depths.DumpObj(b))
		},
		"replace": func(a, b, c string) string {
			return strings.Replace(a, b, c, -1)
		},

		"minus": func(a, b int) int {
			return a - b
		},
		"getN": func(qs template.URL, key string, n int) int {
			v, err := url.ParseQuery(string(qs))
			if err != nil {
				return n
			}

			off := intOrDefault(v.Get(key), n)
			return off
		},
		"getS": func(qs template.URL, key string) string {
			v, err := url.ParseQuery(string(qs))
			if err != nil {
				return ""
			}
			return v.Get(key)
		},
		"addN": func(qs template.URL, key string, n int) template.URL {
			v, err := url.ParseQuery(string(qs))
			if err != nil {
				return template.URL("")
			}

			off := intOrDefault(v.Get(key), 0)
			off += n
			v.Set(key, fmt.Sprintf("%d", off))
			return template.URL(v.Encode())
		},
		"remS": func(qs template.URL, key string) template.URL {
			v, err := url.ParseQuery(string(qs))
			if err != nil {
				return template.URL("")
			}
			v.Del(key)
			return template.URL(v.Encode())
		},
		"banner": func(b string) string {
			return chart.BannerLeft(b)
		},

		"format": func(value uint32) string {
			return fmt.Sprintf("%8s", chart.Fit(float64(value)))
		},

		"sortedKV": func(x map[string]*spec.CountPerKV) []*spec.CountPerKV {
			out := []*spec.CountPerKV{}
			for _, v := range x {
				out = append(out, v)
			}
			sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
			return out
		},

		"sortedCount": func(x map[string]uint32) []*CountedString {
			out := []*CountedString{}
			for k, v := range x {
				out = append(out, &CountedString{Key: k, Count: v})
			}
			sort.Slice(out, func(i, j int) bool { return out[j].Count < out[i].Count })
			return out
		},

		"prettyName": func(key, value string) string {
			return value
		},

		"percent": func(value ...interface{}) string {
			a := float64(value[0].(uint32))
			b := float64(value[1].(uint32))

			return fmt.Sprintf("%.2f", (100 * (b / a)))
		},
		"formatFloat": func(value float64) string {
			return fmt.Sprintf("%.2f", value)
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
	return "/query/" + strings.Join(out, "/")
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
	return "/query/" + strings.Join(out, "/")
}

func NewBreadcrumb(url string) Breadcrumbs {
	splitted := strings.Split(url, "/")
	crumbs := []Breadcrumb{}
	for i := 0; i < len(splitted[2:]); i++ {
		v := splitted[i+2]
		p := strings.Join(splitted[:i+2], "/")
		if len(v) > 0 {
			crumbs = append(crumbs, Breadcrumb{Base: p, Exact: v})
		}
	}
	log.Printf("%v", crumbs)
	return Breadcrumbs(crumbs)
}
