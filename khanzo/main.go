package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"html/template"
	"io/ioutil"
	"net/http"
	_ "net/http/pprof"
	"net/url"
	"os"
	"path"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
	"github.com/gogo/protobuf/proto"
	lru "github.com/hashicorp/golang-lru"
	"github.com/jackdoe/blackrock/depths"
	"github.com/jackdoe/blackrock/jubei/consume"
	"github.com/jackdoe/blackrock/jubei/disk"
	"github.com/jackdoe/blackrock/khanzo/chart"
	"github.com/jackdoe/blackrock/orgrim/spec"
	auth "github.com/jackdoe/gin-basic-auth-dynamic"
	log "github.com/sirupsen/logrus"
	"github.com/spaolacci/murmur3"
)

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

func main() {
	var proot = flag.String("root", "/blackrock/data-topic", "root directory for the files root/topic")
	var basicAuth = flag.String("basic-auth", "", "basic auth user and password, leave empty for no auth [just for testing, better hide it behind nginx]")
	var lruSize = flag.Int("lru-size", 100000, "lru cache size for the forward index")
	var verbose = flag.Bool("verbose", false, "print info level logs to stdout")
	var accept = flag.Bool("not-production-accept-events", false, "also accept events, super simple, so people can test in their laptops without zookeeper, kafka, orgrim, blackhand and jubei setup..")
	var bind = flag.String("bind", ":9002", "bind to")
	var pconfig = flag.String("config", "", "config key:limit:sortByName:hide|key...")
	flag.Parse()
	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()
	root := *proot
	os.MkdirAll(root, 0700)
	config, err := LoadConfigFromString(*pconfig)
	if err != nil {
		log.Fatal(err)
	}
	log.Warnf("config: %s", depths.DumpObj(config))

	forwardContext, err := disk.NewForwardWriter(root, "context")
	if err != nil {
		log.Fatal(err)
	}
	contextCache := NewContextCache(forwardContext)

	if *verbose {
		log.SetLevel(log.InfoLevel)
	} else {
		gin.SetMode(gin.ReleaseMode)
		log.SetLevel(log.WarnLevel)
	}
	cache, err := lru.NewARC(*lruSize)
	if err != nil {
		log.Fatal(err)
	}

	go func() {
		for {
			contextCache.Scan()
			runtime.GC()
			log.Warnf("lru cache size: %d", cache.Len())
			time.Sleep(1 * time.Minute)
		}
	}()

	r := gin.Default()
	r.Use(cors.Default())
	r.Use(gin.Recovery())
	t, err := loadTemplate(contextCache)
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
		dates := expandYYYYMMDD(qr.From, qr.To)

		out := &QueryResponse{
			Hits:  []Hit{},
			Total: 0,
		}
		for _, date := range dates {
			segment := path.Join(root, depths.SegmentFromNs(uint64(date.UnixNano())))
			forward, err := disk.NewForwardWriter(segment, "main")
			if err != nil {
				return nil, err
			}

			query, _, err := fromJson(qr.Query, func(k, v string) Query {
				return NewTermQuery(segment, k, v)
			})
			if err != nil {
				return nil, err
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
					hit, err = getScoredHit(contextCache, forward, did)
					hit.Score = score
					if err != nil {
						// possibly corrupt forward index, igore, error is already printed
						continue
					}
					out.Hits = append(out.Hits, hit)
					doInsert = true
				} else if out.Hits[len(out.Hits)-1].Score < hit.Score {
					doInsert = true
					hit, err = getScoredHit(contextCache, forward, did)
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

	r.POST("/v0/fetch/", func(c *gin.Context) {
		var qr QueryRequest

		if err := c.ShouldBindJSON(&qr); err != nil {
			c.JSON(400, gin.H{"error": err.Error()})
			return
		}

		dates := expandYYYYMMDD(qr.From, qr.To)
		for _, date := range dates {
			segment := path.Join(root, depths.SegmentFromNs(uint64(date.UnixNano())))
			forward, err := disk.NewForwardWriter(segment, "main")
			if err != nil {
				c.JSON(400, gin.H{"error": err.Error()})
				return
			}

			query, _, err := fromJson(qr.Query, func(k, v string) Query {
				return NewTermQuery(segment, k, v)
			})

			if err != nil {
				c.JSON(400, gin.H{"error": err.Error()})
				return
			}

			w := c.Writer
			nl := []byte{'\n'}
			left := qr.Size
			for query.Next() != NO_MORE {
				did := query.GetDocId()
				hit, err := getScoredHit(contextCache, forward, did)
				hit.Score = query.Score()
				if err != nil {
					c.JSON(400, gin.H{"error": err.Error()})
					return
				}
				b, err := json.Marshal(&hit)
				if err != nil {
					c.JSON(400, gin.H{"error": err.Error()})
					return
				}
				w.Write(b)
				w.Write(nl)

				if qr.Size > 0 {
					left--
					if left == 0 {
						return
					}
				}
			}
		}
	})

	r.GET("/", func(c *gin.Context) {
		c.Redirect(302, "/scan/html/")
	})

	r.GET("/scan/:format/*query", func(c *gin.Context) {
		sampleSize := intOrDefault(c.Query("sample_size"), 100)
		counter := NewCounter()

		from := c.Query("from")
		to := c.Query("to")
		if (from == "" || to == "") && c.Param("format") == "html" {
			c.Redirect(302, fmt.Sprintf("%s?from=%s&to=%s", c.Request.URL.Path, yyyymmdd(time.Now().UTC().AddDate(0, 0, -1)), yyyymmdd(time.Now().UTC())))
			return
		}
		dates := expandYYYYMMDD(from, to)
		for _, date := range dates {
			segment := path.Join(root, depths.SegmentFromNs(uint64(date.UnixNano())))
			forward, err := disk.NewForwardWriter(segment, "main")
			if err != nil {
				c.JSON(400, gin.H{"error": err.Error()})
				return
			}

			// FIXME: this needs major cleanup
			queryPath := strings.Trim(c.Param("query"), "/")
			and := strings.Replace(queryPath, "/", " AND ", -1)
			andor := strings.Replace(and, "|", " OR ", -1)
			make := func(k, v string) Query {
				return NewTermQuery(segment, k, v)
			}
			query, nQueries, err := fromString(andor, make)
			if err != nil {
				c.JSON(400, gin.H{"error": err.Error()})
				return
			}

			if nQueries == 0 {
				query = expandTimeToQuery([]time.Time{date}, make)
			}
			for query.Next() != NO_MORE {
				did := query.GetDocId()
				cached, ok := cache.Get(did)

				if !ok {
					data, _, err := forward.Read(uint32(did))
					if err != nil {
						c.JSON(400, gin.H{"error": err.Error()})
						return
					}
					var p spec.Metadata
					err = proto.Unmarshal(data, &p)
					if err != nil {
						c.JSON(400, gin.H{"error": err.Error()})
						return
					}
					cache.Add(did, p)
					cached = p
				}

				cx := cached.(spec.Metadata)
				counter.Add(&cx)
				if len(counter.Sample) < sampleSize {
					counter.Sample = append(counter.Sample, toHit(contextCache, did, cx))
				}
			}
		}
		Render(c, counter.Prettify())
	})

	r.POST("/exp/csv", func(c *gin.Context) {
		var qr ExpQueryRequest

		if err := c.ShouldBindJSON(&qr); err != nil {
			c.JSON(400, gin.H{"error": err.Error()})
			return
		}
		dates := expandYYYYMMDD(qr.From, qr.To)
		for _, date := range dates {
			segment := path.Join(root, depths.SegmentFromNs(uint64(date.UnixNano())))
			forward, err := disk.NewForwardWriter(segment, "main")
			if err != nil {
				c.JSON(400, gin.H{"error": err.Error()})
				return
			}

			make := func(k, v string) Query {
				return NewTermQuery(segment, k, v)
			}
			query, nQueries, err := fromJson(qr.Query, make)
			if err != nil {
				c.JSON(400, gin.H{"error": err.Error()})
				return
			}
			if nQueries == 0 {
				query = expandTimeToQuery(dates, make)
			}

			onlyKey := qr.ExperimentKey
			variants := uint32(qr.Variants)
			if variants == 0 {
				variants = 2
			}

			if len(qr.Cohort) != 0 && qr.Variants > 0 {
				c.JSON(400, gin.H{"error": errors.New("cant use both cohort and variants, specify the variants in the cohort key")})
			}
			hasCohort := len(qr.Cohort) > 0
			for query.Next() != NO_MORE {
				did := query.GetDocId()
				cached, ok := cache.Get(did)
				if !ok {
					// XXX: dont cache or use another cache?
					data, _, err := forward.Read(uint32(did))
					if err != nil {
						c.JSON(400, gin.H{"error": err.Error()})
						return
					}
					var p spec.Metadata
					err = proto.Unmarshal(data, &p)
					if err != nil {
						c.JSON(400, gin.H{"error": err.Error()})
						return
					}
					cache.Add(did, p)
				}
				cx := cached.(spec.Metadata)
				variant := -1
				value := ""
				if cx.ForeignType == onlyKey {
					variant = int(dice(cx.ForeignId, qr.Exp, variants))
					value = cx.ForeignId
				} else {
					for _, kv := range cx.Search {
						if kv.Key == onlyKey {
							value = kv.Value
							if hasCohort {
								v, ok := qr.Cohort[value]
								if ok {
									variant = v
								}
							} else {
								variant = int(dice(value, qr.Exp, variants))
							}
							break
						}
					}
				}
				if variant >= 0 {
					epoch := cx.CreatedAtNs / 1000000000
					row := fmt.Sprintf("%d,%s,%s,%s,%d\n", epoch, cx.EventType, cx.ForeignId, value, variant)
					c.Writer.Write([]byte(row))
				}
			}
		}
	})

	if *accept {
		setupSimpleEventAccept(root, r)
	}

	log.Panic(r.Run(*bind))
}

func dice(id, exp string, variants uint32) uint32 {
	b := make([]byte, len(exp)+1+len(id))
	copy(b[0:], []byte(id))
	b[len(id)] = byte('/')
	copy(b[len(id)+1:], []byte(exp))
	// XXX: config the seed
	return murmur3.Sum32WithSeed(b, 0) % variants
}

func setupSimpleEventAccept(root string, r *gin.Engine) {
	giant := sync.Mutex{}

	forwardContext, err := disk.NewForwardWriter(root, "context")
	if err != nil {
		log.Fatal(err)
	}

	inverted, err := disk.NewInvertedWriter(512)
	if err != nil {
		log.Fatal(err)
	}

	writers := map[string]*disk.ForwardWriter{}

	r.POST("/push/envelope", func(c *gin.Context) {
		var envelope spec.Envelope
		err := depths.UnmarshalAndClose(c, &envelope)
		if err != nil {
			c.JSON(500, gin.H{"error": err.Error()})
			return
		}

		err = spec.ValidateEnvelope(&envelope)
		if err != nil {
			c.JSON(500, gin.H{"error": err.Error()})
			return
		}

		if envelope.Metadata.CreatedAtNs == 0 {
			envelope.Metadata.CreatedAtNs = uint64(time.Now().UnixNano())
		}

		segmentId := path.Join(root, depths.SegmentFromNs(envelope.Metadata.CreatedAtNs))
		giant.Lock()
		defer giant.Unlock()

		w, ok := writers[segmentId]
		if !ok {
			os.MkdirAll(segmentId, 0700)
			w, err = disk.NewForwardWriter(segmentId, "main")
			if err != nil {
				log.Fatal(err)
			}
			writers[segmentId] = w
		}

		err = consume.ConsumeEvents(segmentId, &envelope, w, inverted)
		if err != nil {
			c.JSON(500, gin.H{"error": err.Error()})
			return
		}

		c.JSON(200, gin.H{"success": true})
	})

	r.POST("/push/flatten", func(c *gin.Context) {
		body := c.Request.Body
		defer body.Close()

		converted, err := spec.DecodeAndFlatten(body)
		if err != nil {
			log.Warnf("[orgrim] invalid input, err: %s", err.Error())
			c.JSON(500, gin.H{"error": err.Error()})
			return
		}
		if err != nil {
			c.JSON(500, gin.H{"error": err.Error()})
			return
		}

		segmentId := path.Join(root, depths.SegmentFromNs(converted.Metadata.CreatedAtNs))
		giant.Lock()
		defer giant.Unlock()

		w, ok := writers[segmentId]
		if !ok {
			os.MkdirAll(segmentId, 0700)
			w, err := disk.NewForwardWriter(segmentId, "main")
			if err != nil {
				log.Fatal(err)
			}
			writers[segmentId] = w
		}

		err = consume.ConsumeEvents(segmentId, converted, w, inverted)

		if err != nil {
			c.JSON(500, gin.H{"error": err.Error()})
			return
		}

		c.JSON(200, gin.H{"success": true})
	})

	r.POST("/push/context", func(c *gin.Context) {
		var ctx spec.Context
		err := depths.UnmarshalAndClose(c, &ctx)
		if err != nil {
			c.JSON(500, gin.H{"error": err.Error()})
			return
		}
		err = spec.ValidateContext(&ctx)
		if err != nil {
			c.JSON(500, gin.H{"error": err.Error()})
			return
		}

		if ctx.CreatedAtNs == 0 {
			ctx.CreatedAtNs = uint64(time.Now().UnixNano())
		}

		giant.Lock()
		defer giant.Unlock()

		err = consume.ConsumeContext(&ctx, forwardContext)
		if err != nil {
			c.JSON(500, gin.H{"error": err.Error()})
			return
		}

		c.JSON(200, gin.H{"success": true})
	})
}

func loadTemplate(contextCache *ContextCache) (*template.Template, error) {
	t := template.New("").Funcs(template.FuncMap{
		"banner": func(b string) string {
			return chart.BannerLeft(b)
		},
		"time": func(b uint64) string {
			t := time.Unix(int64(b)/1000000000, 0)
			return t.Format(time.UnixDate)
		},
		"pretty": func(b interface{}) string {
			return depths.DumpObj(b)
		},
		"format": func(value int64) string {
			return fmt.Sprintf("%8s", chart.Fit(float64(value)))
		},
		"replace": func(a, b, c string) string {
			return strings.Replace(a, b, c, -1)
		},
		"ctx": func(key, value string) []*spec.Context {
			return LoadContextForStat(contextCache, key, value, uint64(time.Now().UnixNano()))
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
		"removeQuery": func(base []Breadcrumb, kv string) string {
			out := []string{}
			for _, crumb := range base {
				if crumb.Exact != kv {
					out = append(out, crumb.Exact)
				}
			}
			return "/scan/html/" + strings.Join(out, "/")
		},
		"negateQuery": func(base []Breadcrumb, kv string) string {
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
		"pick": func(from []*PerKey, which ...string) []*PerKey {
			out := []*PerKey{}
			for _, v := range from {
			WHICH:
				for _, k := range which {
					if v.Key == k {
						out = append(out, v)
						break WHICH
					}
				}
			}
			return out
		},
		"findFirstNameOrDefault": func(ctx []*spec.Context, def string) string {
			for _, c := range ctx {
				for _, p := range c.Properties {
					if p.Key == "name" {
						return p.Value
					}
				}
			}
			return def
		},
		"minus": func(a, b int) int {
			return a - b
		},
		"prettyFlat": func(v string) string {
			return strings.Replace(v, ".", " ", -1)
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
