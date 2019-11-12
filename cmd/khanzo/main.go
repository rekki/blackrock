package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	ginprometheus "github.com/mcuadros/go-gin-prometheus"

	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
	"github.com/gin-gonic/gin/binding"
	"github.com/rekki/blackrock/cmd/jubei/disk"
	"github.com/rekki/blackrock/cmd/orgrim/spec"
	"github.com/rekki/blackrock/pkg/depths"
	log "github.com/sirupsen/logrus"
)

func CacheKey(t int64, doc int32) int64 {
	t = depths.SegmentFromNsInt(t)
	return int64(doc)<<32 | t
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
	}

	for _, v := range query {
		out[v] = true
	}
	return out
}

func getTimeBucketNs(b string) int64 {
	if b == "hour" {
		return int64(1 * time.Hour)
	}

	if b == "minute" {
		return int64(1 * time.Minute)
	}

	if b == "second" {
		return int64(1 * time.Second)
	}

	if b == "day" {
		return int64(1*time.Hour) * 24
	}

	if b == "week" {
		return int64(1*time.Hour) * 24 * 7
	}

	return int64(1*time.Hour) * 24
}
func sendResponse(c *gin.Context, out interface{}) {
	switch c.NegotiateFormat(gin.MIMEJSON, binding.MIMEPROTOBUF) {
	case binding.MIMEPROTOBUF:
		c.ProtoBuf(200, out)
	default:
		c.JSON(200, out)
	}
}

func extractQuery(c *gin.Context) (*spec.SearchQueryRequest, error) {
	qr := &spec.SearchQueryRequest{}
	if c.ContentType() == binding.MIMEPROTOBUF {
		if err := c.ShouldBindBodyWith(qr, binding.ProtoBuf); err != nil {
			return nil, err
		}

	} else {
		if err := c.ShouldBindBodyWith(qr, binding.JSON); err != nil {
			c.JSON(400, gin.H{"error": err.Error()})
			return nil, err

		}
	}
	return qr, nil
}

func extractAggregateQuery(c *gin.Context) (*spec.AggregateRequest, error) {
	qr := &spec.AggregateRequest{}
	if c.ContentType() == binding.MIMEPROTOBUF {
		if err := c.ShouldBindBodyWith(qr, binding.ProtoBuf); err != nil {
			return nil, err
		}

	} else {
		if err := c.ShouldBindBodyWith(qr, binding.JSON); err != nil {
			c.JSON(400, gin.H{"error": err.Error()})
			return nil, err

		}
	}
	return qr, nil
}

func main() {
	var proot = flag.String("root", "/blackrock/data-topic", "root directory for the files root/topic")
	var verbose = flag.Bool("verbose", false, "print info level logs to stdout")
	var bind = flag.String("bind", ":9002", "bind to")
	var prometheusListenAddress = flag.String("prometheus", "false", "true to enable prometheus (you can also specify a listener address")
	flag.Parse()

	if *verbose {
		log.SetLevel(log.InfoLevel)
	} else {
		gin.SetMode(gin.ReleaseMode)
		log.SetLevel(log.WarnLevel)
	}

	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()

	root := *proot
	err := os.MkdirAll(root, 0700)
	if err != nil {
		log.Fatal(err)
	}

	r := gin.Default()

	if listenerAddress := *prometheusListenAddress; len(listenerAddress) > 0 && listenerAddress != "false" {
		prometheus := ginprometheus.NewPrometheus("blackrock_khanzo")
		prometheus.ReqCntURLLabelMappingFn = func(c *gin.Context) string {
			url := c.Request.URL.Path
			url = strings.Replace(url, "//", "/", -1)
			return url
		}
		if listenerAddress != "true" {
			prometheus.SetListenAddress(listenerAddress)
		}
		prometheus.Use(r)
	}

	compact := disk.NewCompactIndexCache()
	r.Use(cors.Default())
	r.Use(gin.Recovery())

	r.GET("/health", func(c *gin.Context) {
		c.String(200, "OK")
	})

	foreach := func(qr *spec.SearchQueryRequest, cb func(int32, *spec.Metadata, float32) bool) error {
		l := log.WithField("query", qr)
		dates := expandYYYYMMDD(qr.From, qr.To)
		lock := sync.Mutex{}
		doneChan := make(chan error)
		for _, date := range dates {
			go func(date time.Time) {
				segment := path.Join(root, depths.SegmentFromNs(date.UnixNano()))
				forward, err := disk.NewForwardWriter(segment, "main")
				if err != nil {
					l.Warnf("failed to open forward index: %s, skipping", segment)
					doneChan <- nil
					return
				}

				query, err := fromQuery(qr.Query, func(k, v string) Query {
					return NewTermQuery(segment, k, v, compact)
				})
				if err != nil {
					doneChan <- err
					return
				}
				l.Warnf("running query {%v} in segment: %s", query.String(), segment)

				for query.Next() != NO_MORE {
					did := query.GetDocId()
					m, err := fetchFromForwardIndex(forward, did)
					if err != nil {
						l.Warnf("failed to decode offset %d, err: %s", did, err)
						continue
					}
					score := query.Score()

					lock.Lock()
					stop := cb(did, m, score)
					lock.Unlock()

					if stop {
						break
					}
				}
				doneChan <- nil
			}(date)
		}

		var err error
		for range dates {
			chanErr := <-doneChan
			if chanErr != nil && err == nil {
				err = chanErr
			}
		}

		return err
	}

	r.POST("/api/v0/search", func(c *gin.Context) {
		qr, err := extractQuery(c)
		if err != nil {
			c.JSON(400, gin.H{"error": err.Error()})
			return
		}

		out := &spec.SearchQueryResponse{
			Hits:  []*spec.Hit{},
			Total: 0,
		}
		err = foreach(qr, func(did int32, metadata *spec.Metadata, score float32) bool {
			out.Total++
			if qr.Limit == 0 {
				return true
			}

			doInsert := false
			hit := toHit(did, metadata)
			if len(out.Hits) < int(qr.Limit) {
				hit.Score = score
				out.Hits = append(out.Hits, hit)
				doInsert = true
			} else if out.Hits[len(out.Hits)-1].Score < score {
				doInsert = true
				hit.Score = score
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
			return false
		})
		if err != nil {
			c.JSON(400, gin.H{"error": err.Error()})
			return
		}
		sendResponse(c, out)
	})

	r.POST("/api/v0/aggregate", func(c *gin.Context) {
		qr, err := extractAggregateQuery(c)
		if err != nil {
			c.JSON(400, gin.H{"error": err.Error()})
			return
		}

		out := &spec.Aggregate{
			Search:    map[string]*spec.CountPerKV{},
			Count:     map[string]*spec.CountPerKV{},
			EventType: map[string]*spec.CountPerKV{},
			ForeignId: map[string]*spec.CountPerKV{},
			Possible:  map[string]uint32{},
			Total:     0,
		}

		eventTypeKey := "event_type"
		foreignIdKey := "foreign_id"
		etype := &spec.CountPerKV{Count: map[string]uint32{}}
		out.EventType[eventTypeKey] = etype
		wantEventType := qr.Fields[eventTypeKey]
		wantForeignId := qr.Fields[foreignIdKey]
		add := func(x []spec.KV, into map[string]*spec.CountPerKV) {
			for _, kv := range x {
				out.Possible[kv.Key]++
				if _, ok := qr.Fields[kv.Key]; !ok {
					continue
				}
				m, ok := into[kv.Key]
				if !ok {
					m = &spec.CountPerKV{Count: map[string]uint32{}}
					into[kv.Key] = m
				}
				m.Count[kv.Value]++
				m.Total++
			}
		}
		err = foreach(qr.Query, func(did int32, metadata *spec.Metadata, score float32) bool {
			out.Total++

			add(metadata.Search, out.Search)
			add(metadata.Count, out.Count)

			if wantEventType {
				etype.Count[metadata.EventType]++
				etype.Total++
			}

			if wantForeignId {
				m, ok := out.ForeignId[metadata.ForeignType]
				if !ok {
					m = &spec.CountPerKV{Count: map[string]uint32{}}
					out.ForeignId[metadata.ForeignType] = m
				}
				m.Count[metadata.ForeignId]++
				m.Total++
			}
			return false
		})

		// just for consistency
		out.Possible[foreignIdKey] = out.Total
		out.Possible[eventTypeKey] = out.Total

		if err != nil {
			c.JSON(500, gin.H{"error": err.Error()})
			return
		}
		sendResponse(c, out)
	})

	r.POST("/api/v0/fetch", func(c *gin.Context) {
		qr, err := extractQuery(c)
		if err != nil {
			c.JSON(400, gin.H{"error": err.Error()})
			return
		}
		w := c.Writer
		nl := []byte{'\n'}
		err = foreach(qr, func(did int32, metadata *spec.Metadata, score float32) bool {
			hit := toHit(did, metadata)
			left := qr.Limit
			b, err := json.Marshal(hit)
			if err != nil {
				return false
			}
			_, err = w.Write(b)
			if err != nil {
				return false
			}

			_, err = w.Write(nl)
			if err != nil {
				return false
			}

			if qr.Limit > 0 {
				left--
				if left == 0 {
					return false
				}
			}
			return false
		})
	})

	log.Panic(r.Run(*bind))
}
