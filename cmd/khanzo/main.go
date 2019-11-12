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
	"github.com/oschwald/geoip2-golang"

	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
	"github.com/gin-gonic/gin/binding"
	"github.com/rekki/blackrock/cmd/jubei/consume"
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
	var accept = flag.Bool("not-production-accept-events", false, "also accept events, super simple, so people can test in their laptops without zookeeper, kafka, orgrim, blackhand and jubei setup..")
	var geoipFile = flag.String("not-production-geoip", "", "path to https://dev.maxmind.com/geoip/geoip2/geolite2/ file")
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
		dates := expandYYYYMMDD(qr.From, qr.To)
		lock := sync.Mutex{}
		doneChan := make(chan error)
		for _, date := range dates {
			go func(date time.Time) {
				segment := path.Join(root, depths.SegmentFromNs(date.UnixNano()))
				forward, err := disk.NewForwardWriter(segment, "main")
				if err != nil {
					doneChan <- err
					return
				}

				query, err := fromQuery(qr.Query, func(k, v string) Query {
					return NewTermQuery(segment, k, v, compact)
				})
				if err != nil {
					doneChan <- err
					return
				}
				log.Warnf("running query {%v} in segment: %s", query.String(), segment)

				for query.Next() != NO_MORE {
					did := query.GetDocId()
					m, err := fetchFromForwardIndex(forward, did)
					if err != nil {
						log.Warnf("failed to decode offset %d, err: %s", did, err)
						continue
					}

					lock.Lock()
					stop := cb(did, m, query.Score())
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
			if chanErr != nil {
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

		if len(qr.Fields) == 0 {
			qr.Fields = map[string]bool{
				"event_type":      true,
				"foreign_id":      true,
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
		}

		out := &spec.Aggregate{
			Search:    map[string]*spec.CountPerKV{},
			Count:     map[string]*spec.CountPerKV{},
			EventType: map[string]*spec.CountPerKV{},
			ForeignId: map[string]*spec.CountPerKV{},
			Total:     0,
		}

		eventTypeKey := "event_type"
		etype := &spec.CountPerKV{Count: map[string]uint32{}}
		out.EventType[eventTypeKey] = etype
		wantEventType := qr.Fields[eventTypeKey]
		wantForeignId := qr.Fields["foreign_id"]
		add := func(x []spec.KV, into map[string]*spec.CountPerKV) {
			for _, kv := range x {
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
		if err != nil {
			c.JSON(400, gin.H{"error": err.Error()})
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
				c.JSON(400, gin.H{"error": err.Error()})
				return false
			}
			_, err = w.Write(b)
			if err != nil {
				c.JSON(400, gin.H{"error": err.Error()})
				return false
			}

			_, err = w.Write(nl)
			if err != nil {
				c.JSON(400, gin.H{"error": err.Error()})
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

	if *accept {
		setupSimpleEventAccept(root, *geoipFile, r)
	}

	log.Panic(r.Run(*bind))
}

func setupSimpleEventAccept(root string, geoipPath string, r *gin.Engine) {
	giant := sync.Mutex{}

	var geoip *geoip2.Reader
	var err error
	if geoipPath != "" {
		geoip, err = geoip2.Open(geoipPath)
		if err != nil {
			log.Fatal(err)
		}
	}

	inverted, err := disk.NewInvertedWriter(512)
	if err != nil {
		log.Fatal(err)
	}

	writers := map[string]*disk.ForwardWriter{}
	consumeLocally := func(envelope *spec.Envelope) error {
		if envelope.Metadata.CreatedAtNs == 0 {
			envelope.Metadata.CreatedAtNs = time.Now().UnixNano()
		}

		segmentId := path.Join(root, depths.SegmentFromNs(envelope.Metadata.CreatedAtNs))
		giant.Lock()
		defer giant.Unlock()

		w, ok := writers[segmentId]
		if !ok {
			_ = os.MkdirAll(segmentId, 0700)
			w, err = disk.NewForwardWriter(segmentId, "main")
			if err != nil {
				log.Fatal(err)
			}
			writers[segmentId] = w
		}

		err = consume.ConsumeEvents(segmentId, envelope, w, inverted)
		return err
	}

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
		err = consumeLocally(&envelope)
		if err != nil {
			c.JSON(500, gin.H{"error": err.Error()})
			return
		}

		c.JSON(200, gin.H{"success": true})
	})

	r.GET("/png/:event_type/:foreign_type/:foreign_id/*extra", func(c *gin.Context) {
		c.Header("Cache-Control", "no-cache, no-store, must-revalidate")
		c.Header("Expires", "0")
		c.Header("Pragma", "no-cache")

		envelope := &spec.Envelope{
			Metadata: &spec.Metadata{
				CreatedAtNs: time.Now().UnixNano(),
				EventType:   c.Param("event_type"),
				ForeignType: c.Param("foreign_type"),
				ForeignId:   c.Param("foreign_id"),
			},
		}
		extra := c.Param("extra")
		splitted := strings.Split(extra, "/")
		for _, s := range splitted {
			if s == "" {
				continue
			}
			kv := strings.Split(s, ":")
			if len(kv) != 2 || kv[0] == "" || kv[1] == "" {
				continue
			}
			envelope.Metadata.Search = append(envelope.Metadata.Search, spec.KV{Key: kv[0], Value: kv[1]})
		}
		err = spec.Decorate(geoip, c.Request, envelope)
		if err != nil {
			log.Warnf("[orgrim] failed to decorate, err: %s", err.Error())
		}

		err = spec.ValidateEnvelope(envelope)
		if err != nil {
			log.Warnf("[orgrim] invalid input, err: %s", err.Error())
		} else {
			err = consumeLocally(envelope)
			if err != nil {
				log.Warnf("[orgrim] error sending message, metadata %v, err: %s", envelope.Metadata, err.Error())
			}
		}

		c.Data(200, "image/png", []byte{137, 80, 78, 71, 13, 10, 26, 10, 0, 0, 0, 13, 73, 72, 68, 82, 0, 0, 0, 1, 0, 0, 0, 1, 8, 6, 0, 0, 0, 31, 21, 196, 137, 0, 0, 0, 9, 112, 72, 89, 115, 0, 0, 11, 19, 0, 0, 11, 19, 1, 0, 154, 156, 24, 0, 0, 0, 1, 115, 82, 71, 66, 0, 174, 206, 28, 233, 0, 0, 0, 4, 103, 65, 77, 65, 0, 0, 177, 143, 11, 252, 97, 5, 0, 0, 0, 16, 73, 68, 65, 84, 120, 1, 1, 5, 0, 250, 255, 0, 0, 0, 0, 0, 0, 5, 0, 1, 100, 120, 149, 56, 0, 0, 0, 0, 73, 69, 78, 68, 174, 66, 96, 130})
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

		err = spec.Decorate(geoip, c.Request, converted)
		if err != nil {
			log.Warnf("[orgrim] failed to decorate, err: %s", err.Error())
		}

		if err != nil {
			c.JSON(500, gin.H{"error": err.Error()})
			return
		}
		err = consumeLocally(converted)
		if err != nil {
			c.JSON(500, gin.H{"error": err.Error()})
			return
		}

		c.JSON(200, gin.H{"success": true})
	})
}
