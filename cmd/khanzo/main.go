package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"io/ioutil"
	"net/http"
	_ "net/http/pprof"
	"os"
	"sort"
	"time"

	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
	"github.com/gin-gonic/gin/binding"
	"github.com/gogo/protobuf/jsonpb"
	"github.com/gogo/protobuf/proto"
	"github.com/rekki/blackrock/cmd/orgrim/spec"
	"github.com/rekki/blackrock/pkg/depths"
	log "github.com/sirupsen/logrus"
)

type scoredSegmentHit struct {
	did   int32
	score float32
	sid   int64
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

	body, err := ioutil.ReadAll(c.Request.Body)
	if err != nil {
		return nil, err
	}

	if c.ContentType() == binding.MIMEPROTOBUF {
		if err := proto.Unmarshal(body, qr); err != nil {
			return nil, err
		}
	} else {
		if err := jsonpb.Unmarshal(bytes.NewReader(body), qr); err != nil {
			c.JSON(400, gin.H{"error": err.Error()})
			return nil, err

		}
	}
	return qr, nil
}

func extractAggregateQuery(c *gin.Context) (*spec.AggregateRequest, error) {
	qr := &spec.AggregateRequest{}
	body, err := ioutil.ReadAll(c.Request.Body)
	if err != nil {
		return nil, err
	}

	if c.ContentType() == binding.MIMEPROTOBUF {
		if err := proto.Unmarshal(body, qr); err != nil {
			return nil, err
		}
	} else {
		if err := jsonpb.Unmarshal(bytes.NewReader(body), qr); err != nil {
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
	var nworkers = flag.Int("n-workers-per-query", 20, "how many workers to fetch from disk")
	var updateInterval = flag.Int("update-interval", 60, "searcher update interval")

	var storeInterval = flag.Int("store-interval", 3600, "store interval")
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

	memIndex := NewMemOnlyIndex(root)
	err = memIndex.Refresh()
	if err != nil {
		log.Fatal(err)
	}
	err = memIndex.DumpToDisk()
	if err != nil {
		log.Fatal(err)
	}
	memIndex.PrintStats()
	go func() {
		for {
			time.Sleep(time.Duration(*updateInterval) * time.Second)
			err = memIndex.Refresh()
			if err != nil {
				log.Fatal(err)
			}
		}
	}()

	go func() {
		for {
			time.Sleep(time.Duration(*storeInterval) * time.Second)
			err = memIndex.DumpToDisk()
			if err != nil {
				log.Fatal(err)
			}
			memIndex.PrintStats()
		}
	}()

	r := gin.Default()

	r.Use(cors.Default())
	r.Use(gin.Recovery())

	r.GET("/health", func(c *gin.Context) {
		c.String(200, "OK")
	})

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

		scored := []scoredSegmentHit{}
		err = memIndex.ForEach(qr, 0, func(sid int64, did int32, score float32) {
			out.Total++
			if qr.Limit == 0 {
				return
			}

			doInsert := false
			hit := scoredSegmentHit{did: did, score: score, sid: sid}
			if len(scored) < int(qr.Limit) {
				scored = append(scored, hit)
				doInsert = true
			} else if scored[len(scored)-1].score < score {
				doInsert = true
			}
			if doInsert {
				for i := 0; i < len(scored); i++ {
					if scored[i].score < hit.score {
						copy(scored[i+1:], scored[i:])
						scored[i] = hit
						break
					}
				}

			}
		})
		for _, s := range scored {
			m, err := memIndex.ReadAndDecodeForward(s.sid, s.did)
			if err != nil {
				c.JSON(500, gin.H{"error": err.Error()})
				return
			}
			out.Hits = append(out.Hits, toHit(s.did, m))
		}
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

		dates := depths.ExpandYYYYMMDD(qr.Query.From, qr.Query.To)
		out := &spec.Aggregate{
			Search:    map[string]*spec.CountPerKV{},
			Count:     map[string]*spec.CountPerKV{},
			EventType: map[string]*spec.CountPerKV{},
			ForeignId: map[string]*spec.CountPerKV{},
			Possible:  map[string]uint32{},
			Total:     0,
		}
		var chart *Chart
		if qr.TimeBucketSec != 0 {
			chart = NewChart(qr.TimeBucketSec, dates)
			out.Chart = chart.out
		}

		eventTypeKey := "event_type"
		foreignIdKey := "foreign_id"
		etype := &spec.CountPerKV{Count: map[string]uint32{}, Key: eventTypeKey}

		wantEventType := qr.Fields[eventTypeKey]
		wantForeignId := qr.Fields[foreignIdKey]

		if wantEventType {
			out.EventType[eventTypeKey] = etype
		}
		add := func(x []spec.KV, into map[string]*spec.CountPerKV) {
			for _, kv := range x {
				out.Possible[kv.Key]++
				if _, ok := qr.Fields[kv.Key]; !ok {
					continue
				}
				m, ok := into[kv.Key]
				if !ok {
					m = &spec.CountPerKV{Count: map[string]uint32{}, Key: kv.Key}
					into[kv.Key] = m
				}
				m.Count[kv.Value]++
				m.Total++
			}
		}

		err = memIndex.ForEachDecodeParallel(*nworkers, qr.Query, 0, func(did int32, metadata *spec.Metadata, score float32) {
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
					m = &spec.CountPerKV{Count: map[string]uint32{}, Key: metadata.ForeignType}
					out.ForeignId[metadata.ForeignType] = m
				}
				m.Count[metadata.ForeignId]++
				m.Total++
			}

			if len(out.Sample) < int(qr.SampleLimit) {
				hit := toHit(did, metadata)
				out.Sample = append(out.Sample, hit)
			}
			if chart != nil {
				chart.Add(metadata)
			}
		})
		if err != nil {
			c.JSON(400, gin.H{"error": err.Error()})
			return
		}

		out.Possible[foreignIdKey] = out.Total
		out.Possible[eventTypeKey] = out.Total

		sort.Slice(out.Sample, func(i, j int) bool {
			return out.Sample[i].Metadata.CreatedAtNs < out.Sample[j].Metadata.CreatedAtNs
		})

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
		sent := false
		err = memIndex.ForEachDecodeParallel(*nworkers, qr, qr.Limit, func(did int32, metadata *spec.Metadata, score float32) {
			hit := toHit(did, metadata)

			b, err := json.Marshal(hit)
			if err != nil {
				return
			}
			_, err = w.Write(b)
			if err != nil {
				return
			}

			_, err = w.Write(nl)
			if err != nil {
				// cant stop it
				return
			}
			sent = true
		})
		if !sent && err != nil {
			c.JSON(500, gin.H{"error": err.Error()})
			return
		}
	})

	log.Panic(r.Run(*bind))
}
