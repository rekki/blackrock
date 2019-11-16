package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"io/ioutil"
	"net/http"
	_ "net/http/pprof"
	"os"
	"path"
	"sort"
	"strconv"

	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
	"github.com/gin-gonic/gin/binding"
	"github.com/gogo/protobuf/jsonpb"
	"github.com/gogo/protobuf/proto"
	"github.com/rekki/blackrock/cmd/jubei/disk"
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

type matching struct {
	did   int32
	score float32
	m     *spec.Metadata
}

func main() {
	var proot = flag.String("root", "/blackrock/data-topic", "root directory for the files root/topic")
	var verbose = flag.Bool("verbose", false, "print info level logs to stdout")
	var bind = flag.String("bind", ":9002", "bind to")
	var nworkers = flag.Int("n-workers-per-query", 20, "how many workers to fetch from disk")
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

	r.Use(cors.Default())
	r.Use(gin.Recovery())

	r.GET("/health", func(c *gin.Context) {
		c.String(200, "OK")
	})

	foreach := func(qr *spec.SearchQueryRequest, limit int32, cb func(int32, *spec.Metadata, float32)) error {
		dates := depths.ExpandYYYYMMDD(qr.From, qr.To)
		l := log.WithField("limit", limit)
		for _, date := range dates {
			segment := path.Join(root, depths.SegmentFromNs(date.UnixNano()))
			forward, err := disk.NewForwardWriter(segment, "main")
			if err != nil {
				l.Warnf("failed to open forward index: %s, skipping", segment)
				continue
			}

			query, err := fromQuery(qr.Query, func(k, v string) Query {
				return NewTermQuery(segment, qr.MaxDocumentsPerField, k, v)
			})
			if err != nil {
				return err
			}
			parallel := *nworkers
			l.Warnf("running query {%v} in segment: %s, with %d workers", query.String(), segment, parallel)

			work := make(chan matching)
			doneWorker := make(chan bool)
			doneQuery := make(chan bool)
			doneConsumer := make(chan bool)
			completed := make(chan matching)

			for i := 0; i < parallel; i++ {
				go func() {
					for w := range work {
						m, err := fetchFromForwardIndex(forward, w.did)
						if err != nil {
							l.Warnf("failed to decode offset %d, err: %s", w.did, err)
							continue
						}
						w.m = m
						completed <- w
					}
					doneWorker <- true
				}()
			}

			stopped := false
			go func() {
				for query.Next() != NO_MORE {
					did := query.GetDocId()
					score := query.Score()
					work <- matching{did: did, score: score, m: nil}
					if limit > 0 {
						limit--
						if limit == 0 {
							stopped = true
							break
						}
					}
				}
				doneQuery <- true
			}()

			go func() {
				for matching := range completed {
					cb(matching.did, matching.m, matching.score)
				}
				doneConsumer <- true
			}()

			<-doneQuery
			close(work)

			for i := 0; i < parallel; i++ {
				<-doneWorker
			}

			close(completed)

			<-doneConsumer
			if stopped {
				break
			}
		}

		return nil
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

		err = foreach(qr, 0, func(did int32, metadata *spec.Metadata, score float32) {
			out.Total++
			if qr.Limit == 0 {
				return
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

		err = foreach(qr.Query, 0, func(did int32, metadata *spec.Metadata, score float32) {
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
		err = foreach(qr, qr.Limit, func(did int32, metadata *spec.Metadata, score float32) {
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
