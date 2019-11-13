package main

import (
	"encoding/json"
	"flag"
	"net/http"
	_ "net/http/pprof"
	"os"
	"path"
	"sort"
	"strconv"

	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
	"github.com/gin-gonic/gin/binding"
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

	foreach := func(qr *spec.SearchQueryRequest, cb func(int32, *spec.Metadata, float32) bool) error {
		l := log.WithField("query", qr)
		dates := depths.ExpandYYYYMMDD(qr.From, qr.To)
		for _, date := range dates {
			segment := path.Join(root, depths.SegmentFromNs(date.UnixNano()))
			forward, err := disk.NewForwardWriter(segment, "main")
			if err != nil {
				l.Warnf("failed to open forward index: %s, skipping", segment)
				continue
			}

			query, err := fromQuery(qr.Query, func(k, v string) Query {
				return NewTermQuery(segment, k, v)
			})
			if err != nil {
				return err
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

				stop := cb(did, m, score)

				if stop {
					break
				}
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

		l := log.WithField("query", qr.Query)
		dates := depths.ExpandYYYYMMDD(qr.Query.From, qr.Query.To)

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
		etype := &spec.CountPerKV{Count: map[string]uint32{}, Key: eventTypeKey}
		out.EventType[eventTypeKey] = etype
		wantEventType := qr.Fields[eventTypeKey]
		wantForeignId := qr.Fields[foreignIdKey]

		for _, date := range dates {
			segment := path.Join(root, depths.SegmentFromNs(date.UnixNano()))
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

			forward, err := disk.NewForwardWriter(segment, "main")
			if err != nil {
				l.Warnf("failed to open forward index: %s, skipping", segment)
				return
			}
			query, err := fromQuery(qr.Query.Query, func(k, v string) Query {
				return NewTermQuery(segment, k, v)
			})
			if err != nil {
				c.JSON(400, gin.H{"error": err.Error()})
				return
			}
			l.Warnf("running query {%v} in segment: %s", query.String(), segment)

			for query.Next() != NO_MORE {
				did := query.GetDocId()
				metadata, err := fetchFromForwardIndex(forward, did)
				if err != nil {
					l.Warnf("failed to decode offset %d, err: %s", did, err)
					continue
				}
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
			}
			out.Possible[foreignIdKey] = out.Total
			out.Possible[eventTypeKey] = out.Total
		}

		sort.Slice(out.Sample, func(i, j int) bool {
			return out.Sample[i].Metadata.CreatedAtNs < out.Sample[j].Metadata.CreatedAtNs
		})

		if len(out.Sample) > int(qr.SampleLimit) {
			out.Sample = out.Sample[:qr.SampleLimit]
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
