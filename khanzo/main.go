package main

import (
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"net/http"
	_ "net/http/pprof"
	"os"
	"path"
	"sort"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/gogo/protobuf/proto"
	"github.com/jackdoe/blackrock/jubei/sanitize"
	"github.com/jackdoe/blackrock/khanzo/chart"
	"github.com/jackdoe/blackrock/orgrim/spec"
	log "github.com/sirupsen/logrus"
)

// FIXME(jackdoe): this is very bad
func fromString(text string, makeTermQuery func(string, string) Query) (Query, error) {
	top := []Query{}
	for _, and := range strings.Split(text, " AND ") {
		sub := []Query{}
		for _, or := range strings.Split(and, " OR ") {
			t := strings.Split(or, ":")
			sub = append(sub, makeTermQuery(t[0], t[1]))
		}
		if len(sub) == 1 {
			top = append(top, sub[0])
		} else {
			top = append(top, NewBoolOrQuery(sub...))
		}
	}
	if (len(top)) == 1 {
		return top[0], nil
	}
	return NewBoolAndQuery(top...), nil
}

/*

{
   and: [{"or": [{"tag":{"key":"b","value": "v"}}]}]
}

*/

func fromJson(input interface{}, makeTermQuery func(string, string) Query) (Query, error) {
	mapped, ok := input.(map[string]interface{})
	queries := []Query{}
	if ok {
		if v, ok := mapped["tag"]; ok && v != nil {
			kv, ok := v.(map[string]interface{})
			if !ok {
				return nil, errors.New("[tag] must be map containing {key, value}")
			}
			added := false
			if tk, ok := kv["key"]; ok && tk != nil {
				if tv, ok := kv["value"]; ok && tv != nil {
					sk, ok := tk.(string)
					if !ok {
						return nil, errors.New("[tag][key] must be string")
					}
					sv, ok := tv.(string)
					if !ok {
						return nil, errors.New("[tag][value] must be string")
					}
					queries = append(queries, makeTermQuery(sk, sv))
					added = true
				}
			}
			if !added {
				return nil, errors.New("[tag] must be map containing {key, value}")
			}
		}
		if v, ok := mapped["and"]; ok && v != nil {
			list, ok := v.([]interface{})
			if ok {
				and := NewBoolAndQuery([]Query{}...)
				for _, subQuery := range list {
					q, err := fromJson(subQuery, makeTermQuery)
					if err != nil {
						return nil, err
					}
					and.AddSubQuery(q)
				}
				queries = append(queries, and)
			} else {
				return nil, errors.New("[or] takes array of subqueries")
			}
		}

		if v, ok := mapped["or"]; ok && v != nil {
			list, ok := v.([]interface{})
			if ok {
				or := NewBoolOrQuery([]Query{}...)
				for _, subQuery := range list {
					q, err := fromJson(subQuery, makeTermQuery)
					if err != nil {
						return nil, err
					}
					or.AddSubQuery(q)
				}
				queries = append(queries, or)
			} else {
				return nil, errors.New("[and] takes array of subqueries")
			}
		}
	}

	if len(queries) == 1 {
		return queries[0], nil
	}

	return NewBoolAndQuery(queries...), nil
}

func NewTermQuery(maxDocuments int64, root string, topic string, tagKey, tagValue string) Query {
	dir, filename := sanitize.PathForTag(root, topic, tagKey, tagValue) // sanitizes inside
	fn := path.Join(dir, filename)
	file, err := os.OpenFile(fn, os.O_RDONLY, 0600)
	if os.IsNotExist(err) {
		log.Infof("missing file %s, returning empty", fn)
		return NewTerm(fmt.Sprintf("%s:%s", tagKey, tagValue), []int64{})
	}
	fi, err := file.Stat()
	if err != nil {
		log.Warnf("failed to read file stats: %s, error: %s", fn, err.Error())
		return NewTerm(fmt.Sprintf("%s:%s", tagKey, tagValue), []int64{})
	}

	log.Infof("reading %s, size: %d", fn, fi.Size())
	total := fi.Size() / int64(8)
	seek := int64(0)
	if maxDocuments > 0 && total > maxDocuments {
		seek = (total - maxDocuments) * 8
	}
	file.Seek(seek, 0)
	log.Infof("seek %d, total %d max requested: %d", seek, total, maxDocuments)

	postings, err := ioutil.ReadAll(file)
	if err != nil {
		log.Warnf("failed to read file: %s, error: %s", fn, err.Error())
		return NewTerm(fmt.Sprintf("%s:%s", tagKey, tagValue), []int64{})
	}
	n := len(postings) / 8
	longed := make([]int64, n)
	j := 0
	for i := 0; i < n*8; i += 8 {
		longed[j] = int64(binary.LittleEndian.Uint64(postings[i:]))
		j++
	}

	return NewTerm(fmt.Sprintf("%s:%s", tagKey, tagValue), longed)
}

type ProjectionQuery struct {
	Query interface{} `json:"query"`
	Join  string      `json:"join"`
}

type ProjectionRequest struct {
	Sequence         []ProjectionQuery `json:"queries"`
	ScanMaxDocuments int64             `json:"scan_max_documents"`
}

type QueryRequest struct {
	Query            interface{} `json:"query"`
	Size             int         `json:"size"`
	Decode           bool        `json:"decode"`
	ScanMaxDocuments int64       `json:"scan_max_documents"`
}

type Hit struct {
	Metadata  *spec.Metadata `json:"metadata,omitempty"`
	Score     float32        `json:"score"`
	Offset    int64          `json:"offset"`
	Partition int32          `json:"partition"`
}

type QueryResponse struct {
	Total int64 `json:"size"`
	Hits  []Hit `json:"hits"`
}

func readForward(fd *os.File, did int64, decode bool) (int32, int64, *spec.Metadata, error) {
	data := make([]byte, 12)
	_, err := fd.ReadAt(data, did)
	if err != nil {
		return 0, 0, nil, err
	}

	kafka := binary.LittleEndian.Uint64(data[0:])
	offset := int64(kafka & 0xFFFFFFFFFFFF)
	partition := int32(kafka >> 54)
	if !decode {
		return partition, offset, nil, nil
	}

	metadataLen := binary.LittleEndian.Uint32(data[8:])
	metadataBlob := make([]byte, int(metadataLen))
	_, err = fd.ReadAt(metadataBlob, did+12)
	if err != nil {
		return 0, 0, nil, err
	}

	var meta spec.Metadata
	err = proto.Unmarshal(metadataBlob, &meta)
	if err != nil {
		return 0, 0, nil, err
	}

	return partition, offset, &meta, nil
}

func getScoredHit(forward *os.File, did int64, score float32, decode bool) (Hit, error) {
	partition, offset, meta, err := readForward(forward, did, decode)
	if err != nil {
		return Hit{}, err
	}
	hit := Hit{Offset: offset, Partition: partition, Score: score, Metadata: meta}
	return hit, nil
}

func tagStats(key string, dir string) (*TagKeyStats, error) {
	files, err := ioutil.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	out := &TagKeyStats{
		Values: []TagValueStats{},
		Key:    key,
	}
	for _, fi := range files {
		if !fi.IsDir() && strings.HasSuffix(fi.Name(), ".p") {
			n := fi.Size() / 8
			out.Values = append(out.Values, TagValueStats{
				V:     strings.TrimSuffix(fi.Name(), ".p"),
				Count: n,
			})
			out.TotalCount += n
		}
	}

	sort.Slice(out.Values, func(i, j int) bool {
		return out.Values[j].Count < out.Values[i].Count
	})

	return out, nil
}

type ProjectionResponse struct {
	CountedProperties map[string]map[string]uint64 `json:"counted_properties"`
	CountedTags       map[string]map[string]uint64 `json:"counted_tags"`
	First             *Hit                         `json:"first"`
	Last              *Hit                         `json:"last"`
	Total             uint64                       `json:"total"`
}

func (p *ProjectionResponse) Add(h Hit) {
	for k, v := range h.Metadata.Properties {
		pp, ok := p.CountedProperties[k]
		if !ok {
			pp = map[string]uint64{}
			p.CountedProperties[k] = pp
		}
		pp[v]++
	}

	for k, v := range h.Metadata.Tags {
		pp, ok := p.CountedTags[k]
		if !ok {
			pp = map[string]uint64{}
			p.CountedTags[k] = pp
		}
		pp[v]++
	}
}

type TagValueStats struct {
	V     string `json:"value"`
	Count int64  `json:"count"`
}

type TagKeyStats struct {
	Key        string          `json:"key"`
	Values     []TagValueStats `json:"values"`
	TotalCount int64           `json:"total"`
}

func main() {
	var root = flag.String("root", "/blackrock", "root directory for the files (root/topic/partition)")
	var dataTopic = flag.String("topic-data", "blackrock-data", "topic for the data")
	var verbose = flag.Bool("verbose", false, "print info level logs to stdout")
	var bind = flag.String("bind", ":9002", "bind to")
	flag.Parse()
	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()
	os.MkdirAll(path.Join(*root, *dataTopic), 0700)
	filename := path.Join(*root, *dataTopic, "forward.bin")
	forward, err := os.OpenFile(filename, os.O_RDONLY|os.O_CREATE, 0600)
	if err != nil {
		log.Fatal(err)
	}
	if *verbose {
		log.SetLevel(log.InfoLevel)
	} else {
		gin.SetMode(gin.ReleaseMode)
		log.SetLevel(log.WarnLevel)
	}

	r := gin.Default()

	r.GET("/debug", func(c *gin.Context) {
		fi, err := forward.Stat()
		if err != nil {
			c.JSON(500, gin.H{"error": err.Error()})
			return
		}
		total := fi.Size() / 8
		if total == 0 {
			total = 1
		}
		files, err := ioutil.ReadDir(path.Join(*root, *dataTopic))
		if err != nil {
			c.JSON(500, gin.H{"error": err.Error()})
			return
		}
		stats := []*TagKeyStats{}

		for _, fi := range files {
			if fi.IsDir() {
				key := sanitize.Cleanup(fi.Name())
				tv, err := tagStats(key, path.Join(*root, *dataTopic, key))
				if err != nil {
					c.JSON(500, gin.H{"error": err.Error()})
					return
				}

				stats = append(stats, tv)
			}
		}
		sort.Slice(stats, func(i, j int) bool {
			return stats[j].TotalCount < stats[i].TotalCount
		})

		out := []string{}
		pad := "    "
		width := 80 - len(pad)

		for _, t := range stats {
			x := []float64{}
			y := []string{}
			for _, v := range t.Values {
				x = append(x, float64(v.Count))
				y = append(y, v.V)
			}
			percent := float64(100) * float64(t.TotalCount) / float64(total)
			out = append(out, fmt.Sprintf("« %s » total: %d, %.2f%%\n%s", t.Key, t.TotalCount, percent, chart.HorizontalBar(x, y, '▒', width, pad, 0.5)))
		}

		out = append(out, fmt.Sprintf("\ntotal: %d\n", total))
		c.String(200, strings.Join(out, "\n\n--------\n\n"))
	})

	r.GET("/api/inspect/:tag", func(c *gin.Context) {
		key := sanitize.Cleanup(c.Param("tag"))
		v, err := tagStats(key, path.Join(*root, *dataTopic, key))
		if err != nil {
			c.JSON(500, gin.H{"error": err.Error()})
			return
		}

		c.JSON(200, v)
	})

	r.GET("/api/stat", func(c *gin.Context) {
		fi, err := forward.Stat()
		if err != nil {
			c.JSON(500, gin.H{"error": err.Error()})
			return
		}
		tags := []string{}
		files, err := ioutil.ReadDir(path.Join(*root, *dataTopic))
		if err != nil {
			c.JSON(500, gin.H{"error": err.Error()})
			return
		}
		for _, fi := range files {
			if fi.IsDir() {
				tags = append(tags, fi.Name())
			}
		}
		sort.Strings(tags)
		c.JSON(200, gin.H{"total_documents": fi.Size() / 8, "tags": tags})
	})

	r.POST("/search", func(c *gin.Context) {
		var qr QueryRequest
		if err := c.ShouldBindJSON(&qr); err != nil {
			c.JSON(400, gin.H{"error": err.Error()})
			return
		}

		query, err := fromJson(qr.Query, func(k, v string) Query {
			return NewTermQuery(qr.ScanMaxDocuments, *root, *dataTopic, k, v)
		})
		if err != nil {
			c.JSON(400, gin.H{"error": err.Error()})
			return
		}
		out := QueryResponse{
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
				hit, err = getScoredHit(forward, did, score, qr.Decode)
				if err != nil {
					c.JSON(500, gin.H{"error": err.Error()})
					return
				}
				out.Hits = append(out.Hits, hit)
				doInsert = true
			} else if out.Hits[len(out.Hits)-1].Score < hit.Score {
				doInsert = true
				hit, err = getScoredHit(forward, did, score, qr.Decode)
				if err != nil {
					c.JSON(500, gin.H{"error": err.Error()})
					return
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
		c.JSON(200, out)
	})

	r.POST("/project", func(c *gin.Context) {
		var pr ProjectionRequest
		if err := c.ShouldBindJSON(&pr); err != nil {
			c.JSON(400, gin.H{"error": err.Error()})
			return
		}

		runQuery := func(out *ProjectionResponse, query Query) error {
			log.Printf("%s", query.String())
			for query.Next() != NO_MORE {
				did := query.GetDocId()
				out.Total++
				hit, err := getScoredHit(forward, did, query.Score(), true)
				if err != nil {
					return err
				}
				out.Add(hit)
				if out.First == nil {
					out.First = &hit
				} else {
					out.Last = &hit
				}
			}
			return nil
		}

		out := &ProjectionResponse{
			CountedProperties: map[string]map[string]uint64{},
			CountedTags:       map[string]map[string]uint64{},
		}

		for i, sequence := range pr.Sequence {
			query, err := fromJson(sequence.Query, func(k, v string) Query {
				return NewTermQuery(pr.ScanMaxDocuments, *root, *dataTopic, k, v)
			})
			if err != nil {
				c.JSON(400, gin.H{"error": err.Error()})
				return
			}

			if i == 0 {
				if sequence.Join != "" {
					c.JSON(400, gin.H{"error": "first sequence can not have join"})
					return
				}
				err = runQuery(out, query)
				if err != nil {
					c.JSON(400, gin.H{"error": err.Error()})
				}
			} else {
				if sequence.Join == "" {
					c.JSON(400, gin.H{"error": "sequences must have join"})
					return
				}
				joined := &ProjectionResponse{
					CountedProperties: map[string]map[string]uint64{},
					CountedTags:       map[string]map[string]uint64{},
				}

				if agg, ok := out.CountedTags[sequence.Join]; ok {
					for term, _ := range agg {
						query.Reset()
						tq := NewTermQuery(pr.ScanMaxDocuments, *root, *dataTopic, sequence.Join, term)
						join := NewBoolAndQuery(tq, query)
						err = runQuery(joined, join)
						if err != nil {
							c.JSON(400, gin.H{"error": err.Error()})
							return
						}
					}
				} else {
					out = &ProjectionResponse{}
					break
				}
				out = joined
			}
		}
		c.JSON(200, out)
	})

	log.Panic(r.Run(*bind))
}
