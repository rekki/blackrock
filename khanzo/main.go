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
	"github.com/jackdoe/blackrock/jubei/sanitize"
	"github.com/jackdoe/blackrock/khanzo/chart"
	log "github.com/sirupsen/logrus"
)

/*

{
   and: [{"or": [{"tag":{"key":"b","value": "v"}}]}]
}

*/

func fromJSON(input interface{}, makeTermQuery func(string, string) Query) (Query, error) {
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
					q, err := fromJSON(subQuery, makeTermQuery)
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
					q, err := fromJSON(subQuery, makeTermQuery)
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
		return NewTerm([]int64{})
	}
	fi, err := file.Stat()
	if err != nil {
		log.Warnf("failed to read file stats: %s, error: %s", fn, err.Error())
		return NewTerm([]int64{})
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
		return NewTerm([]int64{})
	}
	n := len(postings) / 8
	longed := make([]int64, n)
	j := 0
	for i := 0; i < n*8; i += 8 {
		longed[j] = int64(binary.LittleEndian.Uint64(postings[i:]))
		j++
	}

	return NewTerm(longed)
}

type QueryRequest struct {
	Query            interface{} `json:"query"`
	Size             int         `json:"size"`
	ScanMaxDocuments int64       `json:"scan_max_documents"`
}

type Hit struct {
	Score     float32 `json:"score"`
	Offset    int64   `json:"offset"`
	Partition int32   `json:"partition"`
}

type QueryResponse struct {
	Total int64 `json:"size"`
	Hits  []Hit `json:"hits"`
}

func readForward(fd *os.File, did int64) (int32, int64, error) {
	data := make([]byte, 8)
	_, err := fd.ReadAt(data, did*8)
	if err != nil {
		return 0, 0, err
	}
	v := binary.LittleEndian.Uint64(data)
	offset := int64(v & 0xFFFFFFFFFFFF)
	partition := int32(v >> 54)
	return partition, offset, nil
}
func getScoredHit(forward *os.File, did int64, score float32) (Hit, error) {
	partition, offset, err := readForward(forward, did)
	if err != nil {
		return Hit{}, err
	}
	hit := Hit{Offset: offset, Partition: partition, Score: score}
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
			out = append(out, fmt.Sprintf("« %s » total: %d, %.2f%%\n%s", t.Key, t.TotalCount, percent, chart.HorizontalBar(x, y, '▒', width, pad, 1)))
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

		query, err := fromJSON(qr.Query, func(k, v string) Query {
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
				hit, err = getScoredHit(forward, did, score)
				if err != nil {
					c.JSON(500, gin.H{"error": err.Error()})
					return
				}
				out.Hits = append(out.Hits, hit)
				doInsert = true
			} else if out.Hits[len(out.Hits)-1].Score < hit.Score {
				doInsert = true
				hit, err = getScoredHit(forward, did, score)
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

	log.Panic(r.Run(*bind))
}
