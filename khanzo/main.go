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

type QueryRequest struct {
	Query            interface{} `json:"query"`
	Size             int         `json:"size"`
	DecodeMetadata   bool        `json:"decode_metadata"`
	DecodeMaker      bool        `json:"decode_maker"`
	ScanMaxDocuments int64       `json:"scan_max_documents"`
}

type Hit struct {
	Metadata  *spec.Metadata `json:"metadata,omitempty"`
	Maker     string         `json:"maker"`
	Score     float32        `json:"score"`
	Offset    int64          `json:"offset"`
	Partition int32          `json:"partition"`
}

type QueryResponse struct {
	Total int64 `json:"total"`
	Hits  []Hit `json:"hits"`
}

func readForward(fd *os.File, did int64, decodeMaker bool, decodeMetadata bool) (int32, int64, string, *spec.Metadata, error) {
	data := make([]byte, 16)
	_, err := fd.ReadAt(data, did)
	if err != nil {
		log.Warnf("railed to read forward header, offset: %d, error: %s", did, err.Error())
		return 0, 0, "", nil, err
	}

	kafka := binary.LittleEndian.Uint64(data[0:])
	offset := int64(kafka & 0xFFFFFFFFFFFF)
	partition := int32(kafka >> 54)
	if !decodeMaker && !decodeMetadata {
		return partition, offset, "", nil, nil
	}

	metadataLen := binary.LittleEndian.Uint32(data[8:])
	makerLen := binary.LittleEndian.Uint32(data[12:])
	if makerLen > 128 || metadataLen > 1024*10 {
		log.Warnf("possibly corrupt header, offset: %d, makerLen: %d, metadataLen %d", did, makerLen, metadataLen)
		return 0, 0, "", nil, errors.New("corrupt header")
	}
	var readInto []byte
	if decodeMetadata {
		readInto = make([]byte, metadataLen+makerLen)
	} else {
		readInto = make([]byte, makerLen)
	}

	_, err = fd.ReadAt(readInto, did+16)
	if err != nil {
		log.Warnf("railed to read forward metadata, offset: %d, size: %d, error: %s", did+16, len(readInto), err.Error())
		return 0, 0, "", nil, err
	}
	var maker string
	var meta spec.Metadata
	if decodeMaker {
		maker = string(readInto[:makerLen])
	}

	if decodeMetadata {
		err = proto.Unmarshal(readInto[makerLen:], &meta)
		if err != nil {
			return 0, 0, "", nil, err
		}
	}

	return partition, offset, maker, &meta, nil
}

func getScoredHit(forward *os.File, did int64, score float32, decodeMaker bool, decodeMetadata bool) (Hit, error) {
	partition, offset, maker, meta, err := readForward(forward, did, decodeMaker, decodeMetadata)
	if err != nil {
		return Hit{}, err
	}
	hit := Hit{Offset: offset, Partition: partition, Score: score, Metadata: meta, Maker: maker}
	return hit, nil
}

func tagStats(out *TagKeyStats, key string, dir string) (*TagKeyStats, error) {
	files, err := ioutil.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	if out == nil {
		out = &TagKeyStats{
			Values: []TagValueStats{},
			Key:    key,
		}
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

type CategoryStats struct {
	Tags       []*TagKeyStats `json:"tags,omitempty"`
	Properties []*TagKeyStats `json:"properties,omitempty"`
	Types      *TagKeyStats   `json:"types,omitempty"`
	Makers     *TagKeyStats   `json:"makers,omitempty"`
}

func NewCategoryStats() CategoryStats {
	return CategoryStats{}
}

func StatsForMap(key string, values map[string]uint64) *TagKeyStats {
	tk := &TagKeyStats{
		Key: key,
	}
	for value, count := range values {
		tk.TotalCount += int64(count)
		tk.Values = append(tk.Values, TagValueStats{V: value, Count: int64(count)})
	}
	return tk
}
func StatsForMapMap(input map[string]map[string]uint64) []*TagKeyStats {
	stats := []*TagKeyStats{}
	for key, values := range input {
		stats = append(stats, StatsForMap(key, values))
	}
	return stats
}

type ProjectionResponse struct {
	CountedProperties map[string]map[string]uint64 `json:"counted_properties"`
	CountedTags       map[string]map[string]uint64 `json:"counted_tags"`
	CountedMaker      map[string]uint64            `json:"counted_makers"`
	CountedType       map[string]uint64            `json:"counted_types"`
	First             *Hit                         `json:"first"`
	Last              *Hit                         `json:"last"`
	Total             uint64                       `json:"total"`
}

func (p *ProjectionResponse) toCategoryStats() CategoryStats {
	cs := NewCategoryStats()
	cs.Properties = StatsForMapMap(p.CountedProperties)
	cs.Tags = StatsForMapMap(p.CountedTags)
	cs.Makers = StatsForMap("maker", p.CountedMaker)
	cs.Types = StatsForMap("type", p.CountedType)
	return cs
}
func NewProjectionResponse() *ProjectionResponse {
	return &ProjectionResponse{
		CountedProperties: map[string]map[string]uint64{},
		CountedTags:       map[string]map[string]uint64{},
		CountedMaker:      map[string]uint64{},
		CountedType:       map[string]uint64{},
	}
}
func (p *ProjectionResponse) Add(h Hit) {
	p.CountedMaker[h.Maker]++
	if h.Metadata == nil {
		return
	}

	p.CountedType[h.Metadata.Type]++
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

func banner(s string) string {
	width := 80
	out := "\n┌"
	for i := 0; i < width-2; i++ {
		out += "─"
	}
	out += "┐"
	out += "\n"
	out += "│"
	out += " "
	out += s

	for i := 0; i < width-3-len(s); i++ {
		out += " "
	}
	out += "│"
	out += "\n"
	out += "└"
	for i := 0; i < width-2; i++ {
		out += "─"
	}
	out += "┘"

	out += "\n"
	return out
}

func prettyCategoryStats(s CategoryStats) string {
	makers := prettyStats([]*TagKeyStats{s.Makers})
	types := prettyStats([]*TagKeyStats{s.Types})
	properties := prettyStats(s.Properties)
	tags := prettyStats(s.Tags)
	out := fmt.Sprintf("%s%s%s%s%s%s", banner("MAKERS"), makers, banner("TYPES"), types, banner("TAGS"), tags)
	if s.Properties != nil {
		out = fmt.Sprintf("%s%s%s", out, banner("PROPERTIES"), properties)
	}
	out += "\n"
	return out
}

func prettyStats(stats []*TagKeyStats) string {
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
		x := []float64{}
		y := []string{}
		sort.Slice(t.Values, func(i, j int) bool {
			return t.Values[j].Count < t.Values[i].Count
		})

		for _, v := range t.Values {
			x = append(x, float64(v.Count))
			y = append(y, v.V)
		}
		percent := float64(100) * float64(t.TotalCount) / float64(total)
		out = append(out, fmt.Sprintf("« %s » total: %d, %.2f%%\n%s", t.Key, t.TotalCount, percent, chart.HorizontalBar(x, y, '▒', width, pad, 50)))
	}

	return strings.Join(out, "\n\n--------\n\n")
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
	r.GET("/health", func(c *gin.Context) {
		c.String(200, "OK")
	})

	r.GET("/debug", func(c *gin.Context) {
		files, err := ioutil.ReadDir(path.Join(*root, *dataTopic))
		if err != nil {
			c.JSON(500, gin.H{"error": err.Error()})
			return
		}
		categoryStats := NewCategoryStats()

		for _, fi := range files {
			if fi.IsDir() {
				shards, err := ioutil.ReadDir(path.Join(*root, *dataTopic, fi.Name()))
				if err != nil {
					c.JSON(500, gin.H{"error": err.Error()})
					return
				}
				if len(shards) == 0 {
					continue
				}
				var tv *TagKeyStats
				for _, shard := range shards {
					tv, err = tagStats(tv, fi.Name(), path.Join(*root, *dataTopic, fi.Name(), shard.Name()))
					if err != nil {
						c.JSON(500, gin.H{"error": err.Error()})
						return
					}
				}
				if fi.Name() == "maker" {
					categoryStats.Makers = tv
				} else if fi.Name() == "type" {
					categoryStats.Types = tv
				} else {
					categoryStats.Tags = append(categoryStats.Tags, tv)
				}
			}
		}

		format := c.Query("format")
		if format == "json" {
			c.JSON(200, categoryStats)
		} else {
			pretty := prettyCategoryStats(categoryStats)
			c.String(200, pretty)
		}
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
				hit, err = getScoredHit(forward, did, score, qr.DecodeMetadata, qr.DecodeMetadata)
				if err != nil {
					// possibly corrupt forward index, igore, error is already printed
					continue
				}
				out.Hits = append(out.Hits, hit)
				doInsert = true
			} else if out.Hits[len(out.Hits)-1].Score < hit.Score {
				doInsert = true
				hit, err = getScoredHit(forward, did, score, qr.DecodeMaker, qr.DecodeMetadata)
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
		c.JSON(200, out)
	})

	r.GET("/project/*query", func(c *gin.Context) {
		queries := []Query{}

		queryPath := strings.Trim(c.Param("query"), "/")
		format := c.Query("format")

		makeTerm := func(k, v string) Query {
			return NewTermQuery(0, *root, *dataTopic, k, v)
		}

		for _, q := range strings.Split(queryPath, "^") {
			query, err := fromString(strings.Replace(q, "+", " AND ", -1), makeTerm)
			if err != nil {
				c.JSON(400, gin.H{"error": err.Error()})
				return
			}
			log.Warnf("adding %s", query.String())
			queries = append(queries, query)
		}

		runQuery := func(out *ProjectionResponse, query Query, decodeMaker bool, decodeMetadata bool) error {
			for query.Next() != NO_MORE {
				did := query.GetDocId()
				out.Total++
				hit, err := getScoredHit(forward, did, query.Score(), decodeMaker, decodeMaker)
				if err != nil {
					// possibly corrupt forward index, igore, error is already printed
					continue
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

		if len(queries) == 0 {
			c.JSON(400, gin.H{"error": "expected query, try /project/type:web/city:london^type:convert"})
			return
		}
		if len(queries) == 1 {
			out := NewProjectionResponse()

			err = runQuery(out, queries[0], true, true)
			if err != nil {
				c.JSON(400, gin.H{"error": err.Error()})
				return
			}
			if format == "json" {
				c.JSON(200, out.toCategoryStats())
			} else {
				c.String(200, prettyCategoryStats(out.toCategoryStats()))
			}
			return
		}

		out := NewProjectionResponse()
		err = runQuery(out, queries[0], true, false)
		if err != nil {
			c.JSON(400, gin.H{"error": err.Error()})
			return
		}
		log.Printf("%v", queries)
		for i, q := range queries[1:] {
			joined := NewProjectionResponse()
			decodeMetadata := i == len(queries)-2 // start from 1
			for maker, _ := range out.CountedMaker {
				q.Reset()
				tq := NewTermQuery(0, *root, *dataTopic, "maker", maker)
				join := NewBoolAndQuery(tq, q)
				err = runQuery(joined, join, true, decodeMetadata)
				if err != nil {
					c.JSON(400, gin.H{"error": err.Error()})
					return
				}
			}
			if len(joined.CountedMaker) == 0 {
				out = &ProjectionResponse{}
				break
			}

			out = joined
		}

		if format == "json" {
			c.JSON(200, out.toCategoryStats())
		} else {
			c.String(200, prettyCategoryStats(out.toCategoryStats()))
		}
	})

	log.Panic(r.Run(*bind))
}
