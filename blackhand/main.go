package main

import (
	"encoding/binary"
	"flag"
	"io/ioutil"
	"net/http"
	"os"
	"path"
	"sort"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/gogo/protobuf/proto"
	"github.com/jackdoe/blackrock/blackhand/balancer"
	"github.com/jackdoe/blackrock/jubei/sanitize"
	"github.com/jackdoe/blackrock/orgrim/spec"
	log "github.com/sirupsen/logrus"
)

func NewTermQuery(root string, topic string, partition int32, tagKey, tagValue string) *Term {
	dir, filename := sanitize.PathForTag(root, topic, partition, tagKey, tagValue)
	fn := path.Join(dir, filename)

	if _, err := os.Stat(fn); os.IsNotExist(err) {
		log.Infof("missing file %s, returning empty", fn)
		return NewTerm([]int64{})
	}
	log.Infof("reading %s", fn)
	postings, err := ioutil.ReadFile(fn)
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

type GetRequest struct {
	Partition int   `uri:"partition"`
	Offset    int64 `uri:"offset"`
}

type QueryRequest struct {
	Query     interface{} `json:"query"`
	Size      int64       `json:"size"`
	Partition int32       `json:"partition"`
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

func main() {
	var dataTopic = flag.String("topic-data", "blackrock-data", "topic for the data")
	var kafkaServers = flag.String("kafka", "localhost:9092", "kafka addr")
	var root = flag.String("root", "/tmp/jubei", "root directory for the files")
	var verbose = flag.Bool("verbose", false, "print info level logs to stdout")
	var bind = flag.String("bind", ":9002", "bind to")
	flag.Parse()

	dc, err := balancer.NewKafkaBalancer(*dataTopic, strings.Split(*kafkaServers, ","))
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
	r.StaticFS("/public", http.Dir("./public"))
	r.POST("/search", func(c *gin.Context) {
		var qr QueryRequest
		if err := c.ShouldBindJSON(&qr); err != nil {
			c.JSON(400, gin.H{"error": err.Error()})
			return
		}

		query, err := fromJSON(*root, *dataTopic, qr.Partition, qr.Query)
		if err != nil {
			c.JSON(400, gin.H{"error": err.Error()})
			return
		}
		out := QueryResponse{
			Hits:  []Hit{},
			Total: 0,
		}

		for query.Next() != NO_MORE {
			offset := query.GetDocId()
			hit := Hit{Offset: offset, Partition: qr.Partition, Score: query.Score()}
			out.Hits = append(out.Hits, hit)
			out.Total++
		}
		sort.Slice(out.Hits, func(i, j int) bool {
			return out.Hits[j].Score < out.Hits[i].Score
		})
		c.JSON(200, out)

	})
	r.GET("/get/:partition/:offset", func(c *gin.Context) {
		var query GetRequest
		if err := c.ShouldBindUri(&query); err != nil {
			c.JSON(400, gin.H{"error": err.Error()})
			return
		}

		blob, err := dc.ReadAt(query.Partition, query.Offset)
		if err != nil {
			c.JSON(500, gin.H{"error": err.Error()})
			return
		}
		envelope := spec.Envelope{}
		err = proto.Unmarshal(blob, &envelope)
		if err != nil {
			log.Warnf("failed to unmarshal, topic %s %v error: %s", *dataTopic, query, err.Error())
			c.JSON(500, gin.H{"error": err.Error()})
			return
		}

		c.Data(200, "octet-stream", envelope.Payload)
	})

	log.Panic(r.Run(*bind))
}
