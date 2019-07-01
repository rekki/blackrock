package main

import (
	"encoding/binary"
	"flag"
	"io/ioutil"
	"os"
	"path"
	"sort"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/jackdoe/blackrock/jubei/sanitize"
	log "github.com/sirupsen/logrus"
)

func NewTermQuery(root string, topic string, tagKey, tagValue string) *Term {
	dir, filename := sanitize.PathForTag(root, topic, tagKey, tagValue) // sanitizes inside
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

type QueryRequest struct {
	Query interface{} `json:"query"`
	Size  int         `json:"size"`
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

func main() {
	var root = flag.String("root", "/blackrock", "root directory for the files (root/topic/partition)")
	var dataTopic = flag.String("topic-data", "blackrock-data", "topic for the data")
	var verbose = flag.Bool("verbose", false, "print info level logs to stdout")
	var bind = flag.String("bind", ":9002", "bind to")
	flag.Parse()
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
	r.GET("/inspect/:tag", func(c *gin.Context) {
		files, err := ioutil.ReadDir(path.Join(*root, *dataTopic, sanitize.Cleanup(c.Param("tag"))))
		if err != nil {
			c.JSON(500, gin.H{"error": err.Error()})
			return
		}
		values := map[string]int64{}
		for _, fi := range files {
			if !fi.IsDir() && strings.HasSuffix(fi.Name(), ".p") {
				values[strings.TrimSuffix(fi.Name(), ".p")] = fi.Size() / 8
			}
		}

		c.JSON(200, gin.H{"values": values})
	})

	r.GET("/stat", func(c *gin.Context) {
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

		query, err := fromJSON(*root, *dataTopic, qr.Query)
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
