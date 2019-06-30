package main

import (
	"flag"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/gogo/protobuf/proto"
	"github.com/jackdoe/blackrock/blackhand/balancer"
	"github.com/jackdoe/blackrock/orgrim/spec"
	log "github.com/sirupsen/logrus"
)

type GetRequest struct {
	Partition int   `uri:"partition"`
	Offset    int64 `uri:"offset"`
}

func main() {
	var dataTopic = flag.String("topic-data", "blackrock-data", "topic for the data")
	var kafkaServers = flag.String("kafka", "localhost:9092", "kafka addr")
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
