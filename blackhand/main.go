package main

import (
	"flag"
	"strings"
	"time"

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

func connectForever(topic string, brokers string) *balancer.KafkaBalancer {
	for {
		dc, err := balancer.NewKafkaBalancer(topic, strings.Split(brokers, ","))
		if err != nil {
			log.Warn(err)
			time.Sleep(1 * time.Second)
			continue
		}
		return dc
	}
}

func main() {
	var dataTopic = flag.String("topic-data", "blackrock-data", "topic for the data")
	var kafkaServers = flag.String("kafka", "localhost:9092", "kafka addr")
	var verbose = flag.Bool("verbose", false, "print info level logs to stdout")
	var bind = flag.String("bind", ":9003", "bind to")
	flag.Parse()
	dc := connectForever(*dataTopic, *kafkaServers)

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
