package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"os/signal"
	"syscall"

	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
	"github.com/gogo/protobuf/jsonpb"
	"github.com/gogo/protobuf/proto"
	"github.com/jackdoe/blackrock/depths"
	"github.com/jackdoe/blackrock/orgrim/spec"
	"github.com/segmentio/kafka-go"
	log "github.com/sirupsen/logrus"

	"strings"
	"time"
)

func dumpObj(src interface{}) string {
	data, err := json.Marshal(src)
	if err != nil {
		log.Fatalf("marshaling to JSON failed: %s", err.Error())
	}
	var out bytes.Buffer
	err = json.Indent(&out, data, "", "  ")
	if err != nil {
		log.Fatalf("failed to dump object: %s", err.Error())
	}
	return string(out.Bytes())
}

type JsonMetadata struct {
	Tags        map[string]interface{} `json:"tags"`
	Properties  map[string]interface{} `json:"properties"`
	CreatedAtNs int64                  `json:"created_at_ns"`
	Maker       string                 `json:"maker"`
	Type        string                 `json:"type"`
}

func main() {
	var dataTopic = flag.String("topic-data", "blackrock-data", "topic for the data")
	var kafkaServers = flag.String("kafka", "localhost:9092", "kafka addr")
	var verbose = flag.Bool("verbose", false, "print info level logs to stdout")
	var statSleep = flag.Int("writer-stats", 60, "print writer stats every # seconds")

	var bind = flag.String("bind", ":9001", "bind to")
	flag.Parse()

	if *verbose {
		log.SetLevel(log.InfoLevel)
	} else {
		gin.SetMode(gin.ReleaseMode)
		log.SetLevel(log.WarnLevel)
	}
	err := depths.HealthCheckKafka(*kafkaServers, *dataTopic)
	if err != nil {
		log.Fatal(err)
	}

	brokers := strings.Split(*kafkaServers, ",")
	kw := kafka.NewWriter(kafka.WriterConfig{
		Brokers:      brokers,
		Topic:        *dataTopic,
		Balancer:     &kafka.LeastBytes{},
		BatchTimeout: 1 * time.Second,
		Async:        true,
	})
	defer kw.Close()

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigs
		log.Warnf("closing the writer...")
		kw.Close()
		os.Exit(0)
	}()

	go func() {
		for {
			s := kw.Stats()
			fmt.Printf("%s\n", dumpObj(s))
			time.Sleep(time.Duration(*statSleep) * time.Second)
		}
	}()

	r := gin.Default()
	r.Use(gin.Recovery())
	r.Use(cors.Default())

	r.GET("/health", func(c *gin.Context) {
		err := depths.HealthCheckKafka(*kafkaServers, *dataTopic)
		if err != nil {
			c.String(400, "BAD")
			return
		}
		c.String(200, "OK")
	})

	r.POST("/push/raw/:type/:maker", func(c *gin.Context) {
		body := c.Request.Body
		defer body.Close()

		data, err := ioutil.ReadAll(body)
		if err != nil {
			log.Infof("[orgrim] error reading, error: %s", err.Error())
			c.JSON(400, gin.H{"error": err.Error()})
			return
		}

		tags := map[string]string{}
		maker := c.Param("maker")
		etype := c.Param("type")
		for k, values := range c.Request.URL.Query() {
			for _, v := range values {
				if v == "" {
					continue
				}
				if k == "maker" {
					maker = v
				} else if k == "type" {
					etype = v
				} else {
					tags[k] = v
				}
			}
		}
		if maker == "" {
			maker = "__nobody"
		}
		if etype == "" {
			etype = "event"
		}
		metadata := &spec.Metadata{
			Tags:        tags,
			CreatedAtNs: time.Now().UnixNano(),
			Type:        etype,
			Maker:       maker,
		}
		envelope := &spec.Envelope{Metadata: metadata, Payload: data}
		encoded, err := proto.Marshal(envelope)
		if err != nil {
			log.Warnf("[orgrim] error encoding metadata %v, error: %s", metadata, err.Error())
			c.JSON(500, gin.H{"error": err.Error()})
			return
		}

		err = kw.WriteMessages(context.Background(), kafka.Message{
			Value: encoded,
		})

		if err != nil {
			log.Warnf("[orgrim] error sending message, metadata %v, error: %s", metadata, err.Error())
			c.JSON(500, gin.H{"error": err.Error()})
			return
		}

		c.JSON(200, gin.H{"success": true})
	})

	r.POST("/push/envelope", func(c *gin.Context) {
		body := c.Request.Body
		defer body.Close()

		var envelope spec.Envelope
		var err error
		if c.Request.Header.Get("content-type") == "application/protobuf" {
			var data []byte
			data, err = ioutil.ReadAll(body)
			if err == nil {
				err = proto.Unmarshal(data, &envelope)
			}
		} else {
			err = jsonpb.Unmarshal(body, &envelope)
		}

		if err != nil {
			log.Warnf("[orgrim] error decoding envelope, error: %s", err.Error())
			c.JSON(500, gin.H{"error": err.Error()})
			return
		}
		if envelope.Metadata == nil {
			log.Warnf("[orgrim] no metadata in envelope, rejecting")
			c.JSON(500, gin.H{"error": "need metadata key"})
			return
		}

		if envelope.Metadata.Type == "" {
			log.Warnf("[orgrim] no type in metadata, rejecting")
			c.JSON(500, gin.H{"error": "need type key in metadata"})
			return
		}

		if envelope.Metadata.CreatedAtNs == 0 {
			envelope.Metadata.CreatedAtNs = time.Now().UnixNano()
		}

		encoded, err := proto.Marshal(&envelope)
		if err != nil {
			log.Warnf("[orgrim] error encoding metadata %v, error: %s", envelope.Metadata, err.Error())
			c.JSON(500, gin.H{"error": err.Error()})
			return
		}

		err = kw.WriteMessages(context.Background(), kafka.Message{
			Value: encoded,
		})

		if err != nil {
			log.Warnf("[orgrim] error sending message, metadata %v, error: %s", envelope.Metadata, err.Error())
			c.JSON(500, gin.H{"error": err.Error()})
			return
		}

		c.JSON(200, gin.H{"success": true})
	})

	r.POST("/push/flatten", func(c *gin.Context) {
		body := c.Request.Body
		defer body.Close()

		var metadata JsonMetadata
		data, err := ioutil.ReadAll(body)
		if err != nil {
			c.JSON(500, gin.H{"error": err.Error()})
			return
		}

		err = json.Unmarshal(data, &metadata)
		if err != nil {
			log.Warnf("[orgrim] error decoding metadata, error: %s", err.Error())
			c.JSON(500, gin.H{"error": err.Error()})
			return
		}

		if metadata.Type == "" {
			log.Warnf("[orgrim] no type in metadata, rejecting")
			c.JSON(500, gin.H{"error": "need type key in metadata"})
			return
		}

		if metadata.CreatedAtNs == 0 {
			metadata.CreatedAtNs = time.Now().UnixNano()
		}

		tags := map[string]string{}
		if metadata.Tags != nil {
			tags, err = depths.Flatten(metadata.Tags, "", depths.DotStyle)
			if err != nil {
				log.Warnf("[orgrim] unable to flatten tags error: %s", err.Error())
				c.JSON(500, gin.H{"error": "unable to flatten"})
				return
			}
		}

		properties := map[string]string{}
		if metadata.Properties != nil {
			properties, err = depths.Flatten(metadata.Properties, "", depths.DotStyle)
			if err != nil {
				log.Warnf("[orgrim] unable to flatten properties error: %s", err.Error())
				c.JSON(500, gin.H{"error": "unable to flatten"})
				return
			}
		}

		converted := spec.Envelope{
			Metadata: &spec.Metadata{
				Tags:        tags,
				Properties:  properties,
				CreatedAtNs: metadata.CreatedAtNs,
				Type:        metadata.Type,
				Maker:       metadata.Maker,
			},
		}

		encoded, err := proto.Marshal(&converted)
		if err != nil {
			log.Warnf("[orgrim] error encoding metadata %v, error: %s", converted.Metadata, err.Error())
			c.JSON(500, gin.H{"error": err.Error()})
			return
		}

		err = kw.WriteMessages(context.Background(), kafka.Message{
			Value: encoded,
		})

		if err != nil {
			log.Warnf("[orgrim] error sending message, metadata %v, error: %s", metadata, err.Error())
			c.JSON(500, gin.H{"error": err.Error()})
			return
		}

		c.JSON(200, gin.H{"success": true})
	})
	log.Panic(r.Run(*bind))
}
