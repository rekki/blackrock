package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"io/ioutil"
	"os"
	"os/signal"
	"syscall"

	"github.com/gin-gonic/gin"
	"github.com/gogo/protobuf/proto"
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

	r := gin.Default()
	r.Use(gin.Recovery())
	go func() {
		for {
			s := kw.Stats()
			log.Warnf("%s", dumpObj(s))
			time.Sleep(time.Duration(*statSleep) * time.Second)
		}
	}()
	r.POST("/push/raw", func(c *gin.Context) {
		body := c.Request.Body
		defer body.Close()

		data, err := ioutil.ReadAll(body)
		if err != nil {
			log.Infof("[orgrim] error reading, error: %s", err.Error())
			c.JSON(400, gin.H{"error": err.Error()})
			return
		}

		if err != nil {
			log.Warnf("[orgrim] error producing in %s, data length: %s, error: %s", *dataTopic, len(data), err.Error())
			c.JSON(500, gin.H{"error": err.Error()})
			return
		}

		tags := map[string]string{}
		for k, values := range c.Request.URL.Query() {
			for _, v := range values {
				tags[k] = v
			}
		}

		metadata := &spec.Metadata{
			Tags:        tags,
			RemoteAddr:  c.Request.RemoteAddr,
			CreatedAtNs: time.Now().UnixNano(),
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
	log.Panic(r.Run(*bind))
}
