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
	"sort"
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

/*

{
   restaurant: {
       "92e2e4af-f833-492e-9ade-f797bbaa80fd": true,
       "ca91f7ab-13fa-46b7-9fbc-3f0276647238": true
   }
   message: "hello",
}
in this case you want to search for message:helo/restaurant:92e2e4af-f833-492e-9ade-f797bbaa80fd

{
   restaurant: {
       "92e2e4af-f833-492e-9ade-f797bbaa80fd": { updated: true  },
       "ca91f7ab-13fa-46b7-9fbc-3f0276647238": { updated: false }
   }
}


{
   example: {
      restaurant: {
          "92e2e4af-f833-492e-9ade-f797bbaa80fd": { updated: true  },
          "ca91f7ab-13fa-46b7-9fbc-3f0276647238": { updated: false }
      }
   }
}

possible search would be restaurant:92e2e4af-f833-492e-9ade-f797bbaa80fd/updated:true
but never restaurant:true or example:true

because of this we make extremely simple convention, every key that
has _id or _ids is expanded, e.g.:

{
   example: {
      restaurant_id: {
          "92e2e4af-f833-492e-9ade-f797bbaa80fd": { updated: true  },
          "ca91f7ab-13fa-46b7-9fbc-3f0276647238": { updated: false }
      }
   }
}

this event will be findable by 'restaurant_id:ca91f7ab-13fa-46b7-9fbc-3f0276647238'
but also 'example.retaurant_id.ca91f7ab-13fa-46b7-9fbc-3f0276647238.updated:true'


*/

type JsonFrame struct {
	Tags        map[string]interface{} `json:"tags"`
	Properties  map[string]interface{} `json:"properties"`
	CreatedAtNs int64                  `json:"created_at_ns"`
	Maker       string                 `json:"maker"`
	Type        string                 `json:"type"`
	Payload     interface{}            `json:"payload"`
}

func transform(m map[string]interface{}, expand bool) ([]*spec.KV, error) {
	out := []*spec.KV{}
	flatten, err := depths.Flatten(m, "", depths.DotStyle)
	if err != nil {
		return nil, err
	}
	seen := map[string]bool{}
	add := func(k, v string) {
		key := k + "_" + v
		if _, ok := seen[key]; ok {
			return
		}
		seen[key] = true
		out = append(out, &spec.KV{Key: k, Value: v})
	}
	hasID := func(s string) bool {
		return strings.HasSuffix(s, "_id") || strings.HasSuffix(s, "_ids")
	}
	for k, v := range flatten {
		if expand {
			// a lot of CoC here, path like example.restaurant_id.92e2e4af-f833-492e-9ade-f797bbaa80fd.updated = true
			// will be expanded to restaurant_id:92e2e4af-f833-492e-9ade-f797bbaa80fd and example.updated:true
			// so that it can be found, this of course is not ideal

			splitted := strings.Split(k, ".")
			noid := []string{}

			for i := len(splitted) - 1; i >= 0; i-- {
				part := splitted[i]
				prev := ""
				if i > 0 {
					prev = splitted[i-1]
				}
				if hasID(part) {
					add(part, v)
				} else {
					if hasID(prev) {
						add(prev, part)
						i--
					} else {
						noid = append(noid, part)
					}
				}
			}

			sort.SliceStable(noid, func(i, j int) bool {
				return true
			})
			if len(noid) > 0 {
				add(strings.Join(noid, "."), v)
			}
		} else {
			out = append(out, &spec.KV{Key: k, Value: v})
		}
	}

	return out, nil
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

		var metadata JsonFrame
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

		tags := []*spec.KV{}
		if metadata.Tags != nil {
			tags, err = transform(metadata.Tags, true)
			if err != nil {
				log.Warnf("[orgrim] unable to flatten tags error: %s", err.Error())
				c.JSON(500, gin.H{"error": "unable to flatten"})
				return
			}
		}

		properties := []*spec.KV{}
		if metadata.Properties != nil {
			properties, err = transform(metadata.Properties, true)
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

		if metadata.Payload != nil {
			payload, err := json.Marshal(&metadata.Payload)
			if err != nil {
				log.Warnf("[orgrim] unable to marshal payload, error: %s", err.Error())
				c.JSON(500, gin.H{"error": err.Error()})
				return
			}
			converted.Payload = payload
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
