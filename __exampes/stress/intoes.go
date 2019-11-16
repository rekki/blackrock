package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	_ "net/http/pprof"
	"strings"
	"sync/atomic"
	"time"

	"github.com/gogo/protobuf/proto"
	spec "github.com/rekki/blackrock/cmd/orgrim/spec"
	"github.com/rekki/blackrock/pkg/depths"
	"github.com/segmentio/kafka-go"
	_ "github.com/segmentio/kafka-go/snappy"
	elastic "gopkg.in/olivere/elastic.v6"
)

const SEARCH_SETTINGS = `{
  "settings": {
    "number_of_shards": 4,
    "number_of_replicas": 0,
    "analysis": {
      "analyzer": {
        "default": {
          "type": "whitespace"
        }
      }
    }
  },
  "mappings": {
    "item": {
      "_source": {
        "enabled": false
      },
      "properties": {
        "foreign_id": {
          "type": "keyword",
          "store": true,
          "index": true
        },
        "foreign_type": {
          "type": "keyword",
          "store": true,
          "index": true
        },
        "search": {
          "type": "object",
          "enabled": true
        },
        "count": {
          "type": "object",
          "enabled": false
        },
        "properties": {
          "type": "object",
          "enabled": false
        },
        "created_at_ns": {
          "type": "long",
          "store": true,
          "index": false
        }
      }
    }
  }
}`

type Document struct {
	ForeignId   string            `json:"foreign_id"`
	ForeignType string            `json:"foreign_type"`
	Search      map[string]string `json:"search"`
	Count       map[string]string `json:"count"`
	Properties  map[string]string `json:"properties"`
	CreatedAtNs int64             `json:"created_at_ns"`
}

func createIndex(client *elastic.Client, name string, settings string) error {
	ctx := context.Background()
	exists, err := client.IndexExists(name).Do(ctx)
	if err != nil {
		return err
	}
	if exists {
		log.Printf("deleting %s", name)
		_, err := client.DeleteIndex(name).Do(ctx)
		if err != nil {
			return err
		}
	}
	_, err = client.CreateIndex(name).Body(settings).Do(ctx)
	if err != nil {
		return err
	}

	_, err = client.IndexPutSettings().Index(name).BodyString(`{
    "index": {
    "blocks": {
    "read_only": "false"
    }
    }
    }`).Do(context.TODO())
	if err != nil {
		return err
	}
	log.Printf("created index %s", name)
	return nil
}

func consumeEventsFromAllPartitions(client *elastic.Client, indexName string, pr []*PartitionReader) {
	ctx := context.Background()
	counter := uint64(0)
	bulkSize := 10000
	for _, p := range pr {
		err := p.Reader.SetOffset(kafka.FirstOffset)
		if err != nil {
			panic(err)
		}

		go func(p *PartitionReader) {
			bulk := client.Bulk().Index(indexName)

			for {
				m, err := p.Reader.FetchMessage(ctx)
				if err != nil {
					panic(err)
				}
				envelope := spec.Envelope{}
				err = proto.Unmarshal(m.Value, &envelope)
				if err != nil {
					log.Printf("failed to unmarshal, data: %s, error: %s", string(m.Value), err.Error())
					continue
				}

				if envelope.Metadata != nil {
					envelope.Metadata.Id = uint64(m.Partition)<<56 | uint64(m.Offset)
				}
				atomic.AddUint64(&counter, 1)
				did := fmt.Sprintf("%d", envelope.Metadata.Id)
				meta := envelope.Metadata
				doc := &Document{
					ForeignId:   meta.ForeignId,
					ForeignType: meta.ForeignType,
					Search:      map[string]string{},
					Count:       map[string]string{},
					Properties:  map[string]string{},
					CreatedAtNs: meta.CreatedAtNs,
				}

				for _, kv := range meta.Search {
					doc.Search[kv.Key] = kv.Value
				}

				for _, kv := range meta.Properties {
					doc.Properties[kv.Key] = kv.Value
				}

				for _, kv := range meta.Count {
					doc.Count[kv.Key] = kv.Value
				}

				bulk.Add(elastic.NewBulkIndexRequest().Id(did).Type("item").Doc(doc))
				if bulk.NumberOfActions() >= bulkSize {
					res, err := bulk.Do(ctx)
					if err != nil {
						panic(err)

					}
					if res.Errors {
						log.Panicf("%v", depths.DumpObj(res))
					}
				}

			}
		}(p)
	}

	for {
		time.Sleep(1 * time.Second)
		counter := atomic.SwapUint64(&counter, 0)
		log.Printf("%d events consumed last second", counter)
	}
}

func main() {
	var dataTopic = flag.String("topic-data", "blackrock-data", "topic for the data")
	var pminBytes = flag.Int("min-bytes", 10*1024*1024, "min bytes")
	var pmaxBytes = flag.Int("max-bytes", 20*1024*1024, "max bytes")
	var esurl = flag.String("es-url", "http://localhost:9200", "Elasticsearch URL")
	var indexName = flag.String("index-name", "blackrock", "index name")
	var kafkaServers = flag.String("kafka", "localhost:9092", "comma separated list of kafka servers")
	flag.Parse()

	go func() {
		log.Println(http.ListenAndServe("localhost:6061", nil))
	}()

	client, err := elastic.NewClient(elastic.SetURL(*esurl), elastic.SetSniff(false))
	if err != nil {
		log.Panic(err)
	}

	err = createIndex(client, *indexName, SEARCH_SETTINGS)
	if err != nil {
		log.Panic(err)
	}

	partitions, err := ReadPartitions(*kafkaServers, *dataTopic)
	if err != nil {
		log.Fatal(err)
	}

	brokers := strings.Split(*kafkaServers, ",")
	readers := []*PartitionReader{}
	for _, p := range partitions {
		rd := kafka.NewReader(kafka.ReaderConfig{
			Brokers:   brokers,
			MinBytes:  *pminBytes,
			MaxBytes:  *pmaxBytes,
			Topic:     *dataTopic,
			MaxWait:   1 * time.Second,
			Partition: p.ID,
		})
		readers = append(readers, &PartitionReader{rd, p})
	}

	go func() {
		consumeEventsFromAllPartitions(client, *indexName, readers)
	}()

	go func() {
		for {

			for _, rd := range readers {
				s := rd.Reader.Stats()
				if s.Lag > 0 {
					log.Printf("partition: %d, lag: %d, messages: %d\n", rd.Partition.ID, s.Lag, s.Messages)
				}
			}
			time.Sleep(1 * time.Second)
		}
	}()

	for {
		time.Sleep(1 * time.Second)
	}
}

func ReadPartitions(brokers string, topic string) ([]kafka.Partition, error) {
	for _, b := range depths.ShuffledStrings(strings.Split(brokers, ",")) {
		conn, err := kafka.Dial("tcp", b)
		if err == nil {
			p, err := conn.ReadPartitions(topic)
			conn.Close()
			if err == nil {
				return p, nil
			}
		}
	}
	return nil, errors.New("failed to get partitions, assuming we cant reach kafka")
}

type PartitionReader struct {
	Reader    *kafka.Reader
	Partition kafka.Partition
}
