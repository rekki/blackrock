package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"

	log "github.com/sirupsen/logrus"

	"net/http"
	"os"
	"strings"
	"time"

	"./spec"
	"github.com/Shopify/sarama"
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

func errToStr(e error) string {
	return dumpObj(map[string]string{"error": e.Error()})
}

func main() {
	var dataTopic = flag.String("topic-data", "blackrock-data", "topic for the data")
	var metaTopic = flag.String("topic-meta", "blackrock-meta", "topic for the metadata")
	var kafkaServers = flag.String("kafka", "localhost:9092,localhost:9092", "kafka addrs")
	var verbose = flag.Bool("verbose", false, "print info level logs to stdout")

	var bind = flag.String("bind", ":9001", "bind to")
	flag.Parse()
	if *verbose {
		log.SetLevel(log.InfoLevel)
	} else {
		log.SetLevel(log.WarnLevel)
	}
	producer, err := initProducer(strings.Split(*kafkaServers, ","))
	if err != nil {
		log.Panic(err)
	}

	http.HandleFunc("/push/raw", func(w http.ResponseWriter, r *http.Request) {
		body := r.Body
		defer body.Close()

		data, err := ioutil.ReadAll(body)
		if err != nil {
			log.Infof("[orgrim] error reading, error: %s", err.Error())
			http.Error(w, errToStr(err), 400)
			return
		}

		tagSet := map[string]bool{}
		for k, values := range r.URL.Query() {
			for _, v := range values {
				tagSet[fmt.Sprintf("%s:%s", k, v)] = true
			}
		}
		p, o, err := publish(*dataTopic, data, producer)
		if err != nil {
			log.Warnf("[orgrim] error producing in %s, error: %s", *dataTopic, err.Error())
			http.Error(w, errToStr(err), 500)
			return
		}

		tags := []string{}
		for t, _ := range tagSet {
			tags = append(tags, t)
		}
		metadata := &spec.EventMetadata{
			Tags:        tags,
			Topic:       *dataTopic,
			RemoteAddr:  r.RemoteAddr,
			CreatedAtNs: time.Now().UnixNano(),
			Partition:   p,
			Offset:      o,
		}

		encoded, err := json.Marshal(metadata)
		if err != nil {
			log.Warnf("[orgrim] error encoding metadata %v, error: %s", metadata, err.Error())
			http.Error(w, errToStr(err), 500)
			return
		}
		_, _, err = publish(*metaTopic, encoded, producer)
		if err != nil {
			log.Warnf("[orgrim] error producing in %s, metadata: %v, error: %s", *metaTopic, metadata, err.Error())
			http.Error(w, errToStr(err), 500)
			return
		}

		log.Infof("[orgrim] [%s] [%d:%d] payload %d bytes, metadata: %v", *metaTopic, p, o, len(data), metadata)

		fmt.Fprintf(w, `{"success":true}`)
	})
	log.Infof("listening to %s", *bind)
	log.Fatal(http.ListenAndServe(*bind, nil))
}

func initProducer(addr []string) (sarama.SyncProducer, error) {
	sarama.Logger = log.StandardLogger()

	config := sarama.NewConfig()
	hostname, err := os.Hostname()
	if err != nil {
		return nil, err
	}
	config.ClientID = hostname
	config.Producer.Retry.Max = 5
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Return.Successes = true

	return sarama.NewSyncProducer(addr, config)
}

func publish(topic string, data []byte, producer sarama.SyncProducer) (partition int32, offset int64, err error) {
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.ByteEncoder(data),
	}
	partition, offset, err = producer.SendMessage(msg)
	return
}
