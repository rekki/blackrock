package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"

	"github.com/gogo/protobuf/proto"
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

		tags := map[string]string{}
		for k, values := range r.URL.Query() {
			for _, v := range values {
				tags[k] = v
			}
		}
		metadata := &spec.Metadata{
			Tags:        tags,
			RemoteAddr:  r.RemoteAddr,
			CreatedAtNs: time.Now().UnixNano(),
		}

		envelope := &spec.Envelope{Metadata: metadata, Payload: data}
		encoded, err := proto.Marshal(envelope)
		if err != nil {
			log.Warnf("[orgrim] error encoding metadata %v, error: %s", metadata, err.Error())
			http.Error(w, errToStr(err), 500)
			return
		}

		p, o, err := publish(*dataTopic, encoded, producer)
		if err != nil {
			log.Warnf("[orgrim] error producing in %s, metadata: %v, error: %s", *dataTopic, metadata, err.Error())
			http.Error(w, errToStr(err), 500)
			return
		}

		log.Infof("[orgrim] [%d:%d] payload %d bytes, metadata: %v", p, o, len(data), metadata)

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
