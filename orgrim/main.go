package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"os/signal"
	"syscall"

	"github.com/gogo/protobuf/proto"
	"github.com/jackdoe/blackrock/orgrim/spec"
	"github.com/segmentio/kafka-go"
	log "github.com/sirupsen/logrus"

	"net/http"
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

func errToStr(e error) string {
	return dumpObj(map[string]string{"error": e.Error()})
}

func main() {
	var dataTopic = flag.String("topic-data", "blackrock-data", "topic for the data")
	var kafkaServers = flag.String("kafka", "localhost:9092", "kafka addr")
	var verbose = flag.Bool("verbose", false, "print info level logs to stdout")
	rand.Seed(time.Now().Unix())

	var bind = flag.String("bind", ":9001", "bind to")
	flag.Parse()
	if *verbose {
		log.SetLevel(log.InfoLevel)
	} else {
		log.SetLevel(log.WarnLevel)
	}

	brokers := strings.Split(*kafkaServers, ",")
	kw := kafka.NewWriter(kafka.WriterConfig{
		Brokers:      brokers,
		Topic:        *dataTopic,
		Balancer:     &kafka.LeastBytes{},
		BatchTimeout: 1 * time.Second,
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

	http.HandleFunc("/push/raw", func(w http.ResponseWriter, r *http.Request) {
		body := r.Body
		defer body.Close()

		data, err := ioutil.ReadAll(body)
		if err != nil {
			log.Infof("[orgrim] error reading, error: %s", err.Error())
			http.Error(w, errToStr(err), 400)
			return
		}

		if err != nil {
			log.Warnf("[orgrim] error producing in %s, data length: %s, error: %s", *dataTopic, len(data), err.Error())
			http.Error(w, errToStr(err), 500)
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
		err = kw.WriteMessages(context.Background(), kafka.Message{
			Value: encoded,
		})

		if err != nil {
			log.Warnf("[orgrim] error sending message, metadata %v, error: %s", metadata, err.Error())
			http.Error(w, errToStr(err), 500)
			return
		}

		log.Infof("[orgrim] payload %d bytes, metadata: %v", len(data), metadata)

		fmt.Fprintf(w, `{"success":true}`)
	})
	log.Infof("listening to %s", *bind)
	log.Fatal(http.ListenAndServe(*bind, nil))
}
