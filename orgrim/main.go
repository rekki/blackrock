package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"math/rand"

	"github.com/gogo/protobuf/proto"
	"github.com/jackdoe/blackrock/orgrim/balancer"
	log "github.com/sirupsen/logrus"

	"net/http"
	"strings"
	"time"

	"./spec"
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
	var metaTopic = flag.String("topic-meta", "blackrock-metadata", "topic for the metadata")
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
	dc, err := balancer.NewKafkaBalancer(*dataTopic, brokers)
	if err != nil {
		log.Fatal(err)
	}
	mc, err := balancer.NewKafkaBalancer(*metaTopic, brokers)
	if err != nil {
		log.Fatal(err)
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

		p, o, err := dc.Write(data)

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
			Offset:      o,
			Partition:   p,
		}

		encoded, err := proto.Marshal(metadata)
		if err != nil {
			log.Warnf("[orgrim] error encoding metadata %v, error: %s", metadata, err.Error())
			http.Error(w, errToStr(err), 500)
			return
		}
		_, _, err = mc.Write(encoded)
		if err != nil {
			log.Warnf("[orgrim] error producing in %s, metadata: %v, error: %s", *metaTopic, metadata, err.Error())
			http.Error(w, errToStr(err), 500)
			// XXX: just panic, hope it will be restarted and pick another broker
			return
		}

		log.Infof("[orgrim] [%d:%d] payload %d bytes, metadata: %v", p, o, len(data), metadata)

		fmt.Fprintf(w, `{"success":true}`)
	})
	log.Infof("listening to %s", *bind)
	log.Fatal(http.ListenAndServe(*bind, nil))
}
