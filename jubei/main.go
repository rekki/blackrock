package main

import (
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"os/signal"
	"path"
	"syscall"
	"time"

	_ "net/http/pprof"

	"github.com/gogo/protobuf/proto"
	"github.com/jackdoe/blackrock/depths"
	"github.com/jackdoe/blackrock/jubei/consume"
	"github.com/jackdoe/blackrock/jubei/disk"
	"github.com/jackdoe/blackrock/orgrim/spec"
	log "github.com/sirupsen/logrus"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

func consumeEvents(dataTopic string, close chan bool, r *kafka.Consumer, dictionary *disk.PersistedDictionary, forward *disk.ForwardWriter, forwardContext *disk.ForwardWriter, payload *disk.ForwardWriter, inverted *disk.InvertedWriter) error {
	log.Warnf("waiting... ")
	for {
		select {
		case <-close:
			r.Close()
			return errors.New("closing")
		default:
			m, err := r.ReadMessage(1 * time.Second)
			if err != nil && err.(kafka.Error).Code() == kafka.ErrTimedOut {
				continue
			}

			if err != nil {
				r.Close()
				return err
			}
			if *m.TopicPartition.Topic == dataTopic {
				envelope := spec.Envelope{}
				err = proto.Unmarshal(m.Value, &envelope)
				if err != nil {
					log.Warnf("failed to unmarshal, data: %s, error: %s", string(m.Value), err.Error())
					continue
				}

				err = consume.ConsumeEvents(uint32(m.TopicPartition.Partition), uint64(m.TopicPartition.Offset), &envelope, dictionary, forward, payload, inverted)
				if err != nil {
					r.Close()
					return err
				}
			} else {
				envelope := spec.Context{}
				err = proto.Unmarshal(m.Value, &envelope)
				if err != nil {
					log.Warnf("context failed to unmarshal, data: %s, error: %s", string(m.Value), err.Error())
					continue
				}

				err = consume.ConsumeContext(&envelope, dictionary, forwardContext)
				if err != nil {
					r.Close()
					return err
				}
			}
			log.Infof("message at %v\n", m.TopicPartition)
		}
	}
}

func main() {
	var dataTopic = flag.String("topic-data", "blackrock-data", "topic for the data")
	var contextTopic = flag.String("topic-context", "blackrock-context", "topic for the context")
	var proot = flag.String("root", "/blackrock", "root directory for the files")
	var kafkaServers = flag.String("kafka", "localhost:9092,localhost:9092", "kafka addrs")
	var verbose = flag.Bool("verbose", false, "print info level logs to stdout")
	var maxDescriptors = flag.Int("max-descriptors", 1000, "max open descriptors")
	flag.Parse()
	go func() {
		log.Println(http.ListenAndServe("localhost:6061", nil))
	}()

	root := *proot

	os.MkdirAll(root, 0700)
	if *verbose {
		log.SetLevel(log.InfoLevel)
	} else {
		log.SetLevel(log.WarnLevel)
	}
	err := depths.HealthCheckKafka(*kafkaServers, *dataTopic)
	if err != nil {
		log.Fatal(err)
	}
	err = depths.HealthCheckKafka(*kafkaServers, *contextTopic)
	if err != nil {
		log.Fatal(err)
	}

	cidb, err := ioutil.ReadFile(path.Join(root, "consumer_id"))
	var consumerId string
	if err != nil {
		log.Warnf("error reading consumer id, generating new one, error: %s", err)
		hostname, err := os.Hostname()
		suffix := time.Now().UnixNano()
		if err == nil {
			consumerId = fmt.Sprintf("%s_%d", depths.Cleanup(hostname), suffix)
		} else {

			consumerId = fmt.Sprintf("__nohost__%d", suffix)
		}
		err = ioutil.WriteFile(path.Join(root, "consumer_id"), []byte(consumerId), 0600)
		if err != nil {
			log.Fatal(err)
		}
	} else {
		consumerId = string(cidb)
	}

	rd, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": *kafkaServers,
		"group.id":          consumerId,
		"auto.offset.reset": "earliest",
	})

	if err != nil {
		log.Fatal(err)
	}
	err = rd.SubscribeTopics([]string{*dataTopic, *contextTopic}, nil)
	if err != nil {
		log.Fatal(err)
	}

	log.Warnf("connecting as consumer %s", consumerId)

	forward, err := disk.NewForwardWriter(root, "main")
	if err != nil {
		log.Fatal(err)
	}

	forwardContext, err := disk.NewForwardWriter(root, "context")
	if err != nil {
		log.Fatal(err)
	}

	payload, err := disk.NewForwardWriter(root, "payload")
	if err != nil {
		log.Fatal(err)
	}

	inverted, err := disk.NewInvertedWriter(root, *maxDescriptors)
	if err != nil {
		log.Fatal(err)
	}

	dictionary, err := disk.NewPersistedDictionary(root)
	if err != nil {
		log.Fatal(err)
	}

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	closeRD := make(chan bool, 1)
	cleanup := func() {
		// no need to close the files, as they are closed on exit
		log.Warnf("closing the readers...")
		closeRD <- true
	}

	go func() {
		<-sigs
		cleanup()
	}()

	err = consumeEvents(*dataTopic, closeRD, rd, dictionary, forward, forwardContext, payload, inverted)
	log.Warnf("error consuming: %s", err.Error())
	log.Warnf("closing the files...")
	inverted.Close()
	dictionary.Close()
	forward.Close()

}
