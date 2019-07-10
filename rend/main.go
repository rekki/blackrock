package main

import (
	"context"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"os/signal"
	"path"
	"syscall"
	"time"

	_ "net/http/pprof"
	"strings"

	"github.com/gogo/protobuf/proto"
	"github.com/jackdoe/blackrock/depths"
	"github.com/jackdoe/blackrock/jubei/disk"
	"github.com/jackdoe/blackrock/orgrim/spec"
	"github.com/segmentio/kafka-go"
	log "github.com/sirupsen/logrus"
)

func main() {
	var contextTopic = flag.String("topic-context", "blackrock-context", "topic for the context")
	var proot = flag.String("root", "/blackrock", "root directory for the files")
	var kafkaServers = flag.String("kafka", "localhost:9092,localhost:9092", "kafka addrs")
	var verbose = flag.Bool("verbose", false, "print info level logs to stdout")
	var maxDescriptors = flag.Int("max-descriptors", 1000, "max open descriptors")
	flag.Parse()

	root := path.Join(*proot, *contextTopic)
	os.MkdirAll(root, 0700)

	if *verbose {
		log.SetLevel(log.InfoLevel)
	} else {
		log.SetLevel(log.WarnLevel)
	}

	err := depths.HealthCheckKafka(*kafkaServers, *contextTopic)
	if err != nil {
		log.Fatal(err)
	}

	cidb, err := ioutil.ReadFile(path.Join(root, "rend_consumer_id"))
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
		err = ioutil.WriteFile(path.Join(root, "rend_consumer_id"), []byte(consumerId), 0600)
		if err != nil {
			log.Fatal(err)
		}
	} else {
		consumerId = string(cidb)
	}

	log.Warnf("connecting as consumer %s", consumerId)

	brokers := strings.Split(*kafkaServers, ",")
	rd := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        brokers,
		Topic:          *contextTopic,
		GroupID:        consumerId,
		CommitInterval: 1 * time.Second,
		MaxWait:        1 * time.Second,
	})

	defer rd.Close()
	files := map[string]*disk.ForwardWriter{}

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigs
		log.Warnf("closing the reader...")
		// no need to close the files, as they are closed on exit

		rd.Close()
		os.Exit(0)
	}()

	ctx := context.Background()
	log.Warnf("waiting...")
	for {
		m, err := rd.ReadMessage(ctx)
		if err != nil {
			log.Warnf("error reading %s", err.Error())
			break
		}

		envelope := spec.Context{}
		err = proto.Unmarshal(m.Value, &envelope)
		if err != nil {
			log.Warnf("failed to unmarshal, data: %s, error: %s", string(m.Value), err.Error())
			continue
		}

		if envelope.CreatedAtNs == 0 {
			envelope.CreatedAtNs = time.Now().UnixNano()
		}

		envelope.Type = depths.Cleanup(strings.ToLower(envelope.Type))
		if envelope.Type == "" {
			log.Warnf("empty type, data: %v", envelope)
			continue
		}

		envelope.Id = depths.Cleanup(strings.ToLower(envelope.Id))
		if envelope.Id == "" {
			log.Warnf("empty id, data: %v", envelope)
			continue
		}

		encoded, err := proto.Marshal(&envelope)
		if err != nil {
			log.Fatal("failed to marshal %v", envelope)
		}

		key := fmt.Sprintf("%s:%s", envelope.Type, envelope.Id)
		forward, ok := files[key]
		if !ok {
			if len(files) > *maxDescriptors {
				for _, v := range files {
					v.Close()
				}
				files = map[string]*disk.ForwardWriter{}
			}

			p := path.Join(root, envelope.Type, fmt.Sprintf("%d", depths.Hashs(envelope.Id)%255))
			os.MkdirAll(p, 0700)

			forward, err = disk.NewForwardWriter(p, envelope.Id)
			if err != nil {
				log.Fatal("failed to open %v, error: %s", envelope, err.Error())
			}
			files[key] = forward
		}

		_, err = forward.Append(0, encoded)
		if err != nil {
			log.Fatal(err)
		}

		log.Infof("message at topic/partition/offset %v/%v/%v: %v\n", m.Topic, m.Partition, m.Offset, envelope)
	}
}
