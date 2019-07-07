package main

import (
	"context"
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
	"strings"

	"github.com/gogo/protobuf/proto"
	"github.com/jackdoe/blackrock/depths"
	"github.com/jackdoe/blackrock/jubei/disk"
	"github.com/jackdoe/blackrock/orgrim/spec"
	"github.com/segmentio/kafka-go"
	log "github.com/sirupsen/logrus"
)

func main() {
	var dataTopic = flag.String("topic-data", "blackrock-data", "topic for the data")
	var proot = flag.String("root", "/blackrock", "root directory for the files")
	var kafkaServers = flag.String("kafka", "localhost:9092,localhost:9092", "kafka addrs")
	var verbose = flag.Bool("verbose", false, "print info level logs to stdout")
	var maxDescriptors = flag.Int("max-descriptors", 1000, "max open descriptors")
	flag.Parse()
	go func() {
		log.Println(http.ListenAndServe("localhost:6061", nil))
	}()

	root := path.Join(*proot, *dataTopic)

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
	cidb, err := ioutil.ReadFile(path.Join(root, "consumer_id"))
	var consumerId string
	if err != nil {
		log.Printf("error reading consumer id, generating new one, error: %s", err)
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
	log.Printf("connecting as consumer %s", consumerId)
	brokers := strings.Split(*kafkaServers, ",")
	rd := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        brokers,
		Topic:          *dataTopic,
		GroupID:        consumerId,
		CommitInterval: 1 * time.Second,
		MaxWait:        1 * time.Second,
	})
	defer rd.Close()
	forward, err := disk.NewForwardWriter(root, "main")
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

	go func() {
		<-sigs
		log.Warnf("closing the reader...")
		// no need to close the files, as they are closed on exit

		rd.Close()
		inverted.Close()
		dictionary.Close()
		forward.Close()
		os.Exit(0)
	}()

	ctx := context.Background()

	makerKey, err := dictionary.GetUniqueTerm("maker")
	typeKey, err := dictionary.GetUniqueTerm("type")
	log.Warnf("waiting... [makerKey: %d, typeKey: %d]", makerKey, typeKey)
	for {
		m, err := rd.ReadMessage(ctx)
		if err != nil {
			log.Warnf("error reading %s", err.Error())
			break
		}

		envelope := spec.Envelope{}
		err = proto.Unmarshal(m.Value, &envelope)
		if err != nil {
			log.Warnf("failed to unmarshal, data: %s, error: %s", string(m.Value), err.Error())
			continue
		}

		if envelope.Metadata.CreatedAtNs == 0 {
			envelope.Metadata.CreatedAtNs = time.Now().UnixNano()
		}

		meta := envelope.Metadata
		maker, err := dictionary.GetUniqueTerm(depths.Cleanup(meta.Maker))
		if err != nil {
			log.Fatal("failed to get dictionary value for %v", meta)
		}
		etype, err := dictionary.GetUniqueTerm(depths.Cleanup(meta.Type))
		if err != nil {
			log.Fatal("failed to get dictionary value for %v", meta)
		}
		persisted := &spec.PersistedMetadata{
			Partition:   uint32(m.Partition),
			Offset:      uint64(m.Offset),
			CreatedAtNs: envelope.Metadata.CreatedAtNs,
			Type:        etype,
			Tags:        map[uint64]string{},
			Properties:  map[uint64]string{},
		}

		for k, v := range meta.Tags {
			tk, err := dictionary.GetUniqueTerm(k)
			if err != nil {
				log.Fatal("failed to get dictionary value for %v", meta)
			}
			persisted.Tags[tk] = depths.Cleanup(strings.ToLower(v))
		}

		for k, v := range meta.Properties {
			pk, err := dictionary.GetUniqueTerm(k)
			if err != nil {
				log.Fatal("failed to get dictionary value for %v", m)
			}
			persisted.Properties[pk] = v
		}

		encoded, err := proto.Marshal(persisted)
		if err != nil {
			log.Fatal("failed to marshal %v", persisted)
		}

		id, err := forward.Append(maker, encoded)
		if err != nil {
			log.Fatal(err)
		}

		inverted.Append(int64(id), makerKey, meta.Maker)
		inverted.Append(int64(id), typeKey, meta.Type)
		for k, v := range persisted.Tags {
			inverted.Append(int64(id), k, v)
		}

		log.Infof("message at topic/partition/offset %v/%v/%v: %s\n", m.Topic, m.Partition, m.Offset, envelope.Metadata.String())
	}
}
