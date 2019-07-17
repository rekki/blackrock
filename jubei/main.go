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

func consumeEvents(r *kafka.Reader, dictionary *disk.PersistedDictionary, forward *disk.ForwardWriter, payload *disk.ForwardWriter, inverted *disk.InvertedWriter) error {
	typeKey, err := dictionary.GetUniqueTerm("event_type")
	if err != nil {
		return err
	}

	yearKey, err := dictionary.GetUniqueTerm("year")
	if err != nil {
		return err
	}
	yearMonthKey, err := dictionary.GetUniqueTerm("year-month")
	if err != nil {
		return err
	}

	yearMonthDayKey, err := dictionary.GetUniqueTerm("year-month-day")
	if err != nil {
		return err
	}

	yearMonthDayHourKey, err := dictionary.GetUniqueTerm("year-month-day-hour")
	if err != nil {
		return err
	}

	log.Warnf("waiting... [typeKey: %d]", typeKey)
	ctx := context.Background()
	for {
		m, err := r.ReadMessage(ctx)
		if err != nil {
			return err
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
		sforeignId := depths.Cleanup(strings.ToLower(meta.ForeignId))
		foreignId, err := dictionary.GetUniqueTerm(sforeignId)
		if err != nil {
			return err
		}
		sforeignType := depths.Cleanup(strings.ToLower(meta.ForeignType))
		foreignType, err := dictionary.GetUniqueTerm(sforeignType)
		if err != nil {
			return err
		}

		poff, err := payload.Append(foreignId, foreignType, envelope.Payload)
		if err != nil {
			return err
		}

		seventType := depths.Cleanup(strings.ToLower(meta.EventType))
		eventType, err := dictionary.GetUniqueTerm(seventType)
		if err != nil {
			return err
		}

		persisted := &spec.PersistedMetadata{
			Partition:   uint32(m.Partition),
			Offset:      uint64(m.Offset),
			CreatedAtNs: envelope.Metadata.CreatedAtNs,
			TagKeys:     []uint64{},
			Payload:     poff,
			EventType:   eventType,
			ForeignType: foreignType,
			ForeignId:   sforeignId,
		}

		for _, kv := range meta.Tags {
			k := kv.Key
			v := kv.Value
			lc := depths.CleanupAllowDot(strings.ToLower(k))
			if lc == "event_type" || lc == "foreign_type" || lc == "foreign_id" || lc == sforeignType {
				continue
			}

			tk, err := dictionary.GetUniqueTerm(lc)
			if err != nil {
				return err
			}
			persisted.TagKeys = append(persisted.TagKeys, tk)
			persisted.TagValues = append(persisted.TagValues, depths.Cleanup(strings.ToLower(v)))
		}

		ns := meta.CreatedAtNs
		second := ns / 1000000000
		t := time.Unix(second, 0).UTC()
		year, month, day := t.Date()
		hour, _, _ := t.Clock()

		persisted.TagKeys = append(persisted.TagKeys, yearKey)
		persisted.TagValues = append(persisted.TagValues, fmt.Sprintf("%d", year))
		persisted.TagKeys = append(persisted.TagKeys, yearMonthKey)
		persisted.TagValues = append(persisted.TagValues, fmt.Sprintf("%d-%02d", year, month))

		persisted.TagKeys = append(persisted.TagKeys, yearMonthDayKey)
		persisted.TagValues = append(persisted.TagValues, fmt.Sprintf("%d-%02d-%02d", year, month, day))

		persisted.TagKeys = append(persisted.TagKeys, yearMonthDayHourKey)
		persisted.TagValues = append(persisted.TagValues, fmt.Sprintf("%d-%02d-%02d-%02d", year, month, day, hour))

		for _, kv := range meta.Properties {
			k := kv.Key
			v := kv.Value
			lc := depths.CleanupAllowDot(strings.ToLower(k))
			if lc == "event_type" || lc == "foreign_type" || lc == "foreign_id" || lc == sforeignType {
				continue
			}

			pk, err := dictionary.GetUniqueTerm(lc)
			if err != nil {
				return err
			}
			persisted.PropertyKeys = append(persisted.PropertyKeys, pk)
			persisted.PropertyValues = append(persisted.PropertyValues, v)
		}

		encoded, err := proto.Marshal(persisted)
		if err != nil {
			return err
		}

		docId, err := forward.Append(foreignId, foreignType, encoded)
		if err != nil {
			return err
		}

		inverted.Append(int64(docId), foreignType, sforeignId)
		inverted.Append(int64(docId), typeKey, seventType)

		for i := 0; i < len(persisted.TagKeys); i++ {
			inverted.Append(int64(docId), persisted.TagKeys[i], persisted.TagValues[i])
		}

		log.Infof("message at topic/partition/offset %v/%v/%v: %v\n", m.Topic, m.Partition, m.Offset, envelope.Metadata)
	}
}

func consumeContext(r *kafka.Reader, dictionary *disk.PersistedDictionary, forward *disk.ForwardWriter) error {
	idKey, err := dictionary.GetUniqueTerm("foreign_id")
	if err != nil {
		return err
	}

	log.Warnf("context waiting... [idKey: %d]", idKey)
	ctx := context.Background()
	for {
		m, err := r.ReadMessage(ctx)
		if err != nil {
			return err
		}

		envelope := spec.Context{}
		err = proto.Unmarshal(m.Value, &envelope)
		if err != nil {
			log.Warnf("context failed to unmarshal, data: %s, error: %s", string(m.Value), err.Error())
			continue
		}

		if envelope.CreatedAtNs == 0 {
			envelope.CreatedAtNs = time.Now().UnixNano()
		}

		sforeignType := depths.Cleanup(strings.ToLower(envelope.ForeignType))
		foreignType, err := dictionary.GetUniqueTerm(sforeignType)
		if err != nil {
			return err
		}

		sforeignId := depths.Cleanup(strings.ToLower(envelope.ForeignId))
		foreignId, err := dictionary.GetUniqueTerm(sforeignId)
		if err != nil {
			return err
		}

		persisted := &spec.PersistedContext{
			CreatedAtNs:  envelope.CreatedAtNs,
			PropertyKeys: []uint64{},
			ForeignType:  foreignType,
			ForeignId:    sforeignId,
		}

		for _, kv := range envelope.Properties {
			k := kv.Key
			v := kv.Value
			lc := depths.CleanupAllowDot(strings.ToLower(k))
			pk, err := dictionary.GetUniqueTerm(lc)
			if err != nil {
				return err
			}
			persisted.PropertyKeys = append(persisted.PropertyKeys, pk)
			persisted.PropertyValues = append(persisted.PropertyValues, v)
		}

		encoded, err := proto.Marshal(persisted)
		if err != nil {
			return err
		}

		_, err = forward.Append(foreignId, foreignType, encoded)
		//forward.Sync()
		if err != nil {
			return err
		}
		log.Infof("context message at topic/partition/offset %v/%v/%v: %v\n", m.Topic, m.Partition, m.Offset, envelope)
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

	log.Warnf("connecting as consumer %s", consumerId)
	brokers := strings.Split(*kafkaServers, ",")
	rd := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        brokers,
		Topic:          *dataTopic,
		GroupID:        consumerId,
		CommitInterval: 1 * time.Second,
		MaxWait:        1 * time.Second,
	})
	defer rd.Close()
	cd := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        brokers,
		Topic:          *contextTopic,
		GroupID:        consumerId,
		CommitInterval: 1 * time.Second,
		MaxWait:        1 * time.Second,
	})
	defer cd.Close()

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

	cleanup := func() {
		// no need to close the files, as they are closed on exit
		log.Warnf("closing the readers...")
		rd.Close()
		cd.Close()
		log.Warnf("closing the files...")
		inverted.Close()
		dictionary.Close()
		forward.Close()
		os.Exit(0)
	}

	go func() {
		<-sigs

		cleanup()
	}()

	go func() {
		err := consumeEvents(rd, dictionary, forward, payload, inverted)
		log.Warnf("error consuming events: %s", err.Error())
		sigs <- syscall.SIGTERM
	}()
	go func() {
		err = consumeContext(cd, dictionary, forwardContext)
		log.Warnf("error consuming context: %s", err.Error())
		sigs <- syscall.SIGTERM
	}()

	for {
		time.Sleep(1 * time.Minute)
	}
}
