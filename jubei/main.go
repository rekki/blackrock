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
	"strconv"
	"syscall"
	"time"

	_ "net/http/pprof"
	"strings"

	"github.com/gogo/protobuf/proto"
	"github.com/jackdoe/blackrock/depths"
	"github.com/jackdoe/blackrock/jubei/consume"
	"github.com/jackdoe/blackrock/jubei/disk"
	"github.com/jackdoe/blackrock/orgrim/spec"
	"github.com/segmentio/kafka-go"
	_ "github.com/segmentio/kafka-go/snappy"
	log "github.com/sirupsen/logrus"
)

func consumeEvents(root string, r *kafka.Reader, maxDescriptors int) error {
	log.Warnf("waiting... ")
	ctx := context.Background()
	inverted, err := disk.NewInvertedWriter(maxDescriptors)
	if err != nil {
		log.Fatal(err)
	}
	writers := map[string]*disk.ForwardWriter{}

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

		segmentId := path.Join(root, depths.SegmentFromNs(envelope.Metadata.CreatedAtNs))
		forward, ok := writers[segmentId]
		if !ok {
			log.Warnf("openning new segment: %s", segmentId)
			forward, err = disk.NewForwardWriter(segmentId, "main")
			if err != nil {
				return err
			}

			writers[segmentId] = forward
		}

		err = consume.ConsumeEvents(segmentId, &envelope, forward, inverted)
		if err != nil {
			return err
		}

		log.Infof("message at topic/partition/offset %v/%v/%v: %v\n", m.Topic, m.Partition, m.Offset, envelope.Metadata)
	}
}

func consumeContext(r *kafka.Reader, forward *disk.ForwardWriter) error {
	log.Warnf("context waiting...")
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

		err = consume.ConsumeContext(&envelope, forward)
		if err != nil {
			return err
		}
		log.Infof("context message at topic/partition/offset %v/%v/%v: %v\n", m.Topic, m.Partition, m.Offset, envelope)
	}
}

func compactEverything(root string) error {
	days, err := ioutil.ReadDir(path.Join(root))
	if err != nil {
		return err
	}

	for _, day := range days {
		if !day.IsDir() || !depths.IsDigit(day.Name()) {
			continue
		}

		n, err := strconv.Atoi(day.Name())
		if err != nil {
			log.Warnf("skipping %s", day.Name())
			continue
		}
		ts := n * 3600 * 24
		t := time.Unix(int64(ts), 0)
		since := time.Since(t)
		if since < time.Hour*48 {
			log.Warnf("skipping %s, too new: %s", day.Name(), since)
			continue
		}
		p := path.Join(root, day.Name())

		if _, err := os.Stat(path.Join(p, "segment.header")); !os.IsNotExist(err) {
			log.Warnf("skipping %s, already compacted", day.Name())
			continue
		}

		log.Warnf("reading %v", p)
		segment, err := disk.ReadAllTermsInSegment(p)
		if err != nil {
			return err
		}

		log.Warnf("compacting %v, %d terms", p, len(segment))
		err = disk.WriteCompactedIndex(p, segment)
		if err != nil {
			return err
		}
		log.Warnf("deleting raw files %v", p)
		err = disk.DeleteUncompactedPostings(p)
		if err != nil {
			return err
		}
	}
	return nil
}

func main() {
	var dataTopic = flag.String("topic-data", "blackrock-data", "topic for the data")
	var contextTopic = flag.String("topic-context", "blackrock-context", "topic for the context")
	var proot = flag.String("root", "/blackrock", "root directory for the files")
	var pqueuelen = flag.Int("kafka-queue-capacity", 1000, "internal queue capacity")
	var kafkaServers = flag.String("kafka", "localhost:9092,localhost:9092", "kafka addrs")
	var verbose = flag.Bool("verbose", false, "print info level logs to stdout")
	var maxDescriptors = flag.Int("max-descriptors", 1000, "max open descriptors")
	var compact = flag.Bool("compact", false, "compact everything until today and exit")
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

	if *compact {
		err := compactEverything(root)
		t0 := time.Now()
		if err != nil {
			log.Fatal(err)
		}
		log.Warnf("successfully compacted everything in %s, took %s", root, time.Since(t0))
		os.Exit(0)
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
	log.Warnf("connecting as consumer '%s'", consumerId)
	brokers := strings.Split(*kafkaServers, ",")
	rd := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        brokers,
		Topic:          *dataTopic,
		GroupID:        consumerId,
		CommitInterval: 1 * time.Second,
		MaxWait:        1 * time.Second,
		QueueCapacity:  *pqueuelen,
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

	forwardContext, err := disk.NewForwardWriter(root, "context")
	if err != nil {
		log.Fatal(err)
	}

	sigs := make(chan os.Signal, 100)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	cleanup := func() {
		// no need to close the files, as they are closed on exit
		log.Warnf("closing the readers...")
		rd.Close()
		cd.Close()
		os.Exit(0)
	}

	go func() {
		<-sigs

		cleanup()
	}()

	go func() {
		err := consumeEvents(root, rd, *maxDescriptors)
		log.Warnf("error consuming events: %s", err.Error())
		sigs <- syscall.SIGTERM
	}()
	go func() {
		err = consumeContext(cd, forwardContext)
		log.Warnf("error consuming context: %s", err.Error())
		sigs <- syscall.SIGTERM
	}()

	for {
		s := rd.Stats()
		fmt.Printf("%s\n", depths.DumpObj(s))

		s = cd.Stats()
		fmt.Printf("%s\n", depths.DumpObj(s))

		time.Sleep(1 * time.Minute)
	}
}
