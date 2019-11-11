package main

import (
	"context"
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"os/signal"
	"path"
	"strconv"
	"sync"
	"syscall"
	"time"

	_ "net/http/pprof"
	"strings"

	"github.com/gofrs/flock"
	"github.com/gogo/protobuf/proto"
	"github.com/rekki/blackrock/cmd/jubei/consume"
	"github.com/rekki/blackrock/cmd/jubei/disk"
	"github.com/rekki/blackrock/cmd/orgrim/spec"
	"github.com/rekki/blackrock/pkg/depths"
	"github.com/segmentio/kafka-go"
	_ "github.com/segmentio/kafka-go/snappy"
	log "github.com/sirupsen/logrus"
)

type DiskWriter struct {
	inverted *disk.InvertedWriter
	writers  map[string]*disk.ForwardWriter
	root     string
	sync.Mutex
}

func consumeEventsFromAllPartitions(root string, pr []*PartitionReader, maxDescriptors int) error {
	log.Warnf("waiting... ")

	inverted, err := disk.NewInvertedWriter(maxDescriptors)
	if err != nil {
		log.Fatal(err)
	}
	writers := map[string]*disk.ForwardWriter{}

	dw := &DiskWriter{inverted: inverted, writers: writers, root: root}
	errChan := make(chan error)

	for _, p := range pr {
		go func(p *PartitionReader) {
			errChan <- consumeEvents(dw, p)
		}(p)
	}

	for range pr {
		err := <-errChan
		log.Printf("received error: %s", err)
		for _, p := range pr {
			p.Reader.Close()
		}
	}

	return nil
}

func consumeEvents(dw *DiskWriter, pr *PartitionReader) error {
	// XXX(jackdoe): there is no fsync at the moment, use at your own risk

	fileLock := flock.New(path.Join(dw.root, fmt.Sprintf("partition_%d.lock", pr.Partition.ID)))
	err := fileLock.Lock()
	if err != nil {
		return err
	}
	defer fileLock.Close() // also unlocks

	state, err := os.OpenFile(path.Join(dw.root, fmt.Sprintf("partition_%d.offset", pr.Partition.ID)), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return err
	}
	defer state.Close() // close also syncs

	storedOffset := make([]byte, 16)
	var offset int64
	_, err = state.ReadAt(storedOffset, 0)
	if err != nil {
		log.Warnf("failed to read bytes, setting offset to First, err: %s", err)
		offset = kafka.FirstOffset
	} else {
		offset = int64(binary.LittleEndian.Uint64(storedOffset)) + 1
		sum := binary.LittleEndian.Uint64(storedOffset[8:])
		expected := depths.Hash(storedOffset[:8])
		if expected != sum {
			return fmt.Errorf("bad checksum partition: %d, expected: %d, got %d, bytes: %v", pr.Partition.ID, expected, sum, storedOffset)
		}
	}

	err = pr.Reader.SetOffset(offset)
	if err != nil {
		return err
	}
	log.Warnf("starting partition: %d at offset: %d", pr.Partition.ID, offset)
	ctx := context.Background()
	for {
		m, err := pr.Reader.FetchMessage(ctx)
		if err != nil {
			return err
		}

		envelope := spec.Envelope{}
		err = proto.Unmarshal(m.Value, &envelope)
		if err != nil {
			log.Warnf("failed to unmarshal, data: %s, error: %s", string(m.Value), err.Error())
			continue
		}

		if envelope.Metadata != nil {
			envelope.Metadata.Id = uint64(m.Partition)<<56 | uint64(m.Offset)
		}

		segmentId := path.Join(dw.root, depths.SegmentFromNs(envelope.Metadata.CreatedAtNs))

		dw.Lock()
		forward, ok := dw.writers[segmentId]
		if !ok {
			log.Warnf("openning new segment: %s", segmentId)
			forward, err = disk.NewForwardWriter(segmentId, "main")
			if err != nil {
				dw.Unlock()
				return err
			}

			dw.writers[segmentId] = forward
		}

		err = consume.ConsumeEvents(segmentId, &envelope, forward, dw.inverted)
		if err != nil {
			dw.Unlock()
			return err
		}
		dw.Unlock()

		log.Infof("message at topic/partition/offset %v/%v/%v: %v\n", m.Topic, m.Partition, m.Offset, envelope.Metadata)
		binary.LittleEndian.PutUint64(storedOffset[0:], uint64(m.Offset))
		offsetBytes := make([]byte, 8)
		binary.LittleEndian.PutUint64(offsetBytes, uint64(m.Offset))
		binary.LittleEndian.PutUint64(storedOffset[8:], depths.Hash(offsetBytes))
		_, err = state.WriteAt(storedOffset, 0)
		if err != nil {
			// XXX(jackdoe): now we are fucked, the index is already written, and we could not write the offset
			// which means we will double write events
			// panic panic
			return err
		}
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

func main() {
	var dataTopic = flag.String("topic-data", "blackrock-data", "topic for the data")
	var proot = flag.String("root", "/blackrock", "root directory for the files")
	var kafkaServers = flag.String("kafka", "localhost:9092", "comma separated list of kafka servers")
	var verbose = flag.Bool("verbose", false, "print info level logs to stdout")
	var maxDescriptors = flag.Int("max-descriptors", 1000, "max open descriptors")
	var compact = flag.Bool("compact", false, "compact everything until today and exit")
	flag.Parse()
	go func() {
		log.Println(http.ListenAndServe("localhost:6061", nil))
	}()

	root := *proot

	if err := os.MkdirAll(root, 0700); err != nil {
		log.Fatal(err)
	}
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

	partitions, err := ReadPartitions(*kafkaServers, *dataTopic)
	if err != nil {
		log.Fatal(err)
	}

	brokers := strings.Split(*kafkaServers, ",")
	readers := []*PartitionReader{}
	for _, p := range partitions {
		rd := kafka.NewReader(kafka.ReaderConfig{
			Brokers:   brokers,
			Topic:     *dataTopic,
			MaxWait:   1 * time.Second,
			Partition: p.ID,
		})
		readers = append(readers, &PartitionReader{rd, p})
	}

	sigs := make(chan os.Signal, 100)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	cleanup := func() {
		// no need to close the files, as they are closed on exit
		log.Warnf("closing the readers...")
		for _, r := range readers {
			r.Reader.Close()
		}
		os.Exit(0)
	}

	go func() {
		<-sigs

		cleanup()
	}()

	go func() {
		err := consumeEventsFromAllPartitions(root, readers, *maxDescriptors)
		log.Warnf("error consuming events: %s", err.Error())
		sigs <- syscall.SIGTERM
	}()

	for {
		for _, rd := range readers {
			s := rd.Reader.Stats()
			log.Warnf("STATS: partition: %d, lag: %d, messages: %d\n", rd.Partition.ID, s.Lag, s.Messages)
		}

		time.Sleep(1 * time.Minute)
	}
}
