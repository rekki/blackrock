package main

import (
	"context"
	"encoding/binary"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path"
	"syscall"
	"time"

	"strings"

	"github.com/gogo/protobuf/proto"
	"github.com/jackdoe/blackrock/jubei/sanitize"
	"github.com/jackdoe/blackrock/orgrim/spec"
	"github.com/segmentio/kafka-go"
	log "github.com/sirupsen/logrus"
)

type FileWriter struct {
	descriptors        map[string]*os.File
	maxOpenDescriptors int
	forward            *os.File
	root               string
	topic              string
	offset             uint64
}

func NewFileWriter(root string, topic string, maxOpenDescriptors int) (*FileWriter, error) {
	os.MkdirAll(path.Join(root, topic), 0700)
	filename := path.Join(root, topic, "forward.bin")
	fd, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return nil, err
	}
	off, err := fd.Seek(0, os.SEEK_END)
	if err != nil {
		return nil, err
	}
	log.Infof("forward %s with %d entries", filename, off/8)
	return &FileWriter{
		maxOpenDescriptors: maxOpenDescriptors,
		descriptors:        map[string]*os.File{},
		root:               root,
		topic:              topic,
		forward:            fd,
		offset:             uint64(off) / 8,
	}, nil
}

func (fw *FileWriter) sync() {
	for _, f := range fw.descriptors {
		f.Sync()
	}
}

func (fw *FileWriter) appendForward(partition int32, offset int64) (uint64, error) {
	current := fw.offset
	log.Infof("writing kafka offset %d:%d as id %d", partition, offset, current)
	fw.offset++
	off := make([]byte, 8)
	binary.LittleEndian.PutUint64(off, uint64(partition)<<54|uint64(offset))
	_, err := fw.forward.WriteAt(off, int64(current*uint64(8)))
	if err != nil {
		return 0, err
	}

	return current, nil
}

func (fw *FileWriter) appendTag(docId uint64, tagKey, tagValue string) error {
	dir, fn := sanitize.PathForTag(fw.root, fw.topic, tagKey, tagValue)
	filename := path.Join(dir, fn)
	f, ok := fw.descriptors[filename]
	if !ok {
		if len(fw.descriptors) > fw.maxOpenDescriptors {
			log.Warnf("clearing descriptor cache len: %s", len(fw.descriptors))
			for dk, fd := range fw.descriptors {
				fd.Close()
				delete(fw.descriptors, dk)
			}
		}
		log.Infof("openning %s", filename)
		os.MkdirAll(dir, 0700)
		fd, err := os.OpenFile(filename, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
		if err != nil {
			return err
		}
		f = fd
		fw.descriptors[filename] = fd

	}
	log.Infof("writing document id %d at %s", docId, filename)
	data := make([]byte, 8)

	binary.LittleEndian.PutUint64(data, docId)
	_, err := f.Write(data)
	if err != nil {
		return err
	}
	return nil
}

func (fw *FileWriter) append(docId uint64, metadata *spec.Metadata) error {
	ns := metadata.CreatedAtNs
	for k, v := range metadata.Tags {
		err := fw.appendTag(docId, k, v)
		if err != nil {
			return err
		}
	}

	second := ns / 1000000000
	t := time.Unix(second, 0)
	year, month, day := t.Date()
	hour, minute, _ := t.Clock()
	fw.appendTag(docId, "year", fmt.Sprintf("%d", year))
	fw.appendTag(docId, "year-month", fmt.Sprintf("%d-%02d", year, month))
	fw.appendTag(docId, "year-month-day", fmt.Sprintf("%d-%02d-%02d", year, month, day))
	fw.appendTag(docId, "year-month-day-hour", fmt.Sprintf("%d-%02d-%02d-%02d", year, month, day, hour))
	fw.appendTag(docId, "year-month-day-hour-minute", fmt.Sprintf("%d-%02d-%02d-%02d:%02d", year, month, day, hour, minute))
	return nil
}

func main() {
	var dataTopic = flag.String("topic-data", "blackrock-data", "topic for the data")
	var root = flag.String("root", "/blackrock", "root directory for the files")
	var kafkaServers = flag.String("kafka", "localhost:9092,localhost:9092", "kafka addrs")
	var verbose = flag.Bool("verbose", false, "print info level logs to stdout")
	var maxDescriptors = flag.Int("max-descriptors", 1000, "max open descriptors")

	flag.Parse()

	if *verbose {
		log.SetLevel(log.InfoLevel)
	} else {
		log.SetLevel(log.WarnLevel)
	}
	consumerId, _ := os.Hostname()
	brokers := strings.Split(*kafkaServers, ",")
	rd := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        brokers,
		Topic:          *dataTopic,
		GroupID:        "jubei_" + consumerId,
		CommitInterval: 1 * time.Second,
		MaxWait:        1 * time.Second,
	})
	defer rd.Close()
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigs
		log.Warnf("closing the reader...")
		// no need to close the files, as they are closed on exit
		rd.Close()
		os.Exit(0)
	}()

	fw, err := NewFileWriter(*root, *dataTopic, *maxDescriptors)
	if err != nil {
		log.Fatal(err)
	}

	ctx := context.Background()
	log.Warnf("waiting...")
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
		id, err := fw.appendForward(int32(m.Partition), m.Offset)
		if err != nil {
			log.Fatal(err)
		}

		err = fw.append(id, envelope.Metadata)
		if err != nil {
			log.Fatal(err)
		}

		log.Infof("message at topic/partition/offset %v/%v/%v: %s\n", m.Topic, m.Partition, m.Offset, envelope.Metadata.String())
	}
}
