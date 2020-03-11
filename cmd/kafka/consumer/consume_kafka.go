package main

import (
	"context"
	"errors"
	"fmt"
	"path"
	"strings"
	"time"

	"github.com/gofrs/flock"
	"github.com/gogo/protobuf/proto"
	spec "github.com/rekki/blackrock/pkg/blackrock_io"
	"github.com/rekki/blackrock/pkg/depths"
	"github.com/rekki/blackrock/pkg/logger"
	pen "github.com/rekki/go-pen"
	"github.com/segmentio/kafka-go"
)

func consumeEvents(si spec.SearchClient, root string, pr *PartitionReader) error {
	l := logger.Log
	fileLock := flock.New(path.Join(root, fmt.Sprintf("partition_%d.lock", pr.Partition.ID)))
	err := fileLock.Lock()
	if err != nil {
		return err
	}
	defer fileLock.Close() // also unlocks

	ow, err := pen.NewOffsetWriter(path.Join(root, fmt.Sprintf("partition_%d.offset", pr.Partition.ID)))
	if err != nil {
		return err
	}
	defer ow.Close() // close also syncs

	offset := ow.ReadOrDefault(kafka.FirstOffset)

	if offset != kafka.FirstOffset {
		offset++ // start from the next one, as we already stored the current one
	}
	err = pr.Reader.SetOffset(offset)
	if err != nil {
		return err
	}

	l.Infof("starting partition: %d at offset: %d", pr.Partition.ID, offset)
	ctx := context.Background()

	for {
		m, err := pr.Reader.FetchMessage(ctx)
		if err != nil {
			return err
		}

		envelope := spec.Envelope{}
		err = proto.Unmarshal(m.Value, &envelope)
		if err != nil {
			l.Warnf("failed to unmarshal, data: %s, error: %s", string(m.Value), err.Error())
			continue
		}

		if envelope.Metadata != nil {
			envelope.Metadata.Id = uint64(m.Partition)<<56 | uint64(m.Offset)
		}

		// FIXME: use one stream complicates because message can be in the wire, but it is not guaranteed to be written
		// so we do one by one
		stream, err := si.SayPush(context.Background())
		if err != nil {
			panic(err)
		}

		err = stream.Send(&envelope)
		if err != nil {
			panic(err)
		}

		_, err = stream.CloseAndRecv()
		if err != nil {
			panic(err)
		}

		l.Infof("consumed event at partition: %d, offset: %d", m.Partition, m.Offset)
		err = ow.SetOffset(m.Offset)
		if err != nil {
			panic(err)
		}
	}
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

func consumeKafka(si spec.SearchClient, root, dataTopic, kafkaServers string) error {
	partitions, err := ReadPartitions(kafkaServers, dataTopic)
	if err != nil {
		return err
	}

	brokers := strings.Split(kafkaServers, ",")
	readers := []*PartitionReader{}
	for _, p := range partitions {
		rd := kafka.NewReader(kafka.ReaderConfig{
			Brokers:   brokers,
			Topic:     dataTopic,
			MaxWait:   1 * time.Second,
			Partition: p.ID,
		})
		readers = append(readers, &PartitionReader{rd, p})
	}

	err = consumeEventsFromAllPartitions(si, root, readers)
	if err != nil {
		logger.Log.Warnf("error consuming events: %s", err.Error())
		return err
	}
	return nil
}

func consumeEventsFromAllPartitions(si spec.SearchClient, root string, pr []*PartitionReader) error {
	errChan := make(chan error)

	for _, p := range pr {
		go func(p *PartitionReader) {
			errChan <- consumeEvents(si, root, p)
		}(p)
	}

	var lastError error
	for range pr {
		err := <-errChan
		lastError = err
		logger.Log.Warnf("received error: %s", err)
		for _, p := range pr {
			p.Reader.Close()
		}
	}

	return lastError
}
