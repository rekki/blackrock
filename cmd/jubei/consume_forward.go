package main

import (
	"context"
	"errors"
	"fmt"
	"path"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gofrs/flock"
	"github.com/gogo/protobuf/proto"

	"github.com/rekki/blackrock/cmd/jubei/disk"
	"github.com/rekki/blackrock/cmd/orgrim/spec"
	"github.com/rekki/blackrock/pkg/depths"
	"github.com/segmentio/kafka-go"
	log "github.com/sirupsen/logrus"
)

type DiskWriter struct {
	writers map[string]*disk.ForwardWriter
	root    string
	counter uint64
	sync.Mutex
}

func prepare(envelope *spec.Envelope) {
	meta := envelope.Metadata
	foreignId := depths.Cleanup(strings.ToLower(meta.ForeignId))
	foreignType := depths.Cleanup(strings.ToLower(meta.ForeignType))
	eventType := depths.Cleanup(strings.ToLower(meta.EventType))

	meta.EventType = eventType
	meta.ForeignType = foreignType
	meta.ForeignId = foreignId
	second := int32(meta.CreatedAtNs / 1e9)
	for i, kv := range meta.Search {
		k := kv.Key
		v := kv.Value

		lc := strings.ToLower(k)
		lc, v = Fixme(lc, v)

		lc = depths.Cleanup(lc)
		if lc == "event_type" || lc == "foreign_type" || lc == "foreign_id" || lc == foreignType || lc == "" {
			continue
		}

		value := depths.Cleanup(strings.ToLower(v))
		if value == "" {
			value = "__empty"
		}
		meta.Search[i] = spec.KV{Key: lc, Value: value}

		if k == "experiment" && strings.HasPrefix(value, "exp_") {
			name, variant, ok := ExtractLastNumber(value, byte('_'))
			if ok {
				if meta.Track == nil {
					meta.Track = map[string]uint32{}
				}
				meta.Track[name] = uint32(variant)
			}
		}
	}
	// add some automatic tags
	{
		t := time.Unix(int64(second), 0).UTC()
		year, month, day := t.Date()
		hour, _, _ := t.Clock()
		meta.Search = append(meta.Search, spec.KV{Key: "year", Value: fmt.Sprintf("%d", year)})
		meta.Search = append(meta.Search, spec.KV{Key: "year-month", Value: fmt.Sprintf("%d-%02d", year, month)})
		meta.Search = append(meta.Search, spec.KV{Key: "year-month-day", Value: fmt.Sprintf("%d-%02d-%02d", year, month, day)})
		meta.Search = append(meta.Search, spec.KV{Key: "year-month-day-hour", Value: fmt.Sprintf("%d-%02d-%02d-%02d", year, month, day, hour)})
	}
}

func consumeEventsFromAllPartitions(root string, pr []*PartitionReader) error {
	writers := map[string]*disk.ForwardWriter{}

	dw := &DiskWriter{writers: writers, root: root}
	errChan := make(chan error)

	for _, p := range pr {

		go func(p *PartitionReader) {
			errChan <- consumeEvents(dw, p)
		}(p)
	}

	go func() {
		for {
			time.Sleep(1 * time.Second)
			log.Warnf("%d events last second", dw.counter)
			atomic.StoreUint64(&dw.counter, 0)
		}
	}()

	for range pr {
		err := <-errChan
		log.Printf("received error: %s", err)
		for _, p := range pr {
			p.Reader.Close()
		}
	}

	return nil
}

const MAX_OPEN_WRITERS = 128

func consumeEvents(dw *DiskWriter, pr *PartitionReader) error {
	fileLock := flock.New(path.Join(dw.root, fmt.Sprintf("partition_%d.lock", pr.Partition.ID)))
	err := fileLock.Lock()
	if err != nil {
		return err
	}
	defer fileLock.Close() // also unlocks

	ow, err := NewOffsetWriter(path.Join(dw.root, fmt.Sprintf("partition_%d.offset", pr.Partition.ID)))
	if err != nil {
		return err
	}
	defer ow.Close() // close also syncs

	offset, err := ow.ReadOrDefault(kafka.FirstOffset)
	if err != nil {
		return err
	}

	if offset != kafka.FirstOffset {
		offset++ // start from the next one, as we already stored the current one
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
		prepare(&envelope)
		segmentId := path.Join(dw.root, depths.SegmentFromNs(envelope.Metadata.CreatedAtNs))

		dw.Lock()
		forward, ok := dw.writers[segmentId]
		if !ok {
			if len(dw.writers) > MAX_OPEN_WRITERS {
				for k, v := range dw.writers {
					v.Close()
					delete(dw.writers, k)
				}
			}

			log.Warnf("openning new segment: %s", segmentId)
			forward, err = disk.NewForwardWriter(segmentId, "main")
			if err != nil {
				dw.Unlock()
				return err
			}

			dw.writers[segmentId] = forward
		}

		dw.Unlock()
		atomic.AddUint64(&dw.counter, 1)
		data, err := proto.Marshal(&envelope)
		if err != nil {
			return err
		}
		_, err = forward.Append(data)
		if err != nil {
			return err
		}

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
