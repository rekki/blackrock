package main

import (
	"context"
	"errors"
	"fmt"
	"path"
	"strconv"
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
	writers map[int64]*disk.ForwardWriter
	root    string
	counter uint64
	sync.Mutex
}

var dateCache = NewDateCache()

// hack to backfix some wrongly flattened keys
// this should be some configurable go script
func Fixme(k, v string) (string, string) {
	if v == "true" {
		splitted := strings.Split(k, ".")
		if len(splitted) > 1 {
			for i := 0; i < len(splitted)-1; i++ {
				p := splitted[i]
				if strings.HasSuffix(p, "_code") {
					k = strings.Join(splitted[:i+1], ".")
					v = strings.Join(splitted[i+1:], ".")
					break
				}
			}
		}
	}
	return k, v
}

func ExtractLastNumber(s string, sep byte) (string, int, bool) {
	pos := -1
	for i := len(s) - 1; i >= 0; i-- {
		if s[i] == sep {
			pos = i
			break
		}
	}

	if pos > 0 && pos < len(s)-1 {
		v, err := strconv.ParseInt(s[pos+1:], 10, 32)
		if err != nil {
			return s, 0, false
		}
		return s[:pos], int(v), true
	}
	return s, 0, false
}

func PrepareEnvelope(envelope *spec.Envelope) {
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
		year, year_month, year_month_day, year_month_day_hour := dateCache.Expand(t)

		meta.Search = append(meta.Search, spec.KV{Key: "year", Value: year})
		meta.Search = append(meta.Search, spec.KV{Key: "year-month", Value: year_month})
		meta.Search = append(meta.Search, spec.KV{Key: "year-month-day", Value: year_month_day})
		meta.Search = append(meta.Search, spec.KV{Key: "year-month-day-hour", Value: year_month_day_hour})
	}
}

func consumeEventsFromAllPartitions(root string, pr []*PartitionReader) error {
	writers := map[int64]*disk.ForwardWriter{}
	l := log.WithField("root", root).WithField("mode", "CONSUME")
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
			counter := atomic.SwapUint64(&dw.counter, 0)
			l.Warnf("%d events consumed last second", counter)
		}
	}()

	var lastError error
	for range pr {
		err := <-errChan
		lastError = err
		l.WithError(err).Warnf("received error: %s", err)
		for _, p := range pr {
			p.Reader.Close()
		}
	}

	return lastError
}

func consumeEvents(dw *DiskWriter, pr *PartitionReader) error {
	l := log.WithField("root", dw.root).WithField("mode", "CONSUME")
	fileLock := flock.New(path.Join(dw.root, fmt.Sprintf("partition_%d.lock", pr.Partition.ID)))
	err := fileLock.Lock()
	if err != nil {
		return err
	}
	defer fileLock.Close() // also unlocks

	ow, err := disk.NewOffsetWriter(path.Join(dw.root, fmt.Sprintf("partition_%d.offset", pr.Partition.ID)), l)
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
			l.WithError(err).Warnf("failed to unmarshal, data: %s, error: %s", string(m.Value), err.Error())
			continue
		}

		if envelope.Metadata != nil {
			envelope.Metadata.Id = uint64(m.Partition)<<56 | uint64(m.Offset)
		}
		PrepareEnvelope(&envelope)
		segmentId := depths.SegmentFromNs(envelope.Metadata.CreatedAtNs)

		dw.Lock()
		forward, ok := dw.writers[segmentId]
		if !ok {
			segmentPath := path.Join(dw.root, fmt.Sprintf("%d", segmentId))
			l.Infof("openning new segment: %s", segmentPath)
			forward, err = disk.NewForwardWriter(segmentPath, "main")
			if err != nil {
				dw.Unlock()
				return err
			}

			dw.writers[segmentId] = forward
		}
		dw.Unlock()

		atomic.AddUint64(&dw.counter, 1)
		data, err := proto.Marshal(envelope.Metadata)
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
