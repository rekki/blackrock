package rend

import (
	"context"
	"sync"
	"time"

	_ "net/http/pprof"
	"strings"

	"github.com/gogo/protobuf/proto"
	"github.com/jackdoe/blackrock/depths"
	"github.com/jackdoe/blackrock/orgrim/spec"
	"github.com/segmentio/kafka-go"
	log "github.com/sirupsen/logrus"
)

type Rend struct {
	reader *kafka.Reader
	cache  map[string]map[string]*spec.Context
	sync.RWMutex
}

func NewRend(servers string, topic string) (*Rend, error) {
	err := depths.HealthCheckKafka(servers, topic)
	if err != nil {
		return nil, err
	}
	brokers := strings.Split(servers, ",")
	rd := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        brokers,
		Topic:          topic,
		CommitInterval: 0,
		GroupID:        "", // always start from scratch
		MaxWait:        1 * time.Second,
	})
	return &Rend{reader: rd, cache: map[string]map[string]*spec.Context{}}, nil
}

func (r *Rend) Lookup(t string, id string, from int64) (*spec.Context, bool) {
	r.RLock()
	defer r.RUnlock()
	m, ok := r.cache[t]
	if !ok {
		return nil, false
	}
	v, ok := m[id]
	// context is in the future
	if v.CreatedAtNs > from {
		return nil, false
	}
	return v, ok
}

func (r *Rend) Close() {
	log.Warnf("rend closing reader")
	r.reader.Close()
}
func (r *Rend) Consume() {
	ctx := context.Background()
	log.Warnf("rend waiting...")
	for {
		m, err := r.reader.ReadMessage(ctx)
		if err != nil {
			break
		}

		data := spec.Context{}
		err = proto.Unmarshal(m.Value, &data)
		if err != nil {
			log.Warnf("rend failed to unmarshal, data: %s, error: %s", string(m.Value), err.Error())
			continue
		}

		if data.CreatedAtNs == 0 {
			data.CreatedAtNs = time.Now().UnixNano()
		}

		if data.Type == "" {
			log.Warnf("rend empty type, data: %v", data)
			continue
		}

		if data.Id == "" {
			log.Warnf("rend empty id, data: %v", data)
			continue
		}

		r.Lock()
		mt, ok := r.cache[data.Type]
		if !ok {
			mt = map[string]*spec.Context{}
			r.cache[data.Type] = mt
		}
		mt[data.Id] = &data
		r.Unlock()

		log.Infof("rend message at topic/partition/offset %v/%v/%v: %v\n", m.Topic, m.Partition, m.Offset, data)
	}
}
