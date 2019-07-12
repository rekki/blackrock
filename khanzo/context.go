package main

import (
	"sync"

	"github.com/gogo/protobuf/proto"
	"github.com/jackdoe/blackrock/jubei/disk"
	"github.com/jackdoe/blackrock/orgrim/spec"
	log "github.com/sirupsen/logrus"
)

type ContextCache struct {
	cache   map[uint64]map[string]*spec.PersistedContext
	offset  uint64
	forward *disk.ForwardWriter
	sync.RWMutex
}

func NewContextCache(forward *disk.ForwardWriter) *ContextCache {
	return &ContextCache{
		cache:   map[uint64]map[string]*spec.PersistedContext{},
		forward: forward,
		offset:  0,
	}
}

func (r *ContextCache) Lookup(t uint64, id string, from int64) (*spec.PersistedContext, bool) {
	r.RLock()
	defer r.RUnlock()
	m, ok := r.cache[t]
	if !ok {
		return nil, false
	}
	v, ok := m[id]
	// context is in the future
	if ok && v.CreatedAtNs <= from {
		return nil, false
	}
	return v, ok
}

func (r *ContextCache) Scan() error {
	log.Printf("scanning from %d", r.offset)
	n := 0
	err := r.forward.Scan(r.offset, true, func(offset uint64, id uint64, data []byte) error {
		decoded := spec.PersistedContext{}
		err := proto.Unmarshal(data, &decoded)
		if err != nil {
			log.Warnf("rend failed to unmarshal, data: %s, error: %s", string(data), err.Error())
			return nil
		}

		r.Lock()
		mt, ok := r.cache[decoded.Type]
		if !ok {
			log.Infof("creating new type %d", decoded.Type)
			mt = map[string]*spec.PersistedContext{}
			r.cache[decoded.Type] = mt
		}
		log.Infof("setting %d:%s to %v", decoded.Type, decoded.ForeignId, decoded)
		mt[decoded.ForeignId] = &decoded
		r.Unlock()

		r.offset = offset
		n++
		return nil
	})
	log.Printf("scanning finished at %d, got %d new entries", r.offset, n)
	return err
}
