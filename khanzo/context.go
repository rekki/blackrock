package main

import (
	"sort"
	"strings"
	"sync"

	"github.com/gogo/protobuf/proto"
	"github.com/rekki/blackrock/jubei/disk"
	"github.com/rekki/blackrock/orgrim/spec"
	log "github.com/sirupsen/logrus"
)

type ContextCache struct {
	cache   map[string]map[string][]*spec.Context
	offset  uint32
	forward *disk.ForwardWriter
	sync.RWMutex
}

func insertSort(data []*spec.Context, el *spec.Context) []*spec.Context {
	index := sort.Search(len(data), func(i int) bool { return data[i].CreatedAtNs <= el.CreatedAtNs })

	if len(data) == 0 {
		return []*spec.Context{el}
	}

	if index < len(data) && data[index].CreatedAtNs == el.CreatedAtNs {
		data[index] = el
		return data
	}
	data = append(data, &spec.Context{})
	copy(data[index+1:], data[index:])
	data[index] = el

	return data
}

func NewContextCache(forward *disk.ForwardWriter) *ContextCache {
	return &ContextCache{
		cache:   map[string]map[string][]*spec.Context{},
		forward: forward,
		offset:  0,
	}
}

func (r *ContextCache) Insert(decoded *spec.Context) {
	r.Lock()
	mt, ok := r.cache[decoded.ForeignType]
	if !ok {
		log.Infof("creating new type %s", decoded.ForeignType)
		mt = map[string][]*spec.Context{}
		r.cache[decoded.ForeignType] = mt
	}
	mt[decoded.ForeignId] = insertSort(mt[decoded.ForeignId], decoded)
	r.Unlock()

	log.Infof("setting %s:%s [%d] to %v", decoded.ForeignType, decoded.ForeignId, decoded.CreatedAtNs, decoded)
}

func (r *ContextCache) Lookup(t string, id string, from int64) (*spec.Context, bool) {
	// XXX: use r.RLock() per request
	m, ok := r.cache[t]
	if !ok {
		return nil, false
	}

	v, ok := m[id]
	if ok {
		if len(v) < 8 {
			for _, vv := range v {
				if vv.CreatedAtNs <= from {
					return vv, true
				}
			}
		} else {
			index := sort.Search(len(v), func(i int) bool { return v[i].CreatedAtNs <= from })

			if index >= len(v) {
				index = len(v) - 1
			}
			if v[index].CreatedAtNs <= from {
				return v[index], true
			}
		}
	}
	return nil, false
}

func (r *ContextCache) Scan() error {
	log.Warnf("scanning from %d", r.offset)
	n := 0
	err := r.forward.Scan(r.offset, func(offset uint32, data []byte) error {
		decoded := &spec.Context{}
		err := proto.Unmarshal(data, decoded)
		if err != nil {
			log.Warnf("rend failed to unmarshal, data: %s, error: %s", string(data), err.Error())
			return nil
		}
		sort.Slice(decoded.Properties, func(i, j int) bool {
			return decoded.Properties[i].Key < decoded.Properties[j].Key
		})
		r.Insert(decoded)
		r.offset = offset
		n++
		return nil
	})
	log.Warnf("scanning finished at %d, got %d new entries", r.offset, n)
	return err
}

func toContextDeep(seen map[string]map[string]bool, contextCache *ContextCache, p *spec.Context) []*spec.Context {
	out := []*spec.Context{p}
	for _, kv := range p.Properties {
		k := kv.Key
		v := kv.Value

		m, ok := seen[k]
		if !ok {
			m = map[string]bool{}
			seen[k] = m
		}
		_, ok = m[v]
		if ok {
			continue
		}
		m[v] = true

		if strings.HasSuffix(k, "_id") {
			if px, ok := contextCache.Lookup(k, v, p.CreatedAtNs); ok {
				out = append(out, toContextDeep(seen, contextCache, px)...)
			}
		}
	}

	return out
}

func LoadContextForStat(contextCache *ContextCache, k, v string, t int64) []*spec.Context {
	seen := map[string]map[string]bool{}
	out := []*spec.Context{}

	if px, ok := contextCache.Lookup(k, v, t); ok {
		out = append(out, toContextDeep(seen, contextCache, px)...)
	}

	return out
}
