package main

import (
	"sort"
	"sync"

	"github.com/gogo/protobuf/proto"
	"github.com/jackdoe/blackrock/jubei/disk"
	"github.com/jackdoe/blackrock/orgrim/spec"
	log "github.com/sirupsen/logrus"
)

type ContextCache struct {
	cache   map[uint64]map[string][]*spec.PersistedContext
	offset  uint64
	forward *disk.ForwardWriter
	sync.RWMutex
}

func insertSort(data []*spec.PersistedContext, el *spec.PersistedContext) []*spec.PersistedContext {
	index := sort.Search(len(data), func(i int) bool { return data[i].CreatedAtNs <= el.CreatedAtNs })

	if len(data) == 0 {
		return []*spec.PersistedContext{el}
	}

	if index < len(data) && data[index].CreatedAtNs == el.CreatedAtNs {
		data[index] = el
		return data
	}
	data = append(data, &spec.PersistedContext{})
	copy(data[index+1:], data[index:])
	data[index] = el

	return data
}

func NewContextCache(forward *disk.ForwardWriter) *ContextCache {
	return &ContextCache{
		cache:   map[uint64]map[string][]*spec.PersistedContext{},
		forward: forward,
		offset:  0,
	}
}

func (r *ContextCache) Insert(decoded *spec.PersistedContext) {
	r.Lock()
	mt, ok := r.cache[decoded.ForeignType]
	if !ok {
		log.Infof("creating new type %d", decoded.ForeignType)
		mt = map[string][]*spec.PersistedContext{}
		r.cache[decoded.ForeignType] = mt
	}
	log.Infof("setting %d:%s [%d] to %v", decoded.ForeignType, decoded.ForeignId, decoded.CreatedAtNs, decoded)
	mt[decoded.ForeignId] = insertSort(mt[decoded.ForeignId], decoded)
	r.Unlock()
}

func (r *ContextCache) Lookup(t uint64, id string, from int64) (*spec.PersistedContext, bool) {
	r.RLock()
	defer r.RUnlock()
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
	log.Printf("scanning from %d", r.offset)
	n := 0
	err := r.forward.Scan(r.offset, true, func(offset uint64, foreignId uint64, foreignType uint64, data []byte) error {
		decoded := &spec.PersistedContext{}
		err := proto.Unmarshal(data, decoded)
		if err != nil {
			log.Warnf("rend failed to unmarshal, data: %s, error: %s", string(data), err.Error())
			return nil
		}
		r.Insert(decoded)
		r.offset = offset
		n++
		return nil
	})
	log.Printf("scanning finished at %d, got %d new entries", r.offset, n)
	return err
}

func toContextDeep(seen map[uint64]map[string]bool, contextCache *ContextCache, dictionary *disk.PersistedDictionary, p *spec.PersistedContext) []*spec.Context {
	out := []*spec.Context{toContext(dictionary, p)}
	for i := 0; i < len(p.PropertyKeys); i++ {
		k := p.PropertyKeys[i]
		v := p.PropertyValues[i]

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
		if px, ok := contextCache.Lookup(k, v, p.CreatedAtNs); ok {
			out = append(out, toContextDeep(seen, contextCache, dictionary, px)...)
		}
	}

	return out
}
func toContext(dictionary *disk.PersistedDictionary, p *spec.PersistedContext) *spec.Context {
	out := &spec.Context{
		CreatedAtNs: p.CreatedAtNs,
		ForeignId:   p.ForeignId,
		ForeignType: dictionary.ReverseResolve(p.ForeignType),
	}
	for i := 0; i < len(p.PropertyKeys); i++ {
		tk := dictionary.ReverseResolve(p.PropertyKeys[i])
		out.Properties = append(out.Properties, &spec.KV{Key: tk, Value: p.PropertyValues[i]})
	}
	sort.Sort(ByKV(out.Properties))
	return out
}

func LoadContextForStat(contextCache *ContextCache, dictionary *disk.PersistedDictionary, k, v string, t int64) []*spec.Context {
	seen := map[uint64]map[string]bool{}
	out := []*spec.Context{}
	key, ok := dictionary.Resolve(k)

	if !ok {
		return out
	}

	if px, ok := contextCache.Lookup(key, v, t); ok {
		out = append(out, toContextDeep(seen, contextCache, dictionary, px)...)
	}

	return out
}
