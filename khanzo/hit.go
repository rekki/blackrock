package main

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/jackdoe/blackrock/jubei/disk"
	"github.com/jackdoe/blackrock/orgrim/spec"
)

type KafkaOffset struct {
	Offset    uint64 `json:"offset,omitempty"`
	Partition uint32 `json:"partition,omitempty"`
}

type Hit struct {
	Score       float32         `json:"score,omitempty"`
	ID          int64           `json:"id,omitempty"`
	ForeignId   string          `json:"foreign_id,omitempty"`
	ForeignType string          `json:"foreign_type,omitempty"`
	Metadata    *spec.Metadata  `json:"metadata,omitempty"`
	KafkaOffset *KafkaOffset    `json:"kafka,omitempty"`
	Context     []*spec.Context `json:"context,omitempty"`
}

func (h Hit) String() string {
	out := []string{}
	m := h.Metadata
	t := time.Unix(m.CreatedAtNs/1000000000, 0)
	out = append(out, fmt.Sprintf("%s:%s\ntype:%s\n%s", m.ForeignType, m.ForeignId, m.EventType, t.Format(time.UnixDate)))
	for _, kv := range m.Search {
		out = append(out, fmt.Sprintf("  %-30s: %s", kv.Key, kv.Value))
	}

	for _, kv := range m.Count {
		out = append(out, fmt.Sprintf("  %-30s: %s", kv.Key, kv.Value))
	}

	for _, kv := range m.Properties {
		out = append(out, fmt.Sprintf("  %-30s: %s", kv.Key, kv.Value))
	}

	for _, ctx := range h.Context {
		out = append(out, fmt.Sprintf("  %-30s:", fmt.Sprintf("@%s:%s", ctx.ForeignType, ctx.ForeignId)))
		for _, kv := range ctx.Properties {
			out = append(out, fmt.Sprintf("    %-28s: %s", kv.Key, kv.Value))
		}
	}

	return strings.Join(out, "\n") + "\n\n"
}

func getScoredHit(contextCache *ContextCache, forward *disk.ForwardWriter, dictionary *disk.PersistedDictionary, did int64, decodeMetadata bool) (Hit, error) {
	foreignId, foreignType, data, _, err := forward.Read(uint64(did), decodeMetadata)
	if err != nil {
		return Hit{}, err
	}

	if decodeMetadata {
		var p spec.PersistedMetadata
		err := proto.Unmarshal(data, &p)
		if err != nil {
			return Hit{}, err
		}

		return toHit(contextCache, dictionary, did, foreignId, foreignType, &p), nil
	}
	return toHit(contextCache, dictionary, did, foreignId, foreignType, nil), nil
}

func toHit(contextCache *ContextCache, dictionary *disk.PersistedDictionary, did int64, foreignId, foreignType uint64, p *spec.PersistedMetadata) Hit {
	hit := Hit{
		ID: did,
	}
	hit.ForeignId = dictionary.ReverseResolve(foreignId)
	hit.ForeignType = dictionary.ReverseResolve(foreignType)
	if p == nil {
		return hit
	}
	pretty := &spec.Metadata{
		ForeignId:   hit.ForeignId,
		ForeignType: hit.ForeignType,
		EventType:   dictionary.ReverseResolve(p.EventType),
		Search:      []*spec.KV{},
		Count:       []*spec.KV{},
		Properties:  []*spec.KV{},
		CreatedAtNs: p.CreatedAtNs,
	}
	hit.KafkaOffset = &KafkaOffset{Offset: p.Offset, Partition: p.Partition}
	seen := map[uint64]map[string]bool{}
	for i := 0; i < len(p.SearchKeys); i++ {
		k := p.SearchKeys[i]
		v := p.SearchValues[i]
		tk := dictionary.ReverseResolve(k)

		pretty.Search = append(pretty.Search, &spec.KV{Key: tk, Value: v})

		if px, ok := contextCache.Lookup(k, v, p.CreatedAtNs); ok {
			hit.Context = append(hit.Context, toContextDeep(seen, contextCache, dictionary, px)...)
		}
	}

	for i := 0; i < len(p.CountKeys); i++ {
		tk := dictionary.ReverseResolve(p.CountKeys[i])
		pretty.Count = append(pretty.Count, &spec.KV{Key: tk, Value: p.CountValues[i]})
	}

	for i := 0; i < len(p.PropertyKeys); i++ {
		tk := dictionary.ReverseResolve(p.PropertyKeys[i])
		pretty.Properties = append(pretty.Properties, &spec.KV{Key: tk, Value: p.PropertyValues[i]})
	}

	sort.Sort(ByCtxName(hit.Context))
	sort.Sort(ByKV(pretty.Search))
	sort.Sort(ByKV(pretty.Count))
	sort.Sort(ByKV(pretty.Properties))

	hit.Metadata = pretty
	return hit
}

type ByCtxName []*spec.Context

func (a ByCtxName) Len() int      { return len(a) }
func (a ByCtxName) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a ByCtxName) Less(i, j int) bool {
	if a[i].ForeignType != a[j].ForeignType {
		return a[i].ForeignType < a[j].ForeignType
	}
	return a[i].ForeignId < a[j].ForeignId
}
