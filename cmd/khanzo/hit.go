package main

import (
	"fmt"
	"strings"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/rekki/blackrock/cmd/jubei/disk"
	"github.com/rekki/blackrock/cmd/orgrim/spec"
)

type Hit struct {
	Score    float32       `json:"score,omitempty"`
	ID       uint64        `json:"id,omitempty"`
	Metadata spec.Metadata `json:"metadata,omitempty"`
}

func (h Hit) String() string {
	out := []string{}
	m := h.Metadata
	t := time.Unix(int64(m.CreatedAtNs)/1000000000, 0)
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

	return strings.Join(out, "\n") + "\n\n"
}

func getScoredHit(forward *disk.ForwardWriter, did int32) (Hit, error) {
	data, _, err := forward.Read(uint32(did))
	if err != nil {
		return Hit{}, err
	}
	p := spec.Metadata{}
	err = proto.Unmarshal(data, &p)
	if err != nil {
		return Hit{}, err
	}
	return toHit(did, &p), nil
}

func toHit(did int32, p *spec.Metadata) Hit {
	id := p.Id
	if id == 0 {
		id = uint64(did) + 1
	}
	hit := Hit{
		ID:       id,
		Metadata: *p,
	}

	return hit
}
