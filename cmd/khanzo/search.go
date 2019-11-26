package main

import (
	"fmt"

	"github.com/gogo/protobuf/proto"
	iq "github.com/jackdoe/go-query"
	"github.com/rekki/blackrock/cmd/jubei/disk"
	"github.com/rekki/blackrock/cmd/orgrim/spec"
)

func fromQuery(input *spec.Query, makeTermQuery func(string, string) iq.Query) (iq.Query, error) {
	if input == nil {
		return nil, fmt.Errorf("nil input")
	}
	if input.Type == spec.Query_TERM {
		if input.Not != nil || len(input.Sub) != 0 {
			return nil, fmt.Errorf("term queries can have only tag and value, %v", input)
		}
		if input.Tag == "" {
			return nil, fmt.Errorf("missing tag, %v", input)
		}
		return makeTermQuery(input.Tag, input.Value), nil
	}
	queries := []iq.Query{}
	for _, q := range input.Sub {
		p, err := fromQuery(q, makeTermQuery)
		if err != nil {
			return nil, err
		}
		queries = append(queries, p)
	}

	if input.Type == spec.Query_AND {
		and := iq.And(queries...)
		if input.Not != nil {
			p, err := fromQuery(input.Not, makeTermQuery)
			if err != nil {
				return nil, err
			}
			and.SetNot(p)
			v
		} else {
			if len(queries) == 1 {
				return queries[0], nil
			}
		}

		return and, nil
	}

	if input.Type == spec.Query_OR {
		if input.Not != nil {
			return nil, fmt.Errorf("or queries cant have 'not' value, %v", input)
		}
		if len(queries) == 1 {
			return queries[0], nil
		}

		return iq.Or(queries...), nil
	}

	return nil, fmt.Errorf("unknown type %v", input)
}

func fetchFromForwardIndex(forward *disk.ForwardWriter, did int32) (*spec.Metadata, error) {
	data, _, err := forward.Read(uint32(did))
	if err != nil {
		return nil, err
	}
	p := spec.Metadata{}
	err = proto.Unmarshal(data, &p)
	if err != nil {
		return nil, err
	}

	return &p, nil
}

func toHit(did int32, p *spec.Metadata) *spec.Hit {
	id := p.Id
	if id == 0 {
		id = uint64(did) + 1
	}
	hit := &spec.Hit{
		Id:       id,
		Metadata: p,
	}

	return hit
}
