package main

import (
	"fmt"
	"strings"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/rekki/blackrock/cmd/jubei/disk"
	"github.com/rekki/blackrock/cmd/orgrim/spec"
	"github.com/rekki/blackrock/pkg/depths"
)

func expandTimeToQuery(dates []time.Time, makeTermQuery func(string, string) Query) Query {
	out := []Query{}
	for _, start := range dates {
		out = append(out, makeTermQuery("year-month-day", yyyymmdd(start)))
	}

	return NewBoolOrQuery(out...)
}

func expandYYYYMMDD(from string, to string) []time.Time {
	fromTime := time.Now().UTC().AddDate(0, 0, -3)
	toTime := time.Now().UTC()

	if from != "" {
		d, err := time.Parse("2006-01-02", from)
		if err == nil {
			fromTime = d
		}
	}
	if to != "" {
		d, err := time.Parse("2006-01-02", to)
		if err == nil {
			toTime = d
		}
	}
	dateQuery := []time.Time{}
	start := fromTime.AddDate(0, 0, 0)
	for {
		dateQuery = append(dateQuery, start)
		start = start.AddDate(0, 0, 1)
		if start.Sub(toTime) > 0 {
			break
		}
	}
	return dateQuery
}

func fromQuery(input *spec.Query, makeTermQuery func(string, string) Query) (Query, error) {
	if input.Type == spec.Query_TERM {
		if input.Not != nil || input.Delta != 0 || len(input.Sub) != 0 {
			return nil, fmt.Errorf("term queries can have only tag and value, %v", input)
		}
		if input.Tag == "" {
			return nil, fmt.Errorf("missing tag, %v", input)
		}
		return makeTermQuery(input.Tag, input.Value), nil
	}
	queries := []Query{}
	for _, q := range input.Sub {
		p, err := fromQuery(q, makeTermQuery)
		if err != nil {
			return nil, err
		}
		queries = append(queries, p)
	}

	if input.Type == spec.Query_AND {
		and := NewBoolAndQuery(queries...)
		if input.Not != nil {
			p, err := fromQuery(input.Not, makeTermQuery)
			if err != nil {
				return nil, err
			}
			and.SetNot(p)
		}
		return and, nil
	}

	if input.Type == spec.Query_AND {
		if input.Not != nil || input.Delta != 0 {
			return nil, fmt.Errorf("or queries cant have 'not' or 'delta' value, %v", input)
		}

		return NewBoolOrQuery(queries...), nil
	}

	if input.Type == spec.Query_AND_THEN {
		if input.Not != nil || len(input.Sub) != 2 {
			return nil, fmt.Errorf("and-then queries must not have 'not' clause and exactly 2 'sub' queries, %v", input)
		}

		return NewAndThenQuery(queries[0], queries[1], input.Delta), nil
	}
	return nil, fmt.Errorf("unknown type %v", input)
}

func NewTermQuery(root string, tagKey string, tagValue string, c *disk.CompactIndexCache) Query {
	tagKey = depths.Cleanup(strings.ToLower(tagKey))
	tagValue = depths.Cleanup(strings.ToLower(tagValue))
	s := fmt.Sprintf("%s:%s", tagKey, tagValue)
	return NewTerm(s, c.FindPostingsList(root, tagKey, tagValue))
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
