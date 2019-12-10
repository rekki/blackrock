package main

import (
	"fmt"

	spec "github.com/rekki/blackrock/cmd/orgrim/blackrock_io"
	iq "github.com/rekki/go-query"
)

func fromQuery(input *spec.Query, makeTermQuery func(string, string) iq.Query) (iq.Query, error) {
	if input == nil {
		return nil, fmt.Errorf("nil input")
	}
	if input.Type == spec.Query_TERM {
		if input.Not != nil || len(input.Queries) != 0 {
			return nil, fmt.Errorf("term queries can have only tag and value, %v", input)
		}
		if input.Key == "" {
			return nil, fmt.Errorf("missing tag, %v", input)
		}
		t := makeTermQuery(input.Key, input.Value)
		if input.Boost > 0 {
			t.SetBoost(input.Boost)
		}
		return t, nil
	}
	queries := []iq.Query{}
	for _, q := range input.Queries {
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

		} else {
			if len(queries) == 1 {
				return queries[0], nil
			}
		}
		if input.Boost > 0 {
			and.SetBoost(input.Boost)
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
		or := iq.Or(queries...)
		if input.Boost > 0 {
			or.SetBoost(input.Boost)
		}

		return or, nil
	}

	if input.Type == spec.Query_DISMAX {
		if input.Not != nil {
			return nil, fmt.Errorf("or queries cant have 'not' value, %v", input)
		}
		if len(queries) == 1 {
			return queries[0], nil
		}

		d := iq.DisMax(input.Tiebreaker, queries...)
		if input.Boost > 0 {
			d.SetBoost(input.Boost)
		}

		return d, nil
	}

	return nil, fmt.Errorf("unknown type %v", input)
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
