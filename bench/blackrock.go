package main

import (
	"log"
	"os"

	spec "github.com/rekki/blackrock/pkg/blackrock_io"
	"github.com/rekki/blackrock/pkg/index"
	"google.golang.org/grpc"
)

func BlackrockRemoteOpen() spec.SearchClient {
	hp := os.Getenv("BLACKROCK")
	if hp == "" {
		hp = "localhost:8002"
	}
	conn, err := grpc.Dial(hp, grpc.WithInsecure())
	if err != nil {
		log.Fatal(err)
	}
	client := spec.NewSearchClient(conn)
	return client
}

func BlackrockLocation(l *Location) *spec.Envelope {
	return &spec.Envelope{
		Metadata: &spec.Metadata{
			EventType:   "osm",
			ForeignType: "node",
			ForeignId:   l.ID,
			Search: []spec.KV{
				spec.KV{Key: "city", Value: l.City},
				spec.KV{Key: "gh5", Value: l.GH5},
				spec.KV{Key: "gh6", Value: l.GH6},
				spec.KV{Key: "match_all", Value: l.MatchAll},
				spec.KV{Key: "gh7", Value: l.GH7},
			},
			Properties: []spec.KV{
				spec.KV{Key: "name", Value: l.City},
			},
		},
	}
}

func BlackrockMemTopN(si *index.SearchIndex, qr *spec.SearchQueryRequest) (*spec.SearchQueryResponse, error) {
	out := &spec.SearchQueryResponse{
		Total: 0,
	}

	scored := []spec.Hit{}
	err := si.ForEach(qr, 0, func(segment *index.Segment, did int32, score float32) error {
		out.Total++
		if qr.Limit == 0 {
			return nil
		}
		doInsert := false
		if len(scored) < int(qr.Limit) {
			doInsert = true
		} else if scored[len(scored)-1].Score < score {
			doInsert = true
		}

		if doInsert {
			hit := spec.Hit{Score: score}
			m := spec.Metadata{}
			err := segment.ReadForwardDecode(did, &m)
			if err != nil {
				// FIXME(jackdoe): should we skip here? or return partial result.
				return nil
			}
			hit.Id = m.Id
			if hit.Id == 0 {
				hit.Id = uint64(did) + 1
			}
			hit.Metadata = &m
			hit.Score = score
			if len(scored) < int(qr.Limit) {
				scored = append(scored, hit)
			}
			for i := 0; i < len(scored); i++ {
				if scored[i].Score < hit.Score {
					copy(scored[i+1:], scored[i:])
					scored[i] = hit
					break
				}
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	out.Hits = make([]*spec.Hit, len(scored))
	for i, v := range scored {
		out.Hits[i] = &v
	}

	return out, nil
}
