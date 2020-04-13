package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"testing"

	spec "github.com/rekki/blackrock/pkg/blackrock_io"
	"github.com/rekki/blackrock/pkg/index"
	"github.com/rekki/blackrock/pkg/logger"
	"github.com/rekki/go-query-index/go_query_dsl"
	iq "github.com/rekki/go-query-index/go_query_dsl"
	elastic "gopkg.in/olivere/elastic.v6"
)

var dontOptimizeMe = uint64(0)

type City struct {
	Name     string
	Expected int
}

var TINY = City{"vinkeveen", 3942}
var SMALL = City{"amstelveen", 50032}
var LARGE = City{"amsterdam", 495083}
var BR_ROOT = "/tmp/br/4"
var si *index.SearchIndex

func init() {
	logger.LogInit(3)
	si = index.NewSearchIndex(BR_ROOT, 0, 3600, true, map[string]bool{})
}

func ElasticScroll(query elastic.Query, expected int, b *testing.B) {
	b.StopTimer()
	es, err := elastic.NewClient()
	if err != nil {
		panic(err)
	}
	b.StartTimer()

	for i := 0; i < b.N; i++ {
		svc := es.Scroll(esIndex).Query(query).Size(expected)
		res, err := svc.Do(context.TODO())
		if err == io.EOF {
			break
		}
		if err != nil {
			panic(err)
		}
		dontOptimizeMe += uint64(len(res.Hits.Hits))
		if len(res.Hits.Hits) != expected {
			panic(fmt.Sprintf("should be %v, got: %v", expected, len(res.Hits.Hits)))
		}

		err = svc.Clear(context.TODO())
		if err != nil {
			panic(err)
		}
	}
}

func ElasticSearch(query elastic.Query, expected int, b *testing.B) {
v	b.StopTimer()
	es, err := elastic.NewClient()
	if err != nil {
		panic(err)
	}

	ctx := context.Background()
	b.StartTimer()

	for i := 0; i < b.N; i++ {
		searchResult, err := es.Search().
			Index(esIndex).
			Type(esType).
			Query(query).
			From(0).Size(1).
			Do(ctx)
		if err != nil {
			panic(err)
		}
		if int(searchResult.TotalHits()) != expected {
			panic(fmt.Sprintf("should be %v, got: %v", expected, searchResult.TotalHits()))
		}
	}
}

func BlackrockMemScroll(qr *spec.SearchQueryRequest, expected int, b *testing.B) {
	for i := 0; i < b.N; i++ {
		m := spec.Metadata{}
		cnt := 0

		err := si.ForEach(qr, uint32(qr.Limit), func(s *index.Segment, did int32, score float32) error {
			err := s.ReadForwardDecode(did, &m)
			if err != nil {
				panic(err)
			}

			dontOptimizeMe += uint64(m.CreatedAtNs)
			cnt++
			return nil

		})
		if err != nil {
			panic(err)
		}
		if cnt != expected {
			log.Panicf("should be at %d, got %d", expected, cnt)
		}
	}
}

func BlackrockRemoteScroll(qr *spec.SearchQueryRequest, expected int, b *testing.B) {
	b.StopTimer()
	ctx := context.Background()
	br := BlackrockRemoteOpen()
	b.StartTimer()

	for i := 0; i < b.N; i++ {
		fc, err := br.SayFetch(ctx, qr)
		if err != nil {
			panic(err)
		}
		cnt := 0
		for i := int32(0); i < qr.Limit; i++ {
			h, err := fc.Recv()
			if err == io.EOF {
				break
			}
			if err != nil {
				panic(err)
			}
			cnt++
			dontOptimizeMe += uint64(h.Metadata.CreatedAtNs)
		}
		if cnt != expected {
			log.Panicf("should be at %d, got %d", expected, cnt)
		}
	}
}

func BlackrockRemoteSearch(qr *spec.SearchQueryRequest, expected int, b *testing.B) {
	b.StopTimer()
	ctx := context.Background()
	br := BlackrockRemoteOpen()
	b.StartTimer()

	for i := 0; i < b.N; i++ {
		out, err := br.SaySearch(ctx, qr)
		if err != nil {
			panic(err)
		}
		if int(out.Total) != expected {
			log.Panicf("should be at %d, got %d", expected, out.Total)
		}

		dontOptimizeMe += out.Total
	}
}

func BlackrockMemSearch(qr *spec.SearchQueryRequest, expected int, b *testing.B) {
	for i := 0; i < b.N; i++ {
		out, err := BlackrockMemTopN(si, qr)
		if err != nil {
			panic(err)
		}
		if int(out.Total) != expected {
			log.Panicf("should be at %d, got %d", expected, out.Total)
		}

		dontOptimizeMe += out.Total
	}
}

func BenchmarkLargeBlackrockRemoteSearch(b *testing.B) {
	BlackrockRemoteSearch(&spec.SearchQueryRequest{Limit: 1, Query: &iq.Query{Field: "city", Value: LARGE.Name}}, LARGE.Expected, b)
}

func BenchmarkLargeBlackrockEmbeddedSearch(b *testing.B) {
	BlackrockMemSearch(&spec.SearchQueryRequest{Limit: 1, Query: &iq.Query{Field: "city", Value: LARGE.Name}}, LARGE.Expected, b)
}

func BenchmarkLargeElasticSearch(b *testing.B) {
	ElasticSearch(elastic.NewTermQuery("city", LARGE.Name), LARGE.Expected, b)
}

func BenchmarkSmallBlackrockRemoteSearch(b *testing.B) {
	BlackrockRemoteSearch(&spec.SearchQueryRequest{Limit: 1, Query: &iq.Query{Field: "city", Value: SMALL.Name}}, SMALL.Expected, b)
}

func BenchmarkSmallBlackrockEmbeddedSearch(b *testing.B) {
	BlackrockMemSearch(&spec.SearchQueryRequest{Limit: 1, Query: &iq.Query{Field: "city", Value: SMALL.Name}}, SMALL.Expected, b)
}

func BenchmarkSmallElasticSearch(b *testing.B) {
	ElasticSearch(elastic.NewTermQuery("city", SMALL.Name), SMALL.Expected, b)
}

func BenchmarkTinyBlackrockRemoteSearch(b *testing.B) {
	BlackrockRemoteSearch(&spec.SearchQueryRequest{Limit: 1, Query: &iq.Query{Field: "city", Value: TINY.Name}}, TINY.Expected, b)
}

func BenchmarkTinyBlackrockEmbeddedSearch(b *testing.B) {
	BlackrockMemSearch(&spec.SearchQueryRequest{Limit: 1, Query: &iq.Query{Field: "city", Value: TINY.Name}}, TINY.Expected, b)
}

func BenchmarkTinyElasticSearch(b *testing.B) {
	ElasticSearch(elastic.NewTermQuery("city", TINY.Name), TINY.Expected, b)
}

func BenchmarkLargeAndTinyBlackrockRemoteSearch(b *testing.B) {
	BlackrockRemoteSearch(&spec.SearchQueryRequest{
		Limit: 1,
		Query: &iq.Query{
			Type: iq.Query_AND, Queries: []*iq.Query{
				&iq.Query{Field: "city", Value: TINY.Name},
				&iq.Query{Field: "city", Value: LARGE.Name},
			},
		},
	}, 0, b)
}

func BenchmarkLargeAndTinyBlackrockRemoteEmbedded(b *testing.B) {
	BlackrockMemSearch(&spec.SearchQueryRequest{
		Limit: 1,
		Query: &iq.Query{
			Type: iq.Query_AND, Queries: []*iq.Query{
				&iq.Query{Field: "city", Value: TINY.Name},
				&iq.Query{Field: "city", Value: LARGE.Name},
			},
		},
	}, 0, b)
}

func BenchmarkLargeAndTinyElasticSearch(b *testing.B) {
	ElasticSearch(elastic.NewBoolQuery().Must(
		elastic.NewTermQuery("city", LARGE.Name),
		elastic.NewTermQuery("city", TINY.Name),
	), 0, b)
}

func BenchmarkSmallOrTinyBlackrockRemoteSearch(b *testing.B) {
	BlackrockRemoteSearch(&spec.SearchQueryRequest{
		Limit: 1,
		Query: &iq.Query{
			Type: iq.Query_OR, Queries: []*iq.Query{
				&iq.Query{Field: "city", Value: TINY.Name},
				&iq.Query{Field: "city", Value: SMALL.Name},
			},
		},
	}, TINY.Expected+SMALL.Expected, b)
}

func BenchmarkSmallOrTinyBlackrockEmbeddedSearch(b *testing.B) {
	BlackrockMemSearch(&spec.SearchQueryRequest{
		Limit: 1,
		Query: &iq.Query{
			Type: iq.Query_OR, Queries: []*iq.Query{
				&iq.Query{Field: "city", Value: TINY.Name},
				&iq.Query{Field: "city", Value: SMALL.Name},
			},
		},
	}, TINY.Expected+SMALL.Expected, b)
}

func BenchmarkSmallOrTinyElasticSearch(b *testing.B) {
	ElasticSearch(elastic.NewBoolQuery().Should(
		elastic.NewTermQuery("city", SMALL.Name),
		elastic.NewTermQuery("city", TINY.Name),
	), SMALL.Expected+TINY.Expected, b)
}

func BenchmarkScroll1BlackrockRemote(b *testing.B) {
	BlackrockRemoteScroll(&spec.SearchQueryRequest{Limit: 1, Query: &go_query_dsl.Query{Field: "city", Value: SMALL.Name}}, 1, b)
}
func BenchmarkScroll1BlackrockEmbedded(b *testing.B) {
	BlackrockMemScroll(&spec.SearchQueryRequest{Limit: 1, Query: &go_query_dsl.Query{Field: "city", Value: SMALL.Name}}, 1, b)
}

func BenchmarkScroll1Elastic(b *testing.B) {
	ElasticScroll(elastic.NewTermQuery("city", SMALL.Name), 1, b)
}

func BenchmarkScroll10000BlackrockRemote(b *testing.B) {
	BlackrockRemoteScroll(&spec.SearchQueryRequest{Limit: 10000, Query: &go_query_dsl.Query{Field: "city", Value: SMALL.Name}}, 10000, b)
}

func BenchmarkScroll10000BlackrockEmbedded(b *testing.B) {
	BlackrockMemScroll(&spec.SearchQueryRequest{Limit: 10000, Query: &go_query_dsl.Query{Field: "city", Value: SMALL.Name}}, 10000, b)
}

func BenchmarkScroll10000Elastic(b *testing.B) {
	ElasticScroll(elastic.NewTermQuery("city", SMALL.Name), 10000, b)
}
