package main

import (
	"context"
	"log"
	"time"

	"github.com/rekki/blackrock/pkg/depths"
	elastic "gopkg.in/olivere/elastic.v6"
)

func ElasticSearchCreateIndex(ctx context.Context, es *elastic.Client) {
	_, _ = es.DeleteIndex(esIndex).Do(ctx)
	_, err := es.CreateIndex(esIndex).BodyString(esMapping).Do(ctx)
	if err != nil {
		panic(err)
	}

	if err != nil {
		panic(err)
	}
}

func IndexElastic() {
	ctx := context.Background()
	var err error
	es, err := elastic.NewClient()
	if err != nil {
		panic(err)
	}

	ElasticSearchCreateIndex(ctx, es)
	bulk := es.Bulk().Index(esIndex)
	t0 := time.Now()
	cnt := 0
	OSMDecoder(func(l *Location) {
		id := l.ID
		cnt++
		bulk.Add(elastic.NewBulkIndexRequest().Type(esType).Id(id).Doc(l))
		if bulk.NumberOfActions() >= 1024 {
			res, err := bulk.Do(ctx)
			if err != nil {
				panic(err)

			}
			if res.Errors {
				panic(depths.DumpObj(res.Items))
			}
		}
	})

	if bulk.NumberOfActions() > 0 {
		_, err := bulk.Do(ctx)
		if err != nil {
			panic(err)
		}
	}

	took := time.Since(t0)

	log.Printf("ElasticSearch took %v to index %v documents", took, cnt)
}

func IndexBlackrock() {
	br := BlackrockRemoteOpen()
	var cnt = 0
	ctx := context.Background()
	t0 := time.Now()
	stream, err := br.SayPush(ctx)
	if err != nil {
		panic(err)
	}

	OSMDecoder(func(l *Location) {
		err := stream.Send(BlackrockLocation(l))
		if err != nil {
			panic(err)
		}

		cnt++
	})
	_, err = stream.CloseAndRecv()
	if err != nil {
		panic(err)
	}

	took := time.Since(t0)

	log.Printf("Blackrock took %v to index %v documents", took, cnt)
}

func main() {
	IndexBlackrock()
	IndexElastic()
}
