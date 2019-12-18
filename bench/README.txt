*) get the grpc branch

$ go get github.com/rekki/blackrock@grpc 

*) download Amsterdam.osm.pbf in the current directory:

$ wget https://download.bbbike.org/osm/bbbike/Amsterdam/Amsterdam.osm.pbf

*) build and run blackrock-search

$ cd ../cmd/search && go build && ./search -root /tmp/br/4 -enable-segment-cache -log-level 3
# NB: /tmp/br/4 is used from the benchmark to load in-memory blackrock

*) run elastic search

$ sudo docker run -ti -e 'network.publish_host=127.0.0.1' -e "ES_JAVA_OPTS=-Xms8g -Xmx8g" -p 9200:9200 -p 9300:9300 -e "discovery.type=single-node" docker.elastic.co/elasticsearch/elasticsearch:6.8.1
# 6.8.1 is because this is the AWS version

*) build the indexes

$ go build && ./bench
2019/12/24 12:56:46 Blackrock took 13.47639164s to index 1221890 documents
2019/12/24 12:57:55 ElasticSearch took 1m8.717758482s to index 1221890 documents

*) run the benchmark

$ go test -bench=.

goos: linux
goarch: amd64
pkg: blackrock/bench
BenchmarkLargeBlackrockRemoteSearch-8                        242           4919705 ns/op
BenchmarkLargeBlackrockEmbeddedSearch-8                      260           4511735 ns/op
BenchmarkLargeElasticSearch-8                                172           6478708 ns/op
BenchmarkSmallBlackrockRemoteSearch-8                       1743            671497 ns/op
BenchmarkSmallBlackrockEmbeddedSearch-8                     2349            507337 ns/op
BenchmarkSmallElasticSearch-8                                543           1978671 ns/op
BenchmarkTinyBlackrockRemoteSearch-8                        5498            206817 ns/op
BenchmarkTinyBlackrockEmbeddedSearch-8                     22053             56483 ns/op
BenchmarkTinyElasticSearch-8                                 922           1237463 ns/op
BenchmarkLargeAndTinyBlackrockRemoteSearch-8                2366            453118 ns/op
BenchmarkLargeAndTinyBlackrockRemoteEmbedded-8              3718            322691 ns/op
BenchmarkLargeAndTinyElasticSearch-8                        1147           1047462 ns/op
BenchmarkSmallOrTinyBlackrockRemoteSearch-8                  694           1664572 ns/op
BenchmarkSmallOrTinyBlackrockEmbeddedSearch-8                818           1516337 ns/op
BenchmarkSmallOrTinyElasticSearch-8                          685           1702238 ns/op
BenchmarkScroll1BlackrockRemote-8                           6861            187523 ns/op
BenchmarkScroll1BlackrockEmbedded-8                        67840             18385 ns/op
BenchmarkScroll1Elastic-8                                    871           1179559 ns/op
BenchmarkScroll10000BlackrockRemote-8                         24          53268066 ns/op
BenchmarkScroll10000BlackrockEmbedded-8                       48          22571960 ns/op
BenchmarkScroll10000Elastic-8                                  6         167438003 ns/op
PASS
ok      blackrock/bench 42.470s
it is testing *1* shard in ES vs *1* goroutine in blackrock

if you see [FORBIDDEN/12/index read-only / allow delete (api)] from
ElasticSearch it means it made indexes read only because you dont habe
enough space. use:


$ curl -X PUT "localhost:9200/_cluster/settings" -H 'Content-Type: application/json' -d'
{
  "transient": {
    "cluster.routing.allocation.disk.watermark.low": "30mb",
    "cluster.routing.allocation.disk.watermark.high": "20mb",
    "cluster.routing.allocation.disk.watermark.flood_stage": "10mb",
    "cluster.info.update.interval": "1m"
  }
}'



If you see unexpected documents matching it means that the Amsterdam file was updated,
modify blackrock_test.go accordingly, the numbers at the point of this push are:

var TINY = City{"vinkeveen", 3942}
var SMALL = City{"amstelveen", 50032}
var LARGE = City{"amsterdam", 495083}

