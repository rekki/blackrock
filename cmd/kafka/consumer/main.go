package main

import (
	"flag"
	"os"

	spec "github.com/rekki/blackrock/pkg/blackrock_io"
	. "github.com/rekki/blackrock/pkg/logger"
	_ "github.com/segmentio/kafka-go/snappy"

	"google.golang.org/grpc"
)

func main() {
	var remote = flag.String("search-grpc", ":8002", "connect to search grpc")
	var dataTopic = flag.String("topic-data", "blackrock-data", "topic for the data")
	var root = flag.String("root", "/blackrock", "root where to store the kafka offsets and locks")
	var kafkaServers = flag.String("kafka", "localhost:9092", "comma separated list of kafka servers")
	var logLevel = flag.Int("log-level", 0, "log level")
	flag.Parse()
	LogInit(*logLevel)

	err := os.MkdirAll(*root, 0700)
	if err != nil {
		Log.Fatal(err)
	}

	conn, err := grpc.Dial(*remote, grpc.WithInsecure())
	if err != nil {
		Log.Fatal(err)
	}
	defer conn.Close()

	si := spec.NewSearchClient(conn)

	err = consumeKafka(si, *root, *dataTopic, *kafkaServers)
	if err != nil {
		conn.Close()
		Log.Fatalf("failed to run the proxy, err: %s", err.Error())
	}
}
