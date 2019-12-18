package main

import (
	"context"
	"flag"
	"io"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/gogo/protobuf/proto"
	"github.com/grpc-ecosystem/grpc-gateway/runtime"
	spec "github.com/rekki/blackrock/pkg/blackrock_io"
	"github.com/rekki/blackrock/pkg/depths"
	. "github.com/rekki/blackrock/pkg/logger"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/snappy"
	"google.golang.org/grpc"

	"strings"
	"time"
)

type server struct {
	kw           *kafka.Writer
	kafkaServers string
	topic        string
}

func (s *server) SayPush(stream spec.Enqueue_SayPushServer) error {
	ctx := context.Background()
	for {
		envelope, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		if envelope.Metadata.CreatedAtNs == 0 {
			envelope.Metadata.CreatedAtNs = time.Now().UnixNano()
		}

		encoded, err := proto.Marshal(envelope)
		if err != nil {
			return err
		}

		err = s.kw.WriteMessages(ctx, kafka.Message{
			Value: encoded,
		})

		if err != nil {
			return err
		}

	}
	return stream.SendAndClose(&spec.Success{Success: true})
}

func runProxy(bindHttp string, bindGrpc string) error {
	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	mux := runtime.NewServeMux()
	opts := []grpc.DialOption{grpc.WithInsecure()}
	err := spec.RegisterEnqueueHandlerFromEndpoint(ctx, mux, bindGrpc, opts)
	if err != nil {
		return err
	}

	return http.ListenAndServe(bindHttp, mux)
}

func (s *server) SayHealth(ctx context.Context, in *spec.HealthRequest) (*spec.Success, error) {
	err := depths.HealthCheckKafka(s.kafkaServers, s.topic)
	if err != nil {
		return nil, err
	}

	return &spec.Success{Success: true}, nil
}

func main() {
	var dataTopic = flag.String("topic-data", "blackrock-data", "topic for the data")
	var kafkaServers = flag.String("kafka", "localhost:9092", "comma separated list of kafka servers")
	var statSleep = flag.Int("writer-stats", 60, "print writer stats every # seconds")
	var logLevel = flag.Int("log-level", 0, "log level")
	var bindHttp = flag.String("http", ":9001", "bind http")
	var bindGrpc = flag.String("grpc", ":8001", "bind grpc")
	flag.Parse()

	LogInit(*logLevel)

	err := depths.HealthCheckKafka(*kafkaServers, *dataTopic)
	if err != nil {
		Log.Fatal(err.Error())
	}

	kw := kafka.NewWriter(kafka.WriterConfig{
		Brokers:          strings.Split(*kafkaServers, ","),
		Topic:            *dataTopic,
		Balancer:         &kafka.LeastBytes{},
		BatchTimeout:     1 * time.Second,
		CompressionCodec: snappy.NewCompressionCodec(),
		Async:            true,
	})
	defer kw.Close()

	srv := &server{
		kafkaServers: *kafkaServers,
		kw:           kw,
		topic:        *dataTopic,
	}

	go func() {
		for {
			s := kw.Stats()
			Log.Infof("%s\n", depths.DumpObj(s))

			time.Sleep(time.Duration(*statSleep) * time.Second)
		}
	}()

	lis, err := net.Listen("tcp", *bindGrpc)
	if err != nil {
		Log.Fatalf("failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer(AddLogging([]grpc.ServerOption{})...)
	spec.RegisterEnqueueServer(grpcServer, srv)

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigs
		Log.Warnf("closing the writer...")
		kw.Close()
		os.Exit(0)
	}()

	go func() {
		err := runProxy(*bindHttp, *bindGrpc)
		if err != nil {
			Log.Warnf("failed to run the proxy, err: %s", err.Error())
			kw.Close()
			os.Exit(0)
		}
	}()

	err = grpcServer.Serve(lis)
	Log.Fatal(err)
}
