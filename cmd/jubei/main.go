package main

import (
	"flag"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	_ "net/http/pprof"
	"strings"

	"github.com/segmentio/kafka-go"
	_ "github.com/segmentio/kafka-go/snappy"
	log "github.com/sirupsen/logrus"
)

func main() {
	var dataTopic = flag.String("topic-data", "blackrock-data", "topic for the data")
	var proot = flag.String("root", "/blackrock", "root directory for the files")
	var kafkaServers = flag.String("kafka", "localhost:9092", "comma separated list of kafka servers")
	var verbose = flag.Bool("verbose", false, "print info level logs to stdout")
	var compact = flag.Bool("compact", false, "compact everything until today and exit")
	flag.Parse()

	go func() {
		log.Println(http.ListenAndServe("localhost:6061", nil))
	}()

	root := *proot

	if err := os.MkdirAll(root, 0700); err != nil {
		log.Fatal(err)
	}

	if *verbose {
		log.SetLevel(log.InfoLevel)
	} else {
		log.SetLevel(log.WarnLevel)
	}

	if *compact {
		err := compactEverything(root)
		t0 := time.Now()
		if err != nil {
			log.Fatal(err)
		}
		log.Warnf("COMPACT: successfully compacted everything in %s, took %s", root, time.Since(t0))
		os.Exit(0)
	}

	partitions, err := ReadPartitions(*kafkaServers, *dataTopic)
	if err != nil {
		log.Fatal(err)
	}

	brokers := strings.Split(*kafkaServers, ",")
	readers := []*PartitionReader{}
	for _, p := range partitions {
		rd := kafka.NewReader(kafka.ReaderConfig{
			Brokers:   brokers,
			MinBytes:  100 * 1024 * 1024,
			MaxBytes:  200 * 1024 * 1024,
			Topic:     *dataTopic,
			MaxWait:   1 * time.Second,
			Partition: p.ID,
		})
		readers = append(readers, &PartitionReader{rd, p})
	}

	build := func() {
		l := log.WithField("root", root).WithField("mode", "BUILD")
		t0 := time.Now()
		err := buildEverything(root, l)
		if err != nil {
			l.WithError(err).Fatal(err)
		}
		l.Warnf("successfully built everything took %s", time.Since(t0))
	}

	sigs := make(chan os.Signal, 100)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	cleanup := func() {
		// no need to close the files, as they are closed on exit
		log.Warnf("closing the readers...")
		for _, r := range readers {
			r.Reader.Close()
		}

		build()
		os.Exit(0)
	}

	go func() {
		<-sigs

		cleanup()
	}()

	go func() {
		err := consumeEventsFromAllPartitions(root, readers)
		if err != nil {
			log.Warnf("error consuming events: %s", err.Error())
		}
		sigs <- syscall.SIGTERM
	}()

	for {
		build()

		for _, rd := range readers {
			s := rd.Reader.Stats()
			if s.Lag > 0 {
				log.WithField("mode", "CONSUME").Warnf("partition: %d, lag: %d, messages: %d\n", rd.Partition.ID, s.Lag, s.Messages)
			}
		}

		time.Sleep(1 * time.Second)
	}
}
