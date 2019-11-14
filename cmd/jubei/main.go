package main

import (
	"flag"
	"net/http"
	"os"
	"os/signal"
	"sync"
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
	var pminBytes = flag.Int("min-bytes", 10*1024*1024, "min bytes")
	var pmaxBytes = flag.Int("max-bytes", 20*1024*1024, "max bytes")
	var kafkaServers = flag.String("kafka", "localhost:9092", "comma separated list of kafka servers")
	var verbose = flag.Bool("verbose", false, "print info level logs to stdout")
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

	partitions, err := ReadPartitions(*kafkaServers, *dataTopic)
	if err != nil {
		log.Fatal(err)
	}

	brokers := strings.Split(*kafkaServers, ",")
	readers := []*PartitionReader{}
	for _, p := range partitions {
		rd := kafka.NewReader(kafka.ReaderConfig{
			Brokers:   brokers,
			MinBytes:  *pminBytes,
			MaxBytes:  *pmaxBytes,
			Topic:     *dataTopic,
			MaxWait:   1 * time.Second,
			Partition: p.ID,
		})
		readers = append(readers, &PartitionReader{rd, p})
	}

	giant := sync.Mutex{}
	build := func() {
		giant.Lock()
		defer giant.Unlock()

		l := log.WithField("root", root).WithField("mode", "BUILD")
		err := buildEverything(root, l)
		if err != nil {
			l.WithError(err).Fatal(err)
		}
	}

	sigs := make(chan os.Signal)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		err := consumeEventsFromAllPartitions(root, readers)
		if err != nil {
			log.Warnf("error consuming events: %s", err.Error())
		}
		sigs <- syscall.SIGTERM
	}()

	go func() {
		<-sigs

		// no need to close the files, as they are closed on exit
		log.Warnf("closing the readers...")
		for _, r := range readers {
			r.Reader.Close()
		}

		os.Exit(0)
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
