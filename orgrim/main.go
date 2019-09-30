package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
	"github.com/gogo/protobuf/proto"
	ginprometheus "github.com/mcuadros/go-gin-prometheus"
	"github.com/oschwald/geoip2-golang"
	"github.com/rekki/blackrock/depths"
	"github.com/rekki/blackrock/orgrim/spec"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/snappy"
	log "github.com/sirupsen/logrus"

	"strings"
	"time"
)

func main() {
	var dataTopic = flag.String("topic-data", "blackrock-data", "topic for the data")
	var contextTopic = flag.String("topic-context", "blackrock-context", "topic for the context")
	var kafkaServers = flag.String("kafka", "localhost:9092", "kafka addr")
	var createConfig = flag.String("create-if-not-exist", "", "create topics if they dont exist, format: partitions:replication factor")
	var verbose = flag.Bool("verbose", false, "print info level logs to stdout")
	var allowOnlyIdentified = flag.Bool("allow-only-identified", false, "allow only requests with valid token")
	var statSleep = flag.Int("writer-stats", 60, "print writer stats every # seconds")
	var geoipFile = flag.String("geoip", "", "path to https://dev.maxmind.com/geoip/geoip2/geolite2/ file")
	var tokentoproductmap = flag.String("token-to-product", "", "csv token to product e.g.: xyz:bookshop,abc:mobile_app if you send token xyz (as auth bearer header) the product will be set to bookshop")
	var tokentocontext = flag.String("token-to-context", "", "which token is allowed to push which context type")
	var bind = flag.String("bind", ":9001", "bind to")
	var prometheusListenAddress = flag.String("prometheus", "false", "true to enable prometheus (you can also specify a listener address")
	flag.Parse()

	if *verbose {
		log.SetLevel(log.InfoLevel)
	} else {
		gin.SetMode(gin.ReleaseMode)
		log.SetLevel(log.WarnLevel)
	}

	tp := spec.NewTokenMap(*tokentoproductmap, *tokentocontext)

	var geoip *geoip2.Reader
	var err error
	if *geoipFile != "" {
		geoip, err = geoip2.Open(*geoipFile)
		if err != nil {
			log.Fatal(err)
		}
		defer geoip.Close()
	}

	if *createConfig != "" {
		splitted := strings.Split(*createConfig, ":")
		if len(splitted) != 2 {
			log.Fatalf("expected format digit:digit (2:0 for example), got: '%s'", *createConfig)
		}
		partitions, err := strconv.ParseInt(splitted[0], 10, 32)
		if err != nil {
			log.Fatalf("partitions is not a number, err: %s", err.Error())
		}
		replicas, err := strconv.ParseInt(splitted[1], 10, 32)
		if err != nil {
			log.Fatalf("replicas is not a number, err: %s", err.Error())
		}

		err = depths.CreateTopic(*kafkaServers, *dataTopic, int(partitions), int(replicas))
		if err != nil {
			log.Fatalf("error creating %s, err: %s", *dataTopic, err.Error())
		}
		err = depths.CreateTopic(*kafkaServers, *contextTopic, int(partitions), int(replicas))
		if err != nil {
			log.Fatalf("error creating %s, err: %s", *contextTopic, err.Error())
		}

	}

	err = depths.HealthCheckKafka(*kafkaServers, *dataTopic)
	if err != nil {
		log.Fatal(err)
	}

	err = depths.HealthCheckKafka(*kafkaServers, *contextTopic)
	if err != nil {
		log.Fatal(err)
	}

	brokers := strings.Split(*kafkaServers, ",")
	kw := kafka.NewWriter(kafka.WriterConfig{
		Brokers:          brokers,
		Topic:            *dataTopic,
		Balancer:         &kafka.LeastBytes{},
		BatchTimeout:     1 * time.Second,
		CompressionCodec: snappy.NewCompressionCodec(),
		Async:            true,
	})
	defer kw.Close()

	cw := kafka.NewWriter(kafka.WriterConfig{
		Brokers:          brokers,
		Topic:            *contextTopic,
		Balancer:         &kafka.LeastBytes{},
		BatchTimeout:     1 * time.Second,
		CompressionCodec: snappy.NewCompressionCodec(),
		Async:            true,
	})
	defer cw.Close()

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigs
		log.Warnf("closing the writer...")
		kw.Close()
		cw.Close()
		os.Exit(0)
	}()

	go func() {
		for {
			s := kw.Stats()
			fmt.Printf("%s\n", depths.DumpObj(s))

			s = cw.Stats()
			fmt.Printf("%s\n", depths.DumpObj(s))

			time.Sleep(time.Duration(*statSleep) * time.Second)
		}
	}()

	r := gin.Default()

	if listenerAddress := *prometheusListenAddress; len(listenerAddress) > 0 && listenerAddress != "false" {
		prometheus := ginprometheus.NewPrometheus("blackrock_khanzo")
		prometheus.ReqCntURLLabelMappingFn = func(c *gin.Context) string {
			url := c.Request.URL.Path
			url = strings.Replace(url, "//", "/", -1)
			return url
		}
		if listenerAddress != "true" {
			prometheus.SetListenAddress(listenerAddress)
		}
		prometheus.Use(r)
	}

	r.Use(gin.Recovery())
	r.Use(cors.Default())

	r.GET("/health", func(c *gin.Context) {
		err := depths.HealthCheckKafka(*kafkaServers, *dataTopic)
		if err != nil {
			c.String(400, "BAD")
			return
		}
		c.String(200, "OK")
	})

	r.GET("/png/:event_type/:foreign_type/:foreign_id/*extra", func(c *gin.Context) {
		envelope := &spec.Envelope{
			Metadata: &spec.Metadata{
				CreatedAtNs: time.Now().UnixNano(),
				EventType:   c.Param("event_type"),
				ForeignType: c.Param("foreign_type"),
				ForeignId:   c.Param("foreign_id"),
			},
		}

		extra := c.Param("extra")
		splitted := strings.Split(extra, "/")
		token := c.Query("token")
		for _, s := range splitted {
			if s == "" {
				continue
			}
			kv := strings.Split(s, ":")
			if len(kv) != 2 || kv[0] == "" || kv[1] == "" {
				continue
			}
			envelope.Metadata.Search = append(envelope.Metadata.Search, spec.KV{Key: kv[0], Value: kv[1]})
		}
		err = spec.Decorate(geoip, c.Request, envelope)
		if err != nil {
			log.Warnf("[orgrim] failed to decorate, err: %s", err.Error())
		}
		err = spec.ValidateEnvelope(envelope)
		if err != nil {
			log.Warnf("[orgrim] invalid input, err: %s", err.Error())
		} else {
			at := tp.ExtractFromToken(token)
			if !at.AllowEnvelope(envelope) && *allowOnlyIdentified {
				log.Warnf("[orgrim] invalid token, err: %s", err.Error())
			} else {

				encoded, err := proto.Marshal(envelope)
				if err != nil {
					log.Warnf("[orgrim] error encoding metadata %v, err: %s", envelope.Metadata, err.Error())
				} else {
					err = kw.WriteMessages(context.Background(), kafka.Message{
						Value: encoded,
					})
					if err != nil {
						log.Warnf("[orgrim] error sending message, metadata %v, err: %s", envelope.Metadata, err.Error())
					}
				}
			}
		}

		c.Header("Cache-Control", "no-cache, no-store, must-revalidate")
		c.Header("Expires", "0")
		c.Header("Pragma", "no-cache")
		c.Data(200, "image/png", []byte{137, 80, 78, 71, 13, 10, 26, 10, 0, 0, 0, 13, 73, 72, 68, 82, 0, 0, 0, 1, 0, 0, 0, 1, 8, 6, 0, 0, 0, 31, 21, 196, 137, 0, 0, 0, 9, 112, 72, 89, 115, 0, 0, 11, 19, 0, 0, 11, 19, 1, 0, 154, 156, 24, 0, 0, 0, 1, 115, 82, 71, 66, 0, 174, 206, 28, 233, 0, 0, 0, 4, 103, 65, 77, 65, 0, 0, 177, 143, 11, 252, 97, 5, 0, 0, 0, 16, 73, 68, 65, 84, 120, 1, 1, 5, 0, 250, 255, 0, 0, 0, 0, 0, 0, 5, 0, 1, 100, 120, 149, 56, 0, 0, 0, 0, 73, 69, 78, 68, 174, 66, 96, 130})
	})

	r.POST("/push/envelope", func(c *gin.Context) {
		var envelope spec.Envelope
		err := depths.UnmarshalAndClose(c, &envelope)
		if err != nil {
			log.Warnf("[orgrim] error decoding envelope, err: %s", err.Error())
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		err = spec.ValidateEnvelope(&envelope)
		if err != nil {
			log.Warnf("[orgrim] invalid input, err: %s", err.Error())
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		if envelope.Metadata.CreatedAtNs == 0 {
			envelope.Metadata.CreatedAtNs = time.Now().UnixNano()
		}

		at := tp.ExtractFromRequest(c.Request)
		if !at.AllowEnvelope(&envelope) && *allowOnlyIdentified {
			log.Warnf("[orgrim] product not allowed for this token")
			c.JSON(http.StatusForbidden, gin.H{"error": "invalid token"})
			return
		}

		encoded, err := proto.Marshal(&envelope)
		if err != nil {
			log.Warnf("[orgrim] error encoding metadata %v, err: %s", envelope.Metadata, err.Error())
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		err = kw.WriteMessages(context.Background(), kafka.Message{
			Value: encoded,
		})

		if err != nil {
			log.Warnf("[orgrim] error sending message, metadata %v, err: %s", envelope.Metadata, err.Error())
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		c.JSON(200, gin.H{"success": true})
	})

	r.POST("/push/context", func(c *gin.Context) {
		var ctx spec.Context
		err := depths.UnmarshalAndClose(c, &ctx)
		if err != nil {
			log.Warnf("[orgrim] error decoding ctx, err: %s", err.Error())
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		err = spec.ValidateContext(&ctx)
		if err != nil {
			log.Warnf("[orgrim] invalid context, err: %s", err.Error())
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		if ctx.CreatedAtNs == 0 {
			ctx.CreatedAtNs = time.Now().UnixNano()
		}

		at := tp.ExtractFromRequest(c.Request)
		if !at.AllowContext(&ctx) && *allowOnlyIdentified {
			log.Warnf("[orgrim] contex type %s not allowed for token", ctx.ForeignType)
			c.JSON(http.StatusForbidden, gin.H{"error": "invalid token"})
			return
		}

		encoded, err := proto.Marshal(&ctx)
		if err != nil {
			log.Warnf("[orgrim] error encoding context %v, err: %s", ctx, err.Error())
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		err = cw.WriteMessages(context.Background(), kafka.Message{
			Value: encoded,
		})

		if err != nil {
			log.Warnf("[orgrim] error sending message, context %v, err: %s", ctx, err.Error())
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		c.JSON(200, gin.H{"success": true})
	})

	r.POST("/push/flatten", func(c *gin.Context) {
		body := c.Request.Body
		defer body.Close()

		converted, err := spec.DecodeAndFlatten(body)
		if err != nil {
			log.Warnf("[orgrim] invalid input, err: %s", err.Error())
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		err = spec.Decorate(geoip, c.Request, converted)
		if err != nil {
			log.Warnf("[orgrim] failed to decorate, err: %s", err.Error())
		}

		at := tp.ExtractFromRequest(c.Request)
		if !at.AllowEnvelope(converted) && *allowOnlyIdentified {
			log.Warnf("[orgrim] product not allowed for this token")
			c.JSON(http.StatusForbidden, gin.H{"error": "invalid token"})
			return
		}

		encoded, err := proto.Marshal(converted)
		if err != nil {
			log.Warnf("[orgrim] error encoding metadata %v, err: %s", converted.Metadata, err.Error())
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		err = kw.WriteMessages(context.Background(), kafka.Message{
			Value: encoded,
		})

		if err != nil {
			log.Warnf("[orgrim] error sending message, metadata %v, err: %s", converted, err.Error())
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		c.JSON(200, gin.H{"success": true})
	})
	log.Panic(r.Run(*bind))
}
