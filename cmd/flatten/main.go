package main

import (
	"context"
	"flag"
	"io/ioutil"
	"net/http"

	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
	"github.com/gogo/protobuf/jsonpb"
	"github.com/gogo/protobuf/proto"
	"github.com/oschwald/geoip2-golang"
	spec "github.com/rekki/blackrock/pkg/blackrock_io"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"

	"strings"
	"time"
)

func UnmarshalAndClose(c *gin.Context, into proto.Message) error {
	body := c.Request.Body
	defer body.Close()

	var err error
	if c.Request.Header.Get("content-type") == "application/protobuf" {
		var data []byte
		data, err = ioutil.ReadAll(body)
		if err == nil {
			err = proto.Unmarshal(data, into)
		}
	} else {
		err = jsonpb.Unmarshal(body, into)
	}

	return err
}
func main() {
	var bind = flag.String("bind", ":9001", "bind to")
	var remote = flag.String("enqueue-grpc", ":8001", "connect to enqueue grpc")
	var geoipFile = flag.String("geoip", "", "path to https://dev.maxmind.com/geoip/geoip2/geolite2/ file")
	flag.Parse()

	var geoip *geoip2.Reader
	var err error
	if *geoipFile != "" {
		geoip, err = geoip2.Open(*geoipFile)
		if err != nil {
			log.Fatal(err)
		}
		defer geoip.Close()
	}

	r := gin.Default()
	r.Use(gin.Recovery())
	r.Use(cors.Default())

	conn, err := grpc.Dial(*remote, grpc.WithInsecure())
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	enqueue := spec.NewEnqueueClient(conn)

	r.GET("/health", func(c *gin.Context) {
		_, err := enqueue.SayHealth(context.Background(), &spec.HealthRequest{})
		if err != nil {
			c.String(http.StatusInternalServerError, "BAD")
		} else {
			c.String(http.StatusOK, "OK")
		}
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
			stream, err := enqueue.SayPush(context.Background())
			if err != nil {
				log.Warnf("[orgrim] error sending message, metadata %v, err: %s", envelope.Metadata, err.Error())
			} else {
				err := stream.Send(envelope)
				if err != nil {
					log.Warnf("[orgrim] error sending message, metadata %v, err: %s", envelope.Metadata, err.Error())
				}
				_, err = stream.CloseAndRecv()
				if err != nil {
					log.Warnf("[orgrim] error sending message, metadata %v, err: %s", envelope.Metadata, err.Error())
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
		err := UnmarshalAndClose(c, &envelope)
		if err != nil {
			log.Warnf("[orgrim] error decoding envelope, err: %s", err.Error())
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}

		stream, err := enqueue.SayPush(context.Background())
		if err != nil {
			log.Warnf("[orgrim] error sending message, metadata %v, err: %s", envelope.Metadata, err.Error())
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		err = stream.Send(&envelope)
		if err != nil {

			_, _ = stream.CloseAndRecv() // close anyway

			log.Warnf("[orgrim] error sending message, metadata %v, err: %s", envelope.Metadata, err.Error())
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		_, err = stream.CloseAndRecv()
		if err != nil {
			log.Warnf("[orgrim] error sending message, metadata %v, err: %s", envelope.Metadata, err.Error())
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
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
		err = spec.Decorate(geoip, c.Request, converted)
		if err != nil {
			log.Warnf("[orgrim] failed to decorate, err: %s", err.Error())
		}

		stream, err := enqueue.SayPush(context.Background())
		if err != nil {
			log.Warnf("[orgrim] error sending message, metadata %v, err: %s", converted.Metadata, err.Error())
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		err = stream.Send(converted)
		if err != nil {
			log.Warnf("[orgrim] error sending message, metadata %v, err: %s", converted.Metadata, err.Error())
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		_, err = stream.CloseAndRecv()
		if err != nil {
			log.Warnf("[orgrim] error sending message, metadata %v, err: %s", converted.Metadata, err.Error())
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		c.JSON(200, gin.H{"success": true})
	})
	log.Panic(r.Run(*bind))
}
