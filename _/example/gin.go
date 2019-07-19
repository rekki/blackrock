package main

import (
	"log"
	"math/rand"
	"os"
	"time"

	"github.com/gin-gonic/gin"
	orgrim "github.com/jackdoe/blackrock/orgrim/client"
	"github.com/jackdoe/blackrock/orgrim/spec"
)

func main() {
	og := orgrim.NewClient("http://localhost:9001/", nil)
	r := gin.New()

	r.Use(Blackrock("request", "gin", "example", og, nil))

	r.Use(gin.Recovery())

	r.GET("/*anything", func(c *gin.Context) {
		if rand.Intn(100) < 20 {
			panic("noooo")
		}
		sleep := rand.Intn(300)
		<-time.After(time.Duration(sleep) * time.Millisecond)
		c.String(200, "pong")
	})

	r.Run(":8080")
}

func Blackrock(eventType string, foreignType string, foreignId string, og *orgrim.Client, appender func(*spec.Envelope, *gin.Context) *spec.Envelope) gin.HandlerFunc {
	hostname, _ := os.Hostname()

	return func(c *gin.Context) {
		t0 := time.Now()

		c.Next()

		r := c.Request
		status := c.Writer.Status()
		took := int64(time.Since(t0) / 1e6)
		nSeconds := took / 1000

		kv := &spec.Envelope{
			Metadata: &spec.Metadata{
				ForeignType: foreignType,
				ForeignId:   foreignId,
				EventType:   eventType,
				Search: []*spec.KV{
					orgrim.KV("status", status),
					orgrim.KV("method", r.Method),
					orgrim.KV("proto", r.Proto),
					orgrim.KV("hostname", hostname),
					orgrim.KV("host", c.Request.Host),
					orgrim.KV("took_seconds", nSeconds),
				},
				Count: []*spec.KV{
					orgrim.KV("took_ms_round", took/100*100),
				},
				Properties: []*spec.KV{
					orgrim.KV("user_agent", c.Request.UserAgent()),
					orgrim.KV("took_ms", took),
				},
			},
		}
		if appender != nil {
			kv = appender(kv, c)
		}
		go func() {
			err := og.Push(kv)
			if err != nil {
				log.Printf("error pushing, err: %s", err.Error())
			}
		}()
	}
}
