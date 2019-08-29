package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/avct/uasurfer"
	"github.com/hpcloud/tail"
	orgrim "github.com/jackdoe/blackrock/orgrim/client"
	"github.com/jackdoe/blackrock/orgrim/spec"
	"github.com/satyrius/gonx"
)

func IsBot(x string) bool {
	ua := uasurfer.Parse(x)
	return ua.IsBot()
}

func main() {
	format := flag.String("format", `"$remote_addr [$time_local] "$request" $status`, "the format line format")
	input := flag.String("input", "", "file to tail")
	orgrimUrl := flag.String("orgrim", "http://127.0.0.1:9001", "orgrim url")
	flag.Parse()

	if *input == "" {
		log.Fatal("need -input to tail a file")
	}

	hostname, _ := os.Hostname()
	cfg := fmt.Sprintf(`
            http {
                log_format   main  '%s';
            }`, *format)

	nginxConfig := strings.NewReader(cfg)

	parser, err := gonx.NewNginxParser(nginxConfig, "main")
	if err != nil {
		log.Panic(err)
	}

	og := orgrim.NewClient(*orgrimUrl, nil)

	t, err := tail.TailFile(*input, tail.Config{Follow: true, ReOpen: true})
	for line := range t.Lines {
		rec, err := parser.ParseString(line.Text)
		if err != nil {
			log.Printf("err: %s", err.Error())
			continue
		}
		f := rec.Fields()
		method := ""
		requestPath := ""
		splitted := strings.Split(f["request"], " ")
		if len(splitted) > 0 {
			method = splitted[0]
			requestPath = splitted[1]
			ps := strings.Split(requestPath, "?")
			if len(ps) > 1 {
				requestPath = ps[1]
			}
		}

		localTime, _ := time.Parse("02/Jan/2006:15:04:05 -0700", f["time_local"])
		supstreamTime := f["upstream_response_time"]
		upstreamTime := float64(0)
		slow := false
		if supstreamTime != "" {
			v, err := strconv.ParseFloat(supstreamTime, 64)
			if err == nil {
				upstreamTime = v
			}
		}

		if upstreamTime > 1 {
			slow = true
		}
		tookMs := uint64(upstreamTime * 1000)
		properties := []spec.KV{}
		for k, v := range f {
			if k == "status" || k == "time_local" {
				continue
			}
			properties = append(properties, orgrim.KV(k, v))
		}

		kv := &spec.Envelope{
			Metadata: &spec.Metadata{
				ForeignType: "nginx",
				ForeignId:   hostname,
				EventType:   "request",
				Search: []spec.KV{
					orgrim.KV("status", f["status"]),
					orgrim.KV("method", method),
					orgrim.KV("path", requestPath),
					orgrim.KV("hostname", hostname),
					orgrim.KV("slow", slow),
					orgrim.KV("bot", IsBot(f["http_user_agent"])),
				},
				Count: []spec.KV{
					orgrim.KV("took_round", tookMs/100*100),
				},

				CreatedAtNs: localTime.UnixNano(),
				Properties:  properties,
			},
		}
		err = og.Push(kv)
		if err != nil {
			log.Printf("error pushing, err: %s", err.Error())
		}
	}
}
