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
	orgrim "github.com/rekki/blackrock/cmd/orgrim/client"
	"github.com/rekki/blackrock/cmd/orgrim/spec"
	"github.com/satyrius/gonx"
)

func isBot(x string) bool {
	ua := uasurfer.Parse(x)
	return ua.IsBot()
}

func main() {
	format := flag.String("format", `"$remote_addr [$time_local] "$request" $status`, "the format line format")
	input := flag.String("input", "", "file to tail")
	orgrimURL := flag.String("orgrim", "http://127.0.0.1:9001", "orgrim url")
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

	og := orgrim.NewClient(*orgrimURL, "", nil)

	t, err := tail.TailFile(*input, tail.Config{Follow: true, ReOpen: true})
	for line := range t.Lines {
		rec, err := parser.ParseString(line.Text)
		if err != nil {
			log.Printf("err: %s", err.Error())
			continue
		}

		timeLocalField, _ := rec.Field("time_local")
		requestField, _ := rec.Field("request")
		upstreamResponseTimeField, _ := rec.Field("upstream_response_time")
		statusField, _ := rec.Field("status")
		httpUserAgentField, _ := rec.Field("http_user_agent_field")

		method := ""
		requestPath := ""
		splitted := strings.Split(requestField, " ")
		if len(splitted) > 0 {
			method = splitted[0]
			requestPath = splitted[1]
			ps := strings.Split(requestPath, "?")
			if len(ps) > 1 {
				requestPath = ps[1]
			}
		}

		localTime, _ := time.Parse("02/Jan/2006:15:04:05 -0700", timeLocalField)
		supstreamTime := upstreamResponseTimeField
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

		// TODO(aymeric): we can no longer iterate on those keys
		properties := []spec.KV{}
		// for k, v := range f {
		// 	if k == "status" || k == "time_local" {
		// 		continue
		// 	}
		// 	properties = append(properties, orgrim.KV(k, v))
		// }

		kv := &spec.Envelope{
			Metadata: &spec.Metadata{
				ForeignType: "nginx",
				ForeignId:   hostname,
				EventType:   "request",
				Search: []spec.KV{
					orgrim.KV("status", statusField),
					orgrim.KV("method", method),
					orgrim.KV("path", requestPath),
					orgrim.KV("hostname", hostname),
					orgrim.KV("slow", slow),
					orgrim.KV("bot", isBot(httpUserAgentField)),
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
