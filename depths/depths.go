package depths

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"math/rand"
	"path"
	"strings"
	"unicode"

	"github.com/dgryski/go-metro"
	"github.com/gin-gonic/gin"
	"github.com/gogo/protobuf/jsonpb"
	"github.com/gogo/protobuf/proto"
	"github.com/segmentio/kafka-go"
	log "github.com/sirupsen/logrus"
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

func ShuffledStrings(list []string) []string {
	shuffledList := make([]string, len(list))
	copy(shuffledList, list)
	rand.Shuffle(len(shuffledList), func(i, j int) {
		shuffledList[i], shuffledList[j] = shuffledList[j], shuffledList[i]
	})

	return shuffledList
}

func HealthCheckKafka(brokers string, topic string) error {
	for _, b := range ShuffledStrings(strings.Split(brokers, ",")) {
		conn, err := kafka.DialLeader(context.Background(), "tcp", b, topic, 0)
		if err == nil {
			conn.Close()

			return nil
		}
		log.Warnf("failed to dial leader for partition 0, error: %s", err.Error())
	}
	return errors.New("failed to dial leader for partition 0, assuming we cant reach kafka")
}

func CreateTopic(brokers string, topic string, partitions int, replication int) error {
	for _, b := range ShuffledStrings(strings.Split(brokers, ",")) {
		conn, err := kafka.Dial("tcp", b)
		if err == nil {
			return conn.CreateTopics(kafka.TopicConfig{Topic: topic, NumPartitions: partitions, ReplicationFactor: replication})
		}
	}
	return errors.New("failed to dial any broker")
}
func epochDayFromNs(ns int64) string {
	s := ns / 1000000000
	d := s / (3600 * 24)
	return fmt.Sprintf("%d", d)
}

func epochDayFromNsInt(ns int64) int64 {
	s := ns / 1000000000
	d := s / (3600 * 24)
	return d
}

func SegmentFromNs(ns int64) string {
	return epochDayFromNs(ns)
}

func SegmentFromNsInt(ns int64) int64 {
	return epochDayFromNsInt(ns)
}

func PathForTag(root, tagKey, tagValue string) (string, string) {
	dir := path.Join(root, tagKey, fmt.Sprintf("shard_%d", Hashs(tagValue)%255))
	return dir, fmt.Sprintf("%s.p", tagValue)
}

func Cleanup(s string) string {
	return cleanup(s, false)
}

func CleanupAllowDot(s string) string {
	return cleanup(s, true)
}

func cleanup(s string, allowDot bool) string {
	clean := strings.Map(
		func(r rune) rune {
			if r > unicode.MaxLatin1 {
				return -1
			}

			if '0' <= r && r <= '9' {
				return r
			}

			if 'A' <= r && r <= 'Z' {
				return r
			}

			if 'a' <= r && r <= 'z' {
				return r
			}

			if r == ':' || r == '-' || r == '_' {
				return r
			}
			if allowDot && r == '.' {
				return r
			}
			return '_'
		},
		s,
	)
	if len(clean) > 64 {
		// FIXME(jackdoe): not good
		clean = clean[:64]
	}
	return clean
}
func CleanupVW(s string) string {
	clean := strings.Map(
		func(r rune) rune {
			if r > unicode.MaxLatin1 {
				return -1
			}

			if '0' <= r && r <= '9' {
				return r
			}

			if 'A' <= r && r <= 'Z' {
				return r
			}

			if 'a' <= r && r <= 'z' {
				return r
			}

			return '_'
		},
		s,
	)
	return clean
}

func Hash(s []byte) uint64 {
	return metro.Hash64(s, 0)
}

func Hashs(s string) uint64 {
	return metro.Hash64Str(s, 0)
}

func DumpObj(src interface{}) string {
	data, err := json.Marshal(src)
	if err != nil {
		log.Fatalf("marshaling to JSON failed: %s", err.Error())
	}
	var out bytes.Buffer
	err = json.Indent(&out, data, "", "  ")
	if err != nil {
		log.Fatalf("failed to dump object: %s", err.Error())
	}
	return string(out.Bytes())
}
