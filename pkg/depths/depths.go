package depths

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"log"
	"math/rand"
	"strings"
	"unicode"

	"github.com/dgryski/go-metro"
	"github.com/segmentio/kafka-go"
)

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
	return out.String()
}

func DumpObjNoIndent(src interface{}) string {
	data, err := json.Marshal(src)
	if err != nil {
		log.Fatalf("marshaling to JSON failed: %s", err.Error())
	}
	return string(data)
}
func ForeachCSV(csv string, cb func(a, b string)) {
	for _, t := range strings.Split(csv, ",") {
		if t == "" {
			continue
		}
		splitted := strings.Split(t, ":")
		if len(splitted) != 2 {
			log.Fatalf("expected a:b, got %v", splitted)
		}
		k := splitted[0]
		v := splitted[1]
		cb(k, v)
	}
}

func UintsToBytes(postings []uint32) []byte {
	n := len(postings) * 4
	longed := make([]byte, n)
	for i := 0; i < len(postings); i++ {
		binary.LittleEndian.PutUint32(longed[i*4:], postings[i])
	}
	return longed
}

func BytesToUints(postings []byte) []uint32 {
	n := len(postings) / 4
	longed := make([]uint32, n)
	j := 0
	for i := 0; i < n*4; i += 4 {
		longed[j] = uint32(binary.LittleEndian.Uint32(postings[i:]))
		j++
	}
	return longed
}

func IntsToBytes(postings []int32) []byte {
	n := len(postings) * 4
	longed := make([]byte, n)
	for i := 0; i < len(postings); i++ {
		binary.LittleEndian.PutUint32(longed[i*4:], uint32(postings[i]))
	}
	return longed
}

func BytesToInts(postings []byte) []int32 {
	n := len(postings) / 4
	longed := make([]int32, n)
	j := 0
	for i := 0; i < n*4; i += 4 {
		longed[j] = int32(binary.LittleEndian.Uint32(postings[i:]))
		j++
	}
	return longed
}

func Uints64ToBytes(postings []uint64) []byte {
	n := len(postings) * 8
	longed := make([]byte, n)
	for i := 0; i < len(postings); i++ {
		binary.LittleEndian.PutUint64(longed[i*8:], postings[i])
	}
	return longed
}

func BytesToUints64(postings []byte) []uint64 {
	n := len(postings) / 8
	longed := make([]uint64, n)
	j := 0
	for i := 0; i < n*8; i += 8 {
		longed[j] = binary.LittleEndian.Uint64(postings[i:])
		j++
	}
	return longed
}

func IsDigit(s string) bool {
	for _, r := range s {
		if !unicode.IsDigit(r) {
			return false
		}
	}
	return true
}
