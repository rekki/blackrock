package depths

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"path"
	"strings"
	"unicode"

	"github.com/dgryski/go-metro"
	"github.com/segmentio/kafka-go"
	log "github.com/sirupsen/logrus"
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
		log.Warnf("failed to dial leader for partition 0, error: %s", err.Error())
	}
	return errors.New("failed to dial leader for partition 0, assuming we cant reach kafka")
}

func PathForTag(root string, topic string, tagKey string, tagValue string) (string, string) {
	dir := path.Join(root, topic, Cleanup(tagKey), fmt.Sprintf("metro_32_%d", Hashs(tagValue)%32))
	return dir, fmt.Sprintf("%s.p", Cleanup(tagValue))
}

func Cleanup(s string) string {
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

func Hash(s []byte) uint64 {
	return metro.Hash64(s, 0)
}

func Hashs(s string) uint64 {
	return metro.Hash64Str(s, 0)
}
