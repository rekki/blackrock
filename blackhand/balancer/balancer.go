package balancer

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
	log "github.com/sirupsen/logrus"
)

type KafkaBalancer struct {
	connections map[int]*kafka.Conn
	sync.Mutex
}

func (k *KafkaBalancer) ReadAt(partition int, offset int64) ([]byte, error) {
	k.Lock()
	defer k.Unlock()
	p, ok := k.connections[partition]
	if !ok {
		return nil, errors.New(fmt.Sprintf("no such partition %d", partition))
	}

	newOffset, err := p.Seek(offset, kafka.SeekAbsolute)
	if err != nil {
		return nil, err
	}
	if newOffset != offset {
		return nil, errors.New(fmt.Sprintf("offset mismatch, expected %d got %d", offset, newOffset))
	}
	m, err := p.ReadMessage(1024 * 1024) // 1mb

	if err != nil {
		return nil, err
	}
	return m.Value, nil
}

func (k *KafkaBalancer) Close() {
	for _, c := range k.connections {
		c.Close()
	}
}

func shuffledStrings(list []string) []string {
	shuffledList := make([]string, len(list))
	copy(shuffledList, list)
	rand.Shuffle(len(shuffledList), func(i, j int) {
		shuffledList[i], shuffledList[j] = shuffledList[j], shuffledList[i]
	})

	return shuffledList
}

func NewKafkaBalancer(topic string, brokers []string) (*KafkaBalancer, error) {
	brokers = shuffledStrings(brokers)
	for _, broker := range brokers {
		log.Infof("connecting to %s", broker)
		conn, err := kafka.Dial("tcp", broker)
		if err != nil {
			log.Warnf("failed to connect to broker %s, error: %s", broker, err.Error())
			continue
		}
		conn.SetWriteDeadline(time.Now().Add(10 * time.Second))

		partitions, err := conn.ReadPartitions(topic)
		if err != nil {
			log.Warnf("failed to get partitions from broker %s, error: %s", broker, err.Error())
			conn.Close()
			continue
		}
		out := &KafkaBalancer{connections: map[int]*kafka.Conn{}}

		for _, p := range partitions {
			c, err := kafka.DialLeader(context.Background(), "tcp", fmt.Sprintf("%s:%d", p.Leader.Host, p.Leader.Port), topic, p.ID)
			if err != nil {
				out.Close()
				log.Warnf("failed to get leader for partition %v, error: %s", p, err.Error())
				return nil, err
			}
			out.connections[p.ID] = c
		}
		return out, nil
	}
	return nil, errors.New("failed to connect")
}
