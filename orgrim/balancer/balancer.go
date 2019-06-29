package balancer

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"time"

	"github.com/segmentio/kafka-go"
	log "github.com/sirupsen/logrus"
)

type KafkaBalancer struct {
	connections []*kafka.Conn
}

func (k *KafkaBalancer) Write(data []byte) (int32, int64, error) {
	conn := k.connections[rand.Int()%len(k.connections)]
	_, p, o, _, err := conn.WriteCompressedMessagesAt(nil, kafka.Message{Value: data})
	return p, o, err
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
		out := &KafkaBalancer{connections: []*kafka.Conn{}}

		for _, p := range partitions {
			c, err := kafka.DialLeader(context.Background(), "tcp", fmt.Sprintf("%s:%d", p.Leader.Host, p.Leader.Port), topic, p.ID)
			if err != nil {
				out.Close()
				log.Warnf("failed to get leader for partition %v, error: %s", p, err.Error())
				return nil, err
			}
			out.connections = append(out.connections, c)
		}
		return out, nil
	}
	return nil, errors.New("failed to connect")
}
