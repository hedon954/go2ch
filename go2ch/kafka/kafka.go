package kf

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/segmentio/kafka-go"
	"go2ch/go2ch/config"
	"net"
	"strconv"
)

type Writer struct {
	ctx           context.Context
	leaderAddress string
	Writer        *kafka.Writer
	topics        []string
}

func NewWriter(ctx context.Context, config *config.KafkaConf) (*Writer, error) {
	if len(config.Brokers) == 0 {
		return nil, fmt.Errorf("NewWriter | lack kafka broker address in config")
	}
	conn, err := kafka.Dial("tcp", config.Brokers[0])
	if err != nil {
		return nil, fmt.Errorf("NewWriter | dial kafka broker[%s] failed: %v", config.Brokers[0], err)
	}
	defer conn.Close()
	controller, err := conn.Controller()
	if err != nil {
		return nil, fmt.Errorf("NewWriter | get kafka connection controller failed: %v", err)
	}
	leaderAddress := net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port))

	return &Writer{
		ctx:           ctx,
		leaderAddress: leaderAddress,
		Writer: &kafka.Writer{
			Addr:     kafka.TCP(leaderAddress),
			Balancer: &kafka.Hash{},
		},
		topics: config.Topics,
	}, nil
}

// Produce sends datas to kafka
func (w *Writer) Produce(datas ...interface{}) error {
	messages, err := w.getMessage(datas...)
	if err != nil {
		return fmt.Errorf("Produce | encapsulates datas to kafka messages failed: %v", err)
	}
	err = w.Writer.WriteMessages(w.ctx, messages...)
	if err != nil {
		return fmt.Errorf("Produce | write messages to kafka failed: %v", err)
	}
	return nil
}

// getMessage encapsulates datas to a slice of kafka message
func (w *Writer) getMessage(datas ...interface{}) ([]kafka.Message, error) {
	messages := make([]kafka.Message, 0)
	for _, data := range datas {
		bs, err := json.Marshal(data)
		if err != nil {
			return nil, fmt.Errorf("getMessahe | marshal data to json bytes failed: %v", err)
		}
		for _, topic := range w.topics {
			messages = append(messages, kafka.Message{
				Topic: topic,
				// Key: []byte("")
				Value: bs,
			})
		}
	}
	return messages, nil
}
