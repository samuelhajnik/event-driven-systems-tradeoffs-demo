package ingest

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"event-driven-systems-tradeoffs-demo/internal/domain"

	"github.com/segmentio/kafka-go"
)

// Producer abstracts event publishing behavior.
type Producer interface {
	Publish(ctx context.Context, event domain.Event) error
	Close() error
}

// KafkaProducer publishes events into Kafka.
type KafkaProducer struct {
	writer *kafka.Writer
}

func NewKafkaProducer(brokers []string, topic string) *KafkaProducer {
	return &KafkaProducer{
		writer: &kafka.Writer{
			Addr:                   kafka.TCP(brokers...),
			Topic:                  topic,
			Balancer:               &kafka.Hash{}, // key-based partitioning supports per-device ordering.
			RequiredAcks:           kafka.RequireAll,
			Async:                  false,
			BatchTimeout:           50 * time.Millisecond,
			AllowAutoTopicCreation: true,
		},
	}
}

func (p *KafkaProducer) Publish(ctx context.Context, event domain.Event) error {
	payload, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal event: %w", err)
	}

	msg := kafka.Message{
		Key:   []byte(event.DeviceID),
		Value: payload,
		Time:  time.Now().UTC(),
	}

	if err := p.writer.WriteMessages(ctx, msg); err != nil {
		return fmt.Errorf("write kafka message: %w", err)
	}
	return nil
}

func (p *KafkaProducer) Close() error {
	return p.writer.Close()
}
