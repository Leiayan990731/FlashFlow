package messagebus

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/example/flashflow/internal/config"
	"github.com/segmentio/kafka-go"
)

type Publisher struct {
	writer  *kafka.Writer
	timeout time.Duration
}

func NewPublisher(cfg config.Kafka) *Publisher {
	return &Publisher{
		writer: &kafka.Writer{
			Addr:         kafka.TCP(cfg.Brokers...),
			Balancer:     &kafka.Hash{},
			RequiredAcks: kafka.RequireAll,
			Async:        false,
			BatchSize:    100,
			BatchBytes:   1 << 20,
			BatchTimeout: 10 * time.Millisecond,
			ReadTimeout:  5 * time.Second,
			WriteTimeout: cfg.PublishTimeout,
			MaxAttempts:  10,
		},
		timeout: cfg.PublishTimeout,
	}
}

func (p *Publisher) Publish(ctx context.Context, topic string, key, value []byte, headers map[string]string) error {
	publishCtx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()
	kafkaHeaders := make([]kafka.Header, 0, len(headers))
	for name, value := range headers {
		kafkaHeaders = append(kafkaHeaders, kafka.Header{Key: name, Value: []byte(value)})
	}
	err := p.writer.WriteMessages(publishCtx, kafka.Message{
		Topic: topic, Key: key, Value: value, Headers: kafkaHeaders, Time: time.Now().UTC(),
	})
	if err != nil {
		return fmt.Errorf("publish kafka message: %w", err)
	}
	return nil
}

func (p *Publisher) PublishDeadLetter(ctx context.Context, topic string, source []byte, reason string) error {
	payload, err := json.Marshal(struct {
		Reason   string    `json:"reason"`
		Payload  string    `json:"payload"`
		FailedAt time.Time `json:"failedAt"`
	}{Reason: reason, Payload: string(source), FailedAt: time.Now().UTC()})
	if err != nil {
		return fmt.Errorf("encode dead letter: %w", err)
	}
	return p.Publish(ctx, topic, nil, payload, map[string]string{"event-type": "dead-letter"})
}

func (p *Publisher) Close() error { return p.writer.Close() }

type Consumer struct {
	reader *kafka.Reader
}

func NewConsumer(cfg config.Kafka, topic, groupID string) *Consumer {
	return &Consumer{reader: kafka.NewReader(kafka.ReaderConfig{
		Brokers:     cfg.Brokers,
		GroupID:     groupID,
		Topic:       topic,
		MinBytes:    cfg.MinBytes,
		MaxBytes:    cfg.MaxBytes,
		MaxWait:     cfg.MaxWait,
		StartOffset: kafka.FirstOffset,
		// Zero makes CommitMessages synchronous. Each Consumer is processed
		// sequentially, so a later partition offset can never skip earlier work.
		CommitInterval: 0,
	})}
}

func (c *Consumer) Fetch(ctx context.Context) (kafka.Message, error) {
	return c.reader.FetchMessage(ctx)
}

func (c *Consumer) Commit(ctx context.Context, message kafka.Message) error {
	if err := c.reader.CommitMessages(ctx, message); err != nil {
		return fmt.Errorf("commit kafka offset: %w", err)
	}
	return nil
}

func (c *Consumer) Close() error { return c.reader.Close() }
