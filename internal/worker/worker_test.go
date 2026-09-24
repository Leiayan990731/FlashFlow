package worker

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/example/flashflow/internal/config"
	"github.com/example/flashflow/internal/domain"
	"github.com/example/flashflow/internal/observability"
	"github.com/example/flashflow/internal/platform"
	"github.com/segmentio/kafka-go"
)

type recordingRepository struct{ event domain.OrderEvent }

func (r *recordingRepository) CreateOrder(_ context.Context, event domain.OrderEvent) (bool, error) {
	r.event = event
	return true, nil
}

type recordingCache struct {
	orderID string
	value   []byte
}

func (c *recordingCache) CacheOrder(_ context.Context, orderID string, value []byte) error {
	c.orderID = orderID
	c.value = append([]byte(nil), value...)
	return nil
}
func (*recordingCache) Compensate(context.Context, string, string, int, []byte) (bool, error) {
	return true, nil
}

type recordingPublisher struct{ deadLetters int }

func (p *recordingPublisher) PublishDeadLetter(context.Context, string, []byte, string) error {
	p.deadLetters++
	return nil
}

func testWorker(repository Repository, cache Cache, publisher Publisher, metricName string) *Worker {
	return New(
		config.Worker{MaxProcessAttempts: 1, RetryBase: time.Nanosecond, RetryMax: time.Nanosecond},
		config.Kafka{DeadLetterTopic: "dlq"}, repository, cache, publisher,
		observability.New(metricName), &platform.Readiness{},
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	)
}

func TestProcessPersistsThenCachesConfirmedOrder(t *testing.T) {
	repository := &recordingRepository{}
	cache := &recordingCache{}
	publisher := &recordingPublisher{}
	w := testWorker(repository, cache, publisher, "worker_test")
	event := domain.OrderEvent{
		EventID: "ac4c2f48-e20e-4ad0-89cc-c70ad9b1452d", EventType: domain.EventOrderRequested,
		OrderID: "2cfb8730-e2bc-45ce-942b-fc18624d43bb", UserID: "user",
		ProductID: "sku-phone", Quantity: 1, IdempotencyKey: "idem", RequestedAt: time.Now().UTC(),
	}
	payload, err := json.Marshal(event)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.process(context.Background(), kafka.Message{Value: payload}); err != nil {
		t.Fatal(err)
	}
	if repository.event.EventID != event.EventID {
		t.Fatalf("event was not persisted: %#v", repository.event)
	}
	if cache.orderID != event.OrderID {
		t.Fatalf("wrong cache key: %q", cache.orderID)
	}
	var cached domain.Order
	if err := json.Unmarshal(cache.value, &cached); err != nil {
		t.Fatal(err)
	}
	if cached.Status != domain.OrderConfirmed {
		t.Fatalf("cached status=%q", cached.Status)
	}
	if publisher.deadLetters != 0 {
		t.Fatal("valid event was sent to the dead-letter topic")
	}
}

func TestProcessDeadLettersMalformedEvent(t *testing.T) {
	publisher := &recordingPublisher{}
	w := testWorker(&recordingRepository{}, &recordingCache{}, publisher, "worker_bad_event_test")
	if err := w.process(context.Background(), kafka.Message{Value: []byte(`{"broken":`)}); err != nil {
		t.Fatal(err)
	}
	if publisher.deadLetters != 1 {
		t.Fatalf("dead letters=%d", publisher.deadLetters)
	}
}
