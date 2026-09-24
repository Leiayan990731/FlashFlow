package relay

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/example/flashflow/internal/backoff"
	"github.com/example/flashflow/internal/config"
	"github.com/example/flashflow/internal/domain"
	"github.com/example/flashflow/internal/observability"
	"github.com/example/flashflow/internal/platform"
	"github.com/example/flashflow/internal/redisstore"
)

type Stream interface {
	EnsureGroup(context.Context) error
	ReadGroup(context.Context, string, int64, time.Duration) ([]redisstore.StreamMessage, error)
	ClaimStale(context.Context, string, time.Duration, int64) ([]redisstore.StreamMessage, error)
	Ack(context.Context, ...string) error
}

type Publisher interface {
	Publish(context.Context, string, []byte, []byte, map[string]string) error
	PublishDeadLetter(context.Context, string, []byte, string) error
}

type Relay struct {
	cfg       config.Relay
	kafkaCfg  config.Kafka
	instance  string
	stream    Stream
	publisher Publisher
	metrics   *observability.Metrics
	ready     *platform.Readiness
	log       *slog.Logger
}

func New(cfg config.Relay, kafkaCfg config.Kafka, instance string, stream Stream, publisher Publisher, metrics *observability.Metrics, ready *platform.Readiness, log *slog.Logger) *Relay {
	return &Relay{cfg: cfg, kafkaCfg: kafkaCfg, instance: instance, stream: stream, publisher: publisher, metrics: metrics, ready: ready, log: log}
}

func (r *Relay) Run(ctx context.Context) error {
	for attempt := 0; ; attempt++ {
		if err := r.stream.EnsureGroup(ctx); err == nil {
			break
		} else if !sleep(ctx, backoff.Duration(attempt, 100*time.Millisecond, 5*time.Second)) {
			return ctx.Err()
		}
	}
	r.ready.Set(true)
	defer r.ready.Set(false)

	var workers sync.WaitGroup
	for i := 0; i < r.cfg.Concurrency; i++ {
		workers.Add(1)
		go func(worker int) {
			defer workers.Done()
			r.consume(ctx, fmt.Sprintf("%s-%d", r.instance, worker), worker == 0)
		}(i)
	}
	workers.Wait()
	return nil
}

func (r *Relay) consume(ctx context.Context, consumer string, reclaim bool) {
	claimEvery := r.cfg.ClaimIdle / 2
	if claimEvery < 5*time.Second {
		claimEvery = 5 * time.Second
	}
	claimTicker := time.NewTicker(claimEvery)
	defer claimTicker.Stop()
	attempt := 0
	for ctx.Err() == nil {
		if reclaim {
			select {
			case <-claimTicker.C:
				messages, err := r.stream.ClaimStale(ctx, consumer, r.cfg.ClaimIdle, r.cfg.BatchSize)
				if err != nil {
					r.log.Warn("claim stale stream messages failed", "error", err, "consumer", consumer)
				} else {
					r.handleBatch(ctx, messages)
				}
			default:
			}
		}
		messages, err := r.stream.ReadGroup(ctx, consumer, r.cfg.BatchSize, r.cfg.Block)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			r.metrics.DependencyUp.WithLabelValues("redis").Set(0)
			r.log.Warn("read redis stream failed", "error", err, "consumer", consumer)
			if !sleep(ctx, backoff.Duration(attempt, 100*time.Millisecond, 5*time.Second)) {
				return
			}
			attempt++
			continue
		}
		attempt = 0
		r.metrics.DependencyUp.WithLabelValues("redis").Set(1)
		r.handleBatch(ctx, messages)
	}
}

func (r *Relay) handleBatch(ctx context.Context, messages []redisstore.StreamMessage) {
	for _, message := range messages {
		if ctx.Err() != nil {
			return
		}
		r.deliver(ctx, message)
	}
}

func (r *Relay) deliver(ctx context.Context, message redisstore.StreamMessage) {
	var event domain.OrderEvent
	if err := json.Unmarshal(message.Event, &event); err != nil || event.Validate() != nil {
		reason := "invalid order event"
		if err != nil {
			reason = err.Error()
		} else if validationErr := event.Validate(); validationErr != nil {
			reason = validationErr.Error()
		}
		r.retry(ctx, func() error {
			return r.publisher.PublishDeadLetter(ctx, r.kafkaCfg.DeadLetterTopic, message.Event, reason)
		})
		r.metrics.StreamMessages.WithLabelValues("dead_lettered").Inc()
	} else {
		r.retry(ctx, func() error {
			return r.publisher.Publish(ctx, r.kafkaCfg.OrderTopic, []byte(event.OrderID), message.Event, map[string]string{
				"event-id": event.EventID, "event-type": event.EventType, "correlation-id": event.CorrelationID,
			})
		})
		r.metrics.StreamMessages.WithLabelValues("published").Inc()
	}
	if ctx.Err() != nil {
		return
	}
	r.retry(ctx, func() error { return r.stream.Ack(ctx, message.ID) })
}

func (r *Relay) retry(ctx context.Context, operation func() error) {
	for attempt := 0; ctx.Err() == nil; attempt++ {
		if err := operation(); err == nil {
			return
		} else {
			r.log.Warn("relay operation will be retried", "error", err, "attempt", attempt+1)
		}
		if !sleep(ctx, backoff.Duration(attempt, 50*time.Millisecond, 5*time.Second)) {
			return
		}
	}
}

func sleep(ctx context.Context, duration time.Duration) bool {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
