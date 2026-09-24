package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/example/flashflow/internal/backoff"
	"github.com/example/flashflow/internal/config"
	"github.com/example/flashflow/internal/domain"
	"github.com/example/flashflow/internal/messagebus"
	"github.com/example/flashflow/internal/observability"
	"github.com/example/flashflow/internal/platform"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/segmentio/kafka-go"
)

type Repository interface {
	CreateOrder(context.Context, domain.OrderEvent) (bool, error)
}

type Cache interface {
	CacheOrder(context.Context, string, []byte) error
	Compensate(context.Context, string, string, int, []byte) (bool, error)
}

type Publisher interface {
	PublishDeadLetter(context.Context, string, []byte, string) error
}

type Worker struct {
	cfg       config.Worker
	kafkaCfg  config.Kafka
	db        Repository
	cache     Cache
	publisher Publisher
	metrics   *observability.Metrics
	ready     *platform.Readiness
	log       *slog.Logger
}

func New(cfg config.Worker, kafkaCfg config.Kafka, db Repository, cache Cache, publisher Publisher, metrics *observability.Metrics, ready *platform.Readiness, log *slog.Logger) *Worker {
	return &Worker{cfg: cfg, kafkaCfg: kafkaCfg, db: db, cache: cache, publisher: publisher, metrics: metrics, ready: ready, log: log}
}

func (w *Worker) Run(ctx context.Context) error {
	workerCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	errCh := make(chan error, w.cfg.Concurrency)
	var group sync.WaitGroup
	for i := 0; i < w.cfg.Concurrency; i++ {
		group.Add(1)
		go func(workerID int) {
			defer group.Done()
			if err := w.consume(workerCtx, workerID); err != nil && workerCtx.Err() == nil {
				select {
				case errCh <- err:
				default:
				}
				cancel()
			}
		}(i)
	}
	w.ready.Set(true)
	defer w.ready.Set(false)

	done := make(chan struct{})
	go func() {
		group.Wait()
		close(done)
	}()
	select {
	case <-ctx.Done():
		cancel()
		<-done
		return nil
	case err := <-errCh:
		cancel()
		<-done
		return err
	case <-done:
		select {
		case err := <-errCh:
			return err
		default:
			return nil
		}
	}
}

func (w *Worker) consume(ctx context.Context, workerID int) error {
	consumer := messagebus.NewConsumer(w.kafkaCfg, w.kafkaCfg.OrderTopic, w.cfg.ConsumerGroup)
	defer consumer.Close()
	for attempt := 0; ctx.Err() == nil; {
		message, err := consumer.Fetch(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			w.ready.Set(false)
			w.metrics.DependencyUp.WithLabelValues("kafka").Set(0)
			w.log.Warn("fetch kafka message failed", "error", err, "worker", workerID)
			if !sleep(ctx, backoff.Duration(attempt, 100*time.Millisecond, 5*time.Second)) {
				return nil
			}
			attempt++
			continue
		}
		attempt = 0
		w.ready.Set(true)
		w.metrics.DependencyUp.WithLabelValues("kafka").Set(1)
		if err := w.process(ctx, message); err != nil {
			return fmt.Errorf("worker %d process offset %d: %w", workerID, message.Offset, err)
		}
		commitCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		err = consumer.Commit(commitCtx, message)
		cancel()
		if err != nil {
			return fmt.Errorf("worker %d: %w", workerID, err)
		}
		w.metrics.KafkaMessages.WithLabelValues("committed").Inc()
	}
	return nil
}

func (w *Worker) process(ctx context.Context, message kafka.Message) error {
	var event domain.OrderEvent
	if err := json.Unmarshal(message.Value, &event); err != nil {
		if dlqErr := w.deadLetter(ctx, message.Value, "invalid JSON: "+err.Error()); dlqErr != nil {
			return dlqErr
		}
		w.metrics.Orders.WithLabelValues("dead_lettered").Inc()
		return nil
	}
	if err := event.Validate(); err != nil {
		if dlqErr := w.deadLetter(ctx, message.Value, "invalid event: "+err.Error()); dlqErr != nil {
			return dlqErr
		}
		w.metrics.Orders.WithLabelValues("dead_lettered").Inc()
		return nil
	}

	var created bool
	var err error
	for attempt := 0; attempt < w.cfg.MaxProcessAttempts; attempt++ {
		created, err = w.db.CreateOrder(ctx, event)
		if err == nil {
			break
		}
		if permanentDatabaseError(err) {
			return w.reject(ctx, event, message.Value, err)
		}
		w.metrics.DependencyUp.WithLabelValues("postgres").Set(0)
		w.log.Warn("postgres write will be retried", "error", err, "order_id", event.OrderID, "attempt", attempt+1)
		if !sleep(ctx, backoff.Duration(attempt, w.cfg.RetryBase, w.cfg.RetryMax)) {
			return ctx.Err()
		}
	}
	if err != nil {
		// Leave the offset uncommitted and restart the pod. This is safer than
		// converting a prolonged database outage into rejected customer orders.
		return fmt.Errorf("postgres retries exhausted: %w", err)
	}
	w.metrics.DependencyUp.WithLabelValues("postgres").Set(1)
	status := domain.Order{
		OrderID: event.OrderID, UserID: event.UserID, ProductID: event.ProductID,
		Quantity: event.Quantity, Status: domain.OrderConfirmed,
		RequestedAt: event.RequestedAt, UpdatedAt: time.Now().UTC(),
	}
	if encoded, marshalErr := json.Marshal(status); marshalErr == nil {
		if cacheErr := w.cache.CacheOrder(ctx, event.OrderID, encoded); cacheErr != nil {
			w.log.Warn("order cache update failed", "error", cacheErr, "order_id", event.OrderID)
		}
	}
	if created {
		w.metrics.Orders.WithLabelValues("created").Inc()
	} else {
		w.metrics.Orders.WithLabelValues("duplicate").Inc()
	}
	return nil
}

func (w *Worker) reject(ctx context.Context, event domain.OrderEvent, source []byte, cause error) error {
	reason := "order violates a permanent database constraint"
	status := domain.Order{
		OrderID: event.OrderID, UserID: event.UserID, ProductID: event.ProductID,
		Quantity: event.Quantity, Status: domain.OrderRejected, FailureReason: reason,
		RequestedAt: event.RequestedAt, UpdatedAt: time.Now().UTC(),
	}
	encoded, err := json.Marshal(status)
	if err != nil {
		return fmt.Errorf("encode rejected order: %w", err)
	}
	if err := w.retry(ctx, func() error {
		_, compensationErr := w.cache.Compensate(ctx, event.OrderID, event.ProductID, event.Quantity, encoded)
		return compensationErr
	}); err != nil {
		return err
	}
	if err := w.deadLetter(ctx, source, cause.Error()); err != nil {
		return err
	}
	w.metrics.Orders.WithLabelValues("rejected").Inc()
	return nil
}

func (w *Worker) deadLetter(ctx context.Context, payload []byte, reason string) error {
	return w.retry(ctx, func() error {
		return w.publisher.PublishDeadLetter(ctx, w.kafkaCfg.DeadLetterTopic, payload, reason)
	})
}

func (w *Worker) retry(ctx context.Context, operation func() error) error {
	var lastErr error
	for attempt := 0; attempt < w.cfg.MaxProcessAttempts; attempt++ {
		if err := operation(); err == nil {
			return nil
		} else {
			lastErr = err
			w.log.Warn("worker operation will be retried", "error", err, "attempt", attempt+1)
		}
		if !sleep(ctx, backoff.Duration(attempt, w.cfg.RetryBase, w.cfg.RetryMax)) {
			return ctx.Err()
		}
	}
	return fmt.Errorf("operation retries exhausted: %w", lastErr)
}

func permanentDatabaseError(err error) bool {
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		return false
	}
	return strings.HasPrefix(pgErr.Code, "22") || strings.HasPrefix(pgErr.Code, "23")
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
