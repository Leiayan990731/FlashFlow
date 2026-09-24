package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/example/flashflow/internal/config"
	"github.com/example/flashflow/internal/domain"
	"github.com/example/flashflow/internal/observability"
	"github.com/example/flashflow/internal/postgres"
	"github.com/example/flashflow/internal/redisstore"
	"github.com/redis/go-redis/v9"
)

type fakeCache struct {
	result   redisstore.ReserveOutput
	reserved redisstore.ReserveInput
}

func (f *fakeCache) Reserve(_ context.Context, input redisstore.ReserveInput) (redisstore.ReserveOutput, error) {
	f.reserved = input
	if f.result.OrderID == "" {
		f.result.OrderID = input.OrderID
	}
	return f.result, nil
}
func (*fakeCache) Allow(context.Context, string, float64, int) (bool, error) { return true, nil }
func (*fakeCache) GetOrder(context.Context, string) ([]byte, error)          { return nil, redis.Nil }
func (*fakeCache) CacheOrder(context.Context, string, []byte) error          { return nil }
func (*fakeCache) GetProduct(context.Context, string) ([]byte, error)        { return nil, redis.Nil }
func (*fakeCache) CacheProduct(context.Context, string, []byte) error        { return nil }
func (*fakeCache) Stock(context.Context, string) (int64, error)              { return 100, nil }
func (*fakeCache) Ping(context.Context) error                                { return nil }

type fakeRepository struct{}

func (fakeRepository) GetOrder(context.Context, string) (domain.Order, error) {
	return domain.Order{}, postgres.ErrNotFound
}
func (fakeRepository) GetProduct(context.Context, string) (domain.Product, error) {
	return domain.Product{}, postgres.ErrNotFound
}
func (fakeRepository) Ping(context.Context) error { return nil }

func TestCreateOrderReservesAndReturnsAccepted(t *testing.T) {
	cache := &fakeCache{result: redisstore.ReserveOutput{Result: redisstore.Reserved}}
	cfg := config.Gateway{
		MaxBodyBytes: 1 << 20, MaxInFlight: 10,
		RatePerSecond: 100, RateBurst: 100, RequestTimeout: time.Second,
	}
	server := New(cfg, cache, fakeRepository{}, observability.New("gateway_test"), slog.New(slog.NewTextHandler(io.Discard, nil)))
	request := httptest.NewRequest(http.MethodPost, "/v1/orders", bytes.NewBufferString(`{"productId":"sku-phone","quantity":2}`))
	request.Header.Set("X-User-ID", "user-1")
	request.Header.Set("Idempotency-Key", "checkout-1")
	response := httptest.NewRecorder()

	server.Handler().ServeHTTP(response, request)

	if response.Code != http.StatusAccepted {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
	if cache.reserved.ProductID != "sku-phone" || cache.reserved.Quantity != 2 {
		t.Fatalf("unexpected reservation: %#v", cache.reserved)
	}
	var event domain.OrderEvent
	if err := json.Unmarshal(cache.reserved.EventJSON, &event); err != nil {
		t.Fatal(err)
	}
	if err := event.Validate(); err != nil {
		t.Fatalf("invalid emitted event: %v", err)
	}
	if event.UserID != "user-1" || event.OrderID == "" || event.EventID == "" {
		t.Fatalf("unexpected event: %#v", event)
	}
}

func TestCreateOrderRequiresIdempotencyKey(t *testing.T) {
	cache := &fakeCache{result: redisstore.ReserveOutput{Result: redisstore.Reserved}}
	cfg := config.Gateway{MaxBodyBytes: 1 << 20, MaxInFlight: 10, RatePerSecond: 100, RateBurst: 100, RequestTimeout: time.Second}
	server := New(cfg, cache, fakeRepository{}, observability.New("gateway_test_idem"), slog.New(slog.NewTextHandler(io.Discard, nil)))
	request := httptest.NewRequest(http.MethodPost, "/v1/orders", bytes.NewBufferString(`{"productId":"sku-phone","quantity":1}`))
	request.Header.Set("X-User-ID", "user-1")
	response := httptest.NewRecorder()

	server.Handler().ServeHTTP(response, request)
	if response.Code != http.StatusBadRequest {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
}
