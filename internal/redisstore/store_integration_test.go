//go:build integration

package redisstore

import (
	"context"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/example/flashflow/internal/config"
	"github.com/example/flashflow/internal/idgen"
)

func TestConcurrentReservationDoesNotOversell(t *testing.T) {
	address := os.Getenv("TEST_REDIS_ADDR")
	if address == "" {
		t.Skip("TEST_REDIS_ADDR is not set")
	}
	store := New(integrationRedisConfig(address))
	defer store.Close()
	ctx := context.Background()
	productID := "integration-" + mustID(t)
	if err := store.SeedStock(ctx, productID, 100, true); err != nil {
		t.Fatal(err)
	}

	var reserved atomic.Int64
	errorsCh := make(chan error, 500)
	var group sync.WaitGroup
	for index := 0; index < 500; index++ {
		group.Add(1)
		go func(index int) {
			defer group.Done()
			orderID, err := idgen.New()
			if err != nil {
				errorsCh <- err
				return
			}
			result, err := store.Reserve(ctx, ReserveInput{
				OrderID: orderID, UserID: fmt.Sprintf("user-%d", index), ProductID: productID,
				Quantity: 1, IdempotencyKey: "one", EventJSON: []byte(`{}`), StatusJSON: []byte(`{}`),
			})
			if err != nil {
				errorsCh <- err
				return
			}
			if result.Result == Reserved {
				reserved.Add(1)
			}
		}(index)
	}
	group.Wait()
	close(errorsCh)
	for err := range errorsCh {
		t.Error(err)
	}
	if got := reserved.Load(); got != 100 {
		t.Fatalf("reserved=%d, want 100", got)
	}
	stock, err := store.Stock(ctx, productID)
	if err != nil {
		t.Fatal(err)
	}
	if stock != 0 {
		t.Fatalf("stock=%d, want 0", stock)
	}
}

func integrationRedisConfig(address string) config.Redis {
	return config.Redis{
		Addrs: []string{address}, PoolSize: 100, MinIdleConns: 5,
		DialTimeout: time.Second, OperationTimeout: 3 * time.Second,
		StreamName: "integration:orders", StreamGroup: "integration-relay",
		ReservationTTL: time.Hour, OrderCacheTTL: time.Hour, IdempotencyTTL: time.Hour,
	}
}

func mustID(t *testing.T) string {
	t.Helper()
	value, err := idgen.New()
	if err != nil {
		t.Fatal(err)
	}
	return value
}
