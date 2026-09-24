//go:build integration

package postgres

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/example/flashflow/internal/config"
	"github.com/example/flashflow/internal/domain"
	"github.com/example/flashflow/internal/idgen"
)

func TestCreateOrderIsIdempotentAcrossRedelivery(t *testing.T) {
	url := os.Getenv("TEST_POSTGRES_URL")
	if url == "" {
		t.Skip("TEST_POSTGRES_URL is not set")
	}
	ctx := context.Background()
	store, err := New(ctx, config.Database{URL: url, MaxConns: 10, MinConns: 1, ConnectTimeout: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err := store.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	event := domain.OrderEvent{
		EventID: mustDatabaseID(t), EventType: domain.EventOrderRequested,
		OrderID: mustDatabaseID(t), UserID: "integration-" + mustDatabaseID(t),
		ProductID: "sku-phone", Quantity: 1, IdempotencyKey: mustDatabaseID(t),
		RequestedAt: time.Now().UTC(),
	}
	created, err := store.CreateOrder(ctx, event)
	if err != nil {
		t.Fatal(err)
	}
	if !created {
		t.Fatal("first delivery did not create an order")
	}
	created, err = store.CreateOrder(ctx, event)
	if err != nil {
		t.Fatal(err)
	}
	if created {
		t.Fatal("redelivery created a duplicate order")
	}
	order, err := store.GetOrder(ctx, event.OrderID)
	if err != nil {
		t.Fatal(err)
	}
	if order.Status != domain.OrderConfirmed || order.UserID != event.UserID {
		t.Fatalf("unexpected order: %#v", order)
	}
}

func mustDatabaseID(t *testing.T) string {
	t.Helper()
	value, err := idgen.New()
	if err != nil {
		t.Fatal(err)
	}
	return value
}
