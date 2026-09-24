package postgres

import (
	"context"
	"embed"
	"errors"
	"fmt"
	"io/fs"
	"sort"
	"time"

	"github.com/example/flashflow/internal/config"
	"github.com/example/flashflow/internal/domain"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var ErrNotFound = errors.New("not found")

//go:embed migrations/*.sql
var migrationFiles embed.FS

type Store struct {
	pool *pgxpool.Pool
}

func New(ctx context.Context, cfg config.Database) (*Store, error) {
	poolConfig, err := pgxpool.ParseConfig(cfg.URL)
	if err != nil {
		return nil, fmt.Errorf("parse postgres config: %w", err)
	}
	poolConfig.MaxConns = int32(cfg.MaxConns)
	poolConfig.MinConns = int32(cfg.MinConns)
	poolConfig.MaxConnLifetime = 30 * time.Minute
	poolConfig.MaxConnIdleTime = 5 * time.Minute
	poolConfig.HealthCheckPeriod = 30 * time.Second

	connectCtx, cancel := context.WithTimeout(ctx, cfg.ConnectTimeout)
	defer cancel()
	pool, err := pgxpool.NewWithConfig(connectCtx, poolConfig)
	if err != nil {
		return nil, fmt.Errorf("connect postgres: %w", err)
	}
	if err := pool.Ping(connectCtx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping postgres: %w", err)
	}
	return &Store{pool: pool}, nil
}

func (s *Store) Close() { s.pool.Close() }

func (s *Store) Ping(ctx context.Context) error { return s.pool.Ping(ctx) }

func (s *Store) Migrate(ctx context.Context) error {
	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquire migration connection: %w", err)
	}
	defer conn.Release()
	if _, err := conn.Exec(ctx, `SELECT pg_advisory_lock(7720240921)`); err != nil {
		return fmt.Errorf("lock migrations: %w", err)
	}
	defer func() { _, _ = conn.Exec(context.Background(), `SELECT pg_advisory_unlock(7720240921)`) }()

	if _, err := conn.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS schema_migrations (
			name text PRIMARY KEY,
			applied_at timestamptz NOT NULL DEFAULT now()
		)`); err != nil {
		return fmt.Errorf("create migration table: %w", err)
	}

	entries, err := fs.ReadDir(migrationFiles, "migrations")
	if err != nil {
		return fmt.Errorf("read embedded migrations: %w", err)
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name() < entries[j].Name() })
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		var applied bool
		err := conn.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM schema_migrations WHERE name=$1)`, entry.Name()).Scan(&applied)
		if err != nil {
			return fmt.Errorf("check migration %s: %w", entry.Name(), err)
		}
		if applied {
			continue
		}
		body, err := migrationFiles.ReadFile("migrations/" + entry.Name())
		if err != nil {
			return fmt.Errorf("read migration %s: %w", entry.Name(), err)
		}
		tx, err := conn.Begin(ctx)
		if err != nil {
			return fmt.Errorf("begin migration %s: %w", entry.Name(), err)
		}
		if _, err = tx.Exec(ctx, string(body)); err != nil {
			_ = tx.Rollback(ctx)
			return fmt.Errorf("apply migration %s: %w", entry.Name(), err)
		}
		if _, err = tx.Exec(ctx, `INSERT INTO schema_migrations(name) VALUES($1)`, entry.Name()); err != nil {
			_ = tx.Rollback(ctx)
			return fmt.Errorf("record migration %s: %w", entry.Name(), err)
		}
		if err = tx.Commit(ctx); err != nil {
			return fmt.Errorf("commit migration %s: %w", entry.Name(), err)
		}
	}
	return nil
}

// CreateOrder persists an event and its business effect in one transaction.
// A unique processed_events row makes Kafka redelivery harmless.
func (s *Store) CreateOrder(ctx context.Context, event domain.OrderEvent) (bool, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return false, fmt.Errorf("begin order transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()

	result, err := tx.Exec(ctx, `
		INSERT INTO processed_events(event_id, event_type)
		VALUES ($1, $2)
		ON CONFLICT (event_id) DO NOTHING`, event.EventID, event.EventType)
	if err != nil {
		return false, fmt.Errorf("record processed event: %w", err)
	}
	if result.RowsAffected() == 0 {
		if err := tx.Commit(ctx); err != nil {
			return false, fmt.Errorf("commit duplicate event: %w", err)
		}
		return false, nil
	}

	result, err = tx.Exec(ctx, `
		INSERT INTO orders(
			order_id, event_id, user_id, product_id, quantity,
			idempotency_key, status, requested_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		ON CONFLICT DO NOTHING`,
		event.OrderID, event.EventID, event.UserID, event.ProductID,
		event.Quantity, event.IdempotencyKey, domain.OrderConfirmed, event.RequestedAt,
	)
	if err != nil {
		return false, fmt.Errorf("insert order: %w", err)
	}
	created := result.RowsAffected() == 1
	if err := tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("commit order transaction: %w", err)
	}
	return created, nil
}

func (s *Store) GetOrder(ctx context.Context, orderID string) (domain.Order, error) {
	var order domain.Order
	err := s.pool.QueryRow(ctx, `
		SELECT order_id::text, user_id, product_id, quantity, status,
		       COALESCE(failure_reason, ''), requested_at, created_at, updated_at
		FROM orders WHERE order_id=$1`, orderID).Scan(
		&order.OrderID, &order.UserID, &order.ProductID, &order.Quantity,
		&order.Status, &order.FailureReason, &order.RequestedAt,
		&order.CreatedAt, &order.UpdatedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.Order{}, ErrNotFound
	}
	if err != nil {
		return domain.Order{}, fmt.Errorf("get order: %w", err)
	}
	return order, nil
}

func (s *Store) GetProduct(ctx context.Context, productID string) (domain.Product, error) {
	var product domain.Product
	err := s.pool.QueryRow(ctx, `
		SELECT product_id, name, price_cents, initial_stock
		FROM products WHERE product_id=$1`, productID).Scan(
		&product.ID, &product.Name, &product.PriceCents, &product.InitialStock,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.Product{}, ErrNotFound
	}
	if err != nil {
		return domain.Product{}, fmt.Errorf("get product: %w", err)
	}
	return product, nil
}

func (s *Store) ListProducts(ctx context.Context) ([]domain.Product, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT product_id, name, price_cents, initial_stock
		FROM products ORDER BY product_id`)
	if err != nil {
		return nil, fmt.Errorf("list products: %w", err)
	}
	defer rows.Close()
	var products []domain.Product
	for rows.Next() {
		var product domain.Product
		if err := rows.Scan(&product.ID, &product.Name, &product.PriceCents, &product.InitialStock); err != nil {
			return nil, fmt.Errorf("scan product: %w", err)
		}
		products = append(products, product)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate products: %w", err)
	}
	return products, nil
}
