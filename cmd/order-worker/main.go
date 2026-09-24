package main

import (
	"context"
	"fmt"
	"os"

	"github.com/example/flashflow/internal/config"
	"github.com/example/flashflow/internal/logging"
	"github.com/example/flashflow/internal/messagebus"
	"github.com/example/flashflow/internal/observability"
	"github.com/example/flashflow/internal/platform"
	"github.com/example/flashflow/internal/postgres"
	"github.com/example/flashflow/internal/redisstore"
	"github.com/example/flashflow/internal/worker"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	if err := cfg.Validate("worker"); err != nil {
		return err
	}
	log := logging.New(cfg.LogLevel).With("service", "order-worker", "instance", cfg.InstanceID)
	ctx, stop := platform.SignalContext()
	defer stop()
	db, err := postgres.New(ctx, cfg.Database)
	if err != nil {
		return err
	}
	defer db.Close()
	cache := redisstore.New(cfg.Redis)
	defer cache.Close()
	if err := cache.Ping(ctx); err != nil {
		return fmt.Errorf("redis startup check: %w", err)
	}
	publisher := messagebus.NewPublisher(cfg.Kafka)
	defer publisher.Close()
	metrics := observability.New("order_worker")
	readiness := &platform.Readiness{}
	runner := worker.New(cfg.Worker, cfg.Kafka, db, cache, publisher, metrics, readiness, log)
	admin := platform.AdminServer(cfg.AdminAddr, metrics.Handler(), readiness)
	return platform.RunServices(ctx,
		runner.Run,
		func(serviceCtx context.Context) error {
			return platform.Serve(serviceCtx, admin, cfg.ShutdownTimeout, log)
		},
	)
}
