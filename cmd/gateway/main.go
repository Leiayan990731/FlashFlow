package main

import (
	"fmt"
	"os"

	"github.com/example/flashflow/internal/config"
	"github.com/example/flashflow/internal/gateway"
	"github.com/example/flashflow/internal/logging"
	"github.com/example/flashflow/internal/observability"
	"github.com/example/flashflow/internal/platform"
	"github.com/example/flashflow/internal/postgres"
	"github.com/example/flashflow/internal/redisstore"
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
	if err := cfg.Validate("gateway"); err != nil {
		return err
	}
	log := logging.New(cfg.LogLevel).With("service", "gateway", "instance", cfg.InstanceID)
	ctx, stop := platform.SignalContext()
	defer stop()

	cache := redisstore.New(cfg.Redis)
	defer cache.Close()
	if err := cache.Ping(ctx); err != nil {
		return fmt.Errorf("redis startup check: %w", err)
	}
	db, err := postgres.New(ctx, cfg.Database)
	if err != nil {
		return err
	}
	defer db.Close()

	metrics := observability.New("gateway")
	handler := gateway.New(cfg.Gateway, cache, db, metrics, log).Handler()
	server := platform.NewHTTPServer(cfg.HTTPAddr, handler)
	log.Info("gateway ready")
	return platform.Serve(ctx, server, cfg.ShutdownTimeout, log)
}
