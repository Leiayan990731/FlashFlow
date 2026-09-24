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
	"github.com/example/flashflow/internal/redisstore"
	"github.com/example/flashflow/internal/relay"
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
	if err := cfg.Validate("relay"); err != nil {
		return err
	}
	log := logging.New(cfg.LogLevel).With("service", "relay", "instance", cfg.InstanceID)
	ctx, stop := platform.SignalContext()
	defer stop()
	stream := redisstore.New(cfg.Redis)
	defer stream.Close()
	if err := stream.Ping(ctx); err != nil {
		return fmt.Errorf("redis startup check: %w", err)
	}
	publisher := messagebus.NewPublisher(cfg.Kafka)
	defer publisher.Close()
	metrics := observability.New("relay")
	readiness := &platform.Readiness{}
	runner := relay.New(cfg.Relay, cfg.Kafka, cfg.InstanceID, stream, publisher, metrics, readiness, log)
	admin := platform.AdminServer(cfg.AdminAddr, metrics.Handler(), readiness)
	return platform.RunServices(ctx,
		runner.Run,
		func(serviceCtx context.Context) error {
			return platform.Serve(serviceCtx, admin, cfg.ShutdownTimeout, log)
		},
	)
}
