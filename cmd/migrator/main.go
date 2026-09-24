package main

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/example/flashflow/internal/config"
	"github.com/example/flashflow/internal/logging"
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
	if err := cfg.Validate("migrator"); err != nil {
		return err
	}
	log := logging.New(cfg.LogLevel).With("service", "migrator")
	ctx, stop := platform.SignalContext()
	defer stop()
	db, err := postgres.New(ctx, cfg.Database)
	if err != nil {
		return err
	}
	defer db.Close()
	if err := db.Migrate(ctx); err != nil {
		return err
	}
	cache := redisstore.New(cfg.Redis)
	defer cache.Close()
	if err := cache.Ping(ctx); err != nil {
		return fmt.Errorf("redis startup check: %w", err)
	}
	products, err := db.ListProducts(ctx)
	if err != nil {
		return err
	}
	for _, product := range products {
		if err := cache.SeedStock(ctx, product.ID, product.InitialStock, cfg.ResetStock); err != nil {
			return fmt.Errorf("seed stock for %s: %w", product.ID, err)
		}
		encoded, err := json.Marshal(product)
		if err != nil {
			return fmt.Errorf("encode product %s: %w", product.ID, err)
		}
		if err := cache.CacheProduct(ctx, product.ID, encoded); err != nil {
			return fmt.Errorf("cache product %s: %w", product.ID, err)
		}
	}
	log.Info("database migrated and inventory initialized", "products", len(products), "reset_stock", cfg.ResetStock)
	return nil
}
