package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	LogLevel        string
	InstanceID      string
	HTTPAddr        string
	AdminAddr       string
	ShutdownTimeout time.Duration
	Redis           Redis
	Kafka           Kafka
	Database        Database
	Gateway         Gateway
	Relay           Relay
	Worker          Worker
	ResetStock      bool
}

type Redis struct {
	Addrs            []string
	MasterName       string
	Username         string
	Password         string
	SentinelPassword string
	DB               int
	PoolSize         int
	MinIdleConns     int
	DialTimeout      time.Duration
	OperationTimeout time.Duration
	StreamName       string
	StreamGroup      string
	StreamMaxLen     int64
	ReservationTTL   time.Duration
	OrderCacheTTL    time.Duration
	IdempotencyTTL   time.Duration
	WaitReplicas     int
	WaitTimeout      time.Duration
}

type Kafka struct {
	Brokers         []string
	OrderTopic      string
	DeadLetterTopic string
	ClientID        string
	PublishTimeout  time.Duration
	MinBytes        int
	MaxBytes        int
	MaxWait         time.Duration
}

type Database struct {
	URL            string
	MaxConns       int
	MinConns       int
	ConnectTimeout time.Duration
}

type Gateway struct {
	MaxBodyBytes   int64
	MaxInFlight    int
	RatePerSecond  float64
	RateBurst      int
	RequestTimeout time.Duration
}

type Relay struct {
	Concurrency int
	BatchSize   int64
	Block       time.Duration
	ClaimIdle   time.Duration
}

type Worker struct {
	Concurrency        int
	ConsumerGroup      string
	MaxProcessAttempts int
	RetryBase          time.Duration
	RetryMax           time.Duration
}

func Load() (Config, error) {
	var errs []error
	getInt := func(name string, fallback int) int {
		value, err := intEnv(name, fallback)
		if err != nil {
			errs = append(errs, err)
			return fallback
		}
		return value
	}
	getInt64 := func(name string, fallback int64) int64 {
		value, err := int64Env(name, fallback)
		if err != nil {
			errs = append(errs, err)
			return fallback
		}
		return value
	}
	getFloat := func(name string, fallback float64) float64 {
		value, err := floatEnv(name, fallback)
		if err != nil {
			errs = append(errs, err)
			return fallback
		}
		return value
	}
	getDuration := func(name string, fallback time.Duration) time.Duration {
		value, err := durationEnv(name, fallback)
		if err != nil {
			errs = append(errs, err)
			return fallback
		}
		return value
	}
	getBool := func(name string, fallback bool) bool {
		value, err := boolEnv(name, fallback)
		if err != nil {
			errs = append(errs, err)
			return fallback
		}
		return value
	}

	cfg := Config{
		LogLevel:        stringEnv("LOG_LEVEL", "info"),
		InstanceID:      stringEnv("INSTANCE_ID", hostname()),
		HTTPAddr:        stringEnv("HTTP_ADDR", ":8080"),
		AdminAddr:       stringEnv("ADMIN_ADDR", ":9090"),
		ShutdownTimeout: getDuration("SHUTDOWN_TIMEOUT", 20*time.Second),
		Redis: Redis{
			Addrs:            csvEnv("REDIS_ADDRS", "redis:6379"),
			MasterName:       strings.TrimSpace(os.Getenv("REDIS_MASTER_NAME")),
			Username:         strings.TrimSpace(os.Getenv("REDIS_USERNAME")),
			Password:         os.Getenv("REDIS_PASSWORD"),
			SentinelPassword: os.Getenv("REDIS_SENTINEL_PASSWORD"),
			DB:               getInt("REDIS_DB", 0),
			PoolSize:         getInt("REDIS_POOL_SIZE", 200),
			MinIdleConns:     getInt("REDIS_MIN_IDLE_CONNS", 20),
			DialTimeout:      getDuration("REDIS_DIAL_TIMEOUT", 2*time.Second),
			OperationTimeout: getDuration("REDIS_OPERATION_TIMEOUT", 800*time.Millisecond),
			StreamName:       stringEnv("REDIS_STREAM", "flashflow:orders"),
			StreamGroup:      stringEnv("REDIS_STREAM_GROUP", "order-relays"),
			StreamMaxLen:     getInt64("REDIS_STREAM_MAX_LEN", 0),
			ReservationTTL:   getDuration("RESERVATION_TTL", 30*24*time.Hour),
			OrderCacheTTL:    getDuration("ORDER_CACHE_TTL", 24*time.Hour),
			IdempotencyTTL:   getDuration("IDEMPOTENCY_TTL", 24*time.Hour),
			WaitReplicas:     getInt("REDIS_WAIT_REPLICAS", 0),
			WaitTimeout:      getDuration("REDIS_WAIT_TIMEOUT", 500*time.Millisecond),
		},
		Kafka: Kafka{
			Brokers:         csvEnv("KAFKA_BROKERS", "kafka:9092"),
			OrderTopic:      stringEnv("KAFKA_ORDER_TOPIC", "flashflow.order.requested"),
			DeadLetterTopic: stringEnv("KAFKA_DLQ_TOPIC", "flashflow.order.dlq"),
			ClientID:        stringEnv("KAFKA_CLIENT_ID", "flashflow"),
			PublishTimeout:  getDuration("KAFKA_PUBLISH_TIMEOUT", 5*time.Second),
			MinBytes:        getInt("KAFKA_MIN_BYTES", 1),
			MaxBytes:        getInt("KAFKA_MAX_BYTES", 10<<20),
			MaxWait:         getDuration("KAFKA_MAX_WAIT", 500*time.Millisecond),
		},
		Database: Database{
			URL:            stringEnv("POSTGRES_URL", "postgres://flashflow:flashflow-local-password@postgres:5432/flashflow?sslmode=disable"),
			MaxConns:       getInt("POSTGRES_MAX_CONNS", 80),
			MinConns:       getInt("POSTGRES_MIN_CONNS", 10),
			ConnectTimeout: getDuration("POSTGRES_CONNECT_TIMEOUT", 5*time.Second),
		},
		Gateway: Gateway{
			MaxBodyBytes:   getInt64("HTTP_MAX_BODY_BYTES", 1<<20),
			MaxInFlight:    getInt("HTTP_MAX_IN_FLIGHT", 2000),
			RatePerSecond:  getFloat("RATE_LIMIT_PER_SECOND", 20),
			RateBurst:      getInt("RATE_LIMIT_BURST", 40),
			RequestTimeout: getDuration("HTTP_REQUEST_TIMEOUT", 3*time.Second),
		},
		Relay: Relay{
			Concurrency: getInt("RELAY_CONCURRENCY", 8),
			BatchSize:   getInt64("RELAY_BATCH_SIZE", 100),
			Block:       getDuration("RELAY_BLOCK", time.Second),
			ClaimIdle:   getDuration("RELAY_CLAIM_IDLE", 60*time.Second),
		},
		Worker: Worker{
			Concurrency:        getInt("WORKER_CONCURRENCY", 4),
			ConsumerGroup:      stringEnv("KAFKA_CONSUMER_GROUP", "order-workers"),
			MaxProcessAttempts: getInt("WORKER_MAX_PROCESS_ATTEMPTS", 8),
			RetryBase:          getDuration("WORKER_RETRY_BASE", 100*time.Millisecond),
			RetryMax:           getDuration("WORKER_RETRY_MAX", 10*time.Second),
		},
		ResetStock: getBool("RESET_STOCK", false),
	}
	return cfg, errors.Join(errs...)
}

func (c Config) Validate(role string) error {
	var problems []string
	require := func(ok bool, message string) {
		if !ok {
			problems = append(problems, message)
		}
	}
	require(len(c.Redis.Addrs) > 0, "REDIS_ADDRS must contain at least one address")
	require(c.Redis.PoolSize > 0, "REDIS_POOL_SIZE must be positive")
	require(c.Redis.StreamMaxLen >= 0, "REDIS_STREAM_MAX_LEN cannot be negative")
	require(c.Redis.WaitReplicas >= 0, "REDIS_WAIT_REPLICAS cannot be negative")
	switch role {
	case "gateway":
		require(c.Gateway.MaxInFlight > 0, "HTTP_MAX_IN_FLIGHT must be positive")
		require(c.Gateway.RatePerSecond > 0, "RATE_LIMIT_PER_SECOND must be positive")
		require(c.Gateway.RateBurst > 0, "RATE_LIMIT_BURST must be positive")
		require(c.Database.URL != "", "POSTGRES_URL is required")
	case "relay":
		require(len(c.Kafka.Brokers) > 0, "KAFKA_BROKERS is required")
		require(c.Relay.Concurrency > 0, "RELAY_CONCURRENCY must be positive")
	case "worker":
		require(len(c.Kafka.Brokers) > 0, "KAFKA_BROKERS is required")
		require(c.Database.URL != "", "POSTGRES_URL is required")
		require(c.Worker.Concurrency > 0, "WORKER_CONCURRENCY must be positive")
		require(c.Worker.MaxProcessAttempts > 0, "WORKER_MAX_PROCESS_ATTEMPTS must be positive")
	case "migrator":
		require(c.Database.URL != "", "POSTGRES_URL is required")
	default:
		problems = append(problems, "unknown service role: "+role)
	}
	if len(problems) > 0 {
		return errors.New(strings.Join(problems, "; "))
	}
	return nil
}

func stringEnv(name, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(name)); value != "" {
		return value
	}
	return fallback
}

func csvEnv(name, fallback string) []string {
	parts := strings.Split(stringEnv(name, fallback), ",")
	result := make([]string, 0, len(parts))
	for _, part := range parts {
		if value := strings.TrimSpace(part); value != "" {
			result = append(result, value)
		}
	}
	return result
}

func intEnv(name string, fallback int) (int, error) {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return fallback, nil
	}
	value, err := strconv.Atoi(raw)
	if err != nil {
		return fallback, fmt.Errorf("%s must be an integer: %w", name, err)
	}
	return value, nil
}

func int64Env(name string, fallback int64) (int64, error) {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return fallback, nil
	}
	value, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return fallback, fmt.Errorf("%s must be an integer: %w", name, err)
	}
	return value, nil
}

func floatEnv(name string, fallback float64) (float64, error) {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return fallback, nil
	}
	value, err := strconv.ParseFloat(raw, 64)
	if err != nil {
		return fallback, fmt.Errorf("%s must be a number: %w", name, err)
	}
	return value, nil
}

func durationEnv(name string, fallback time.Duration) (time.Duration, error) {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return fallback, nil
	}
	value, err := time.ParseDuration(raw)
	if err != nil {
		return fallback, fmt.Errorf("%s must be a duration: %w", name, err)
	}
	return value, nil
}

func boolEnv(name string, fallback bool) (bool, error) {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return fallback, nil
	}
	value, err := strconv.ParseBool(raw)
	if err != nil {
		return fallback, fmt.Errorf("%s must be a boolean: %w", name, err)
	}
	return value, nil
}

func hostname() string {
	name, err := os.Hostname()
	if err != nil || strings.TrimSpace(name) == "" {
		return "unknown-instance"
	}
	return name
}
