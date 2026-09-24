package redisstore

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/example/flashflow/internal/config"
	"github.com/redis/go-redis/v9"
)

type ReservationResult string

const (
	Reserved        ReservationResult = "reserved"
	Duplicate       ReservationResult = "duplicate"
	OutOfStock      ReservationResult = "out_of_stock"
	ProductNotFound ReservationResult = "product_not_found"
)

type ReserveInput struct {
	OrderID        string
	UserID         string
	ProductID      string
	Quantity       int
	IdempotencyKey string
	EventJSON      []byte
	StatusJSON     []byte
}

type ReserveOutput struct {
	Result  ReservationResult
	OrderID string
}

type StreamMessage struct {
	ID    string
	Event []byte
}

type Store struct {
	client         *redis.Client
	opTimeout      time.Duration
	stream         string
	group          string
	streamMaxLen   int64
	reservationTTL time.Duration
	orderCacheTTL  time.Duration
	idempotencyTTL time.Duration
	waitReplicas   int
	waitTimeout    time.Duration
}

var reserveScript = redis.NewScript(`
local existing = redis.call('GET', KEYS[2])
if existing then
  return {2, existing}
end

local raw_stock = redis.call('GET', KEYS[1])
if not raw_stock then
  return {-2, ''}
end

local quantity = tonumber(ARGV[1])
local stock = tonumber(raw_stock)
if not quantity or quantity <= 0 then
  return {-3, ''}
end
if stock < quantity then
  return {-1, ''}
end

redis.call('DECRBY', KEYS[1], quantity)
redis.call('SET', KEYS[2], ARGV[2], 'PX', ARGV[4])
redis.call('SET', KEYS[3], ARGV[3], 'PX', ARGV[5])
local max_len = tonumber(ARGV[6])
if max_len and max_len > 0 then
  redis.call('XADD', KEYS[4], 'MAXLEN', '~', max_len, '*', 'event', ARGV[7])
else
  redis.call('XADD', KEYS[4], '*', 'event', ARGV[7])
end
return {1, ARGV[2]}
`)

var compensateScript = redis.NewScript(`
if redis.call('EXISTS', KEYS[1]) == 0 then
  return -1
end
local first = redis.call('SET', KEYS[2], '1', 'NX', 'PX', ARGV[2])
if not first then
  return 0
end
redis.call('INCRBY', KEYS[1], ARGV[1])
redis.call('SET', KEYS[3], ARGV[3], 'PX', ARGV[4])
return 1
`)

var rateLimitScript = redis.NewScript(`
local values = redis.call('HMGET', KEYS[1], 'tokens', 'updated')
local tokens = tonumber(values[1])
local updated = tonumber(values[2])
local now = tonumber(ARGV[1])
local rate = tonumber(ARGV[2])
local burst = tonumber(ARGV[3])
if not tokens then tokens = burst end
if not updated then updated = now end
local elapsed = math.max(0, now - updated)
tokens = math.min(burst, tokens + (elapsed * rate / 1000.0))
local allowed = 0
if tokens >= 1 then
  tokens = tokens - 1
  allowed = 1
end
redis.call('HSET', KEYS[1], 'tokens', tokens, 'updated', now)
redis.call('PEXPIRE', KEYS[1], ARGV[4])
return {allowed, math.floor(tokens)}
`)

func New(cfg config.Redis) *Store {
	common := func(options *redis.Options) {
		options.Username = cfg.Username
		options.Password = cfg.Password
		options.DB = cfg.DB
		options.PoolSize = cfg.PoolSize
		options.MinIdleConns = cfg.MinIdleConns
		options.DialTimeout = cfg.DialTimeout
		options.ReadTimeout = 3 * time.Second
		options.WriteTimeout = 3 * time.Second
		options.MaxRetries = 2
		options.MinRetryBackoff = 20 * time.Millisecond
		options.MaxRetryBackoff = 200 * time.Millisecond
	}

	var client *redis.Client
	if cfg.MasterName != "" {
		client = redis.NewFailoverClient(&redis.FailoverOptions{
			MasterName:       cfg.MasterName,
			SentinelAddrs:    cfg.Addrs,
			SentinelPassword: cfg.SentinelPassword,
			Username:         cfg.Username,
			Password:         cfg.Password,
			DB:               cfg.DB,
			PoolSize:         cfg.PoolSize,
			MinIdleConns:     cfg.MinIdleConns,
			DialTimeout:      cfg.DialTimeout,
			ReadTimeout:      3 * time.Second,
			WriteTimeout:     3 * time.Second,
			MaxRetries:       2,
			MinRetryBackoff:  20 * time.Millisecond,
			MaxRetryBackoff:  200 * time.Millisecond,
		})
	} else {
		options := &redis.Options{Addr: cfg.Addrs[0]}
		common(options)
		client = redis.NewClient(options)
	}

	return &Store{
		client: client, opTimeout: cfg.OperationTimeout,
		stream: cfg.StreamName, group: cfg.StreamGroup,
		streamMaxLen: cfg.StreamMaxLen, reservationTTL: cfg.ReservationTTL,
		orderCacheTTL: cfg.OrderCacheTTL, idempotencyTTL: cfg.IdempotencyTTL,
		waitReplicas: cfg.WaitReplicas, waitTimeout: cfg.WaitTimeout,
	}
}

func (s *Store) Close() error { return s.client.Close() }

func (s *Store) Ping(ctx context.Context) error {
	opCtx, cancel := context.WithTimeout(ctx, s.opTimeout)
	defer cancel()
	return s.client.Ping(opCtx).Err()
}

func (s *Store) Reserve(ctx context.Context, input ReserveInput) (ReserveOutput, error) {
	opCtx, cancel := context.WithTimeout(ctx, s.opTimeout)
	defer cancel()
	connection := s.client.Conn()
	defer connection.Close()

	keys := []string{
		stockKey(input.ProductID),
		idempotencyKey(input.UserID, input.IdempotencyKey),
		orderKey(input.OrderID),
		s.stream,
	}
	result, err := reserveScript.Run(opCtx, connection, keys,
		input.Quantity,
		input.OrderID,
		string(input.StatusJSON),
		s.idempotencyTTL.Milliseconds(),
		s.orderCacheTTL.Milliseconds(),
		s.streamMaxLen,
		string(input.EventJSON),
	).Slice()
	if err != nil {
		return ReserveOutput{}, fmt.Errorf("reserve inventory: %w", err)
	}
	if len(result) != 2 {
		return ReserveOutput{}, fmt.Errorf("reserve inventory: unexpected script response %#v", result)
	}
	code, err := integer(result[0])
	if err != nil {
		return ReserveOutput{}, fmt.Errorf("reserve inventory response: %w", err)
	}
	orderID := text(result[1])
	if (code == 1 || code == 2) && s.waitReplicas > 0 {
		replicas, waitErr := connection.Wait(opCtx, s.waitReplicas, s.waitTimeout).Result()
		if waitErr != nil {
			return ReserveOutput{}, fmt.Errorf("wait for redis replicas: %w", waitErr)
		}
		if replicas < int64(s.waitReplicas) {
			return ReserveOutput{}, fmt.Errorf("wait for redis replicas: acknowledged by %d of %d", replicas, s.waitReplicas)
		}
	}
	switch code {
	case 1:
		return ReserveOutput{Result: Reserved, OrderID: orderID}, nil
	case 2:
		return ReserveOutput{Result: Duplicate, OrderID: orderID}, nil
	case -1:
		return ReserveOutput{Result: OutOfStock}, nil
	case -2:
		return ReserveOutput{Result: ProductNotFound}, nil
	default:
		return ReserveOutput{}, fmt.Errorf("reserve inventory: unexpected result code %d", code)
	}
}

func (s *Store) Allow(ctx context.Context, principal string, rate float64, burst int) (bool, error) {
	opCtx, cancel := context.WithTimeout(ctx, s.opTimeout)
	defer cancel()
	ttl := time.Duration(float64(time.Second) * float64(burst) / rate * 2).Milliseconds()
	if ttl < time.Second.Milliseconds() {
		ttl = time.Second.Milliseconds()
	}
	result, err := rateLimitScript.Run(opCtx, s.client, []string{rateKey(principal)},
		time.Now().UnixMilli(), rate, burst, ttl).Slice()
	if err != nil {
		return false, fmt.Errorf("distributed rate limit: %w", err)
	}
	if len(result) == 0 {
		return false, errors.New("distributed rate limit: empty response")
	}
	allowed, err := integer(result[0])
	return allowed == 1, err
}

func (s *Store) GetOrder(ctx context.Context, orderID string) ([]byte, error) {
	opCtx, cancel := context.WithTimeout(ctx, s.opTimeout)
	defer cancel()
	value, err := s.client.Get(opCtx, orderKey(orderID)).Bytes()
	if err != nil {
		return nil, err
	}
	return value, nil
}

func (s *Store) CacheOrder(ctx context.Context, orderID string, value []byte) error {
	opCtx, cancel := context.WithTimeout(ctx, s.opTimeout)
	defer cancel()
	return s.client.Set(opCtx, orderKey(orderID), value, s.orderCacheTTL).Err()
}

func (s *Store) GetProduct(ctx context.Context, productID string) ([]byte, error) {
	opCtx, cancel := context.WithTimeout(ctx, s.opTimeout)
	defer cancel()
	return s.client.Get(opCtx, productKey(productID)).Bytes()
}

func (s *Store) CacheProduct(ctx context.Context, productID string, value []byte) error {
	opCtx, cancel := context.WithTimeout(ctx, s.opTimeout)
	defer cancel()
	return s.client.Set(opCtx, productKey(productID), value, 12*time.Hour).Err()
}

func (s *Store) Stock(ctx context.Context, productID string) (int64, error) {
	opCtx, cancel := context.WithTimeout(ctx, s.opTimeout)
	defer cancel()
	return s.client.Get(opCtx, stockKey(productID)).Int64()
}

func (s *Store) SeedStock(ctx context.Context, productID string, stock int64, reset bool) error {
	opCtx, cancel := context.WithTimeout(ctx, s.opTimeout)
	defer cancel()
	if reset {
		return s.client.Set(opCtx, stockKey(productID), stock, 0).Err()
	}
	return s.client.SetNX(opCtx, stockKey(productID), stock, 0).Err()
}

func (s *Store) Compensate(ctx context.Context, orderID, productID string, quantity int, statusJSON []byte) (bool, error) {
	opCtx, cancel := context.WithTimeout(ctx, s.opTimeout)
	defer cancel()
	result, err := compensateScript.Run(opCtx, s.client,
		[]string{stockKey(productID), compensationKey(orderID), orderKey(orderID)},
		quantity, s.reservationTTL.Milliseconds(), string(statusJSON), s.orderCacheTTL.Milliseconds(),
	).Int64()
	if err != nil {
		return false, fmt.Errorf("compensate inventory: %w", err)
	}
	return result == 1, nil
}

func (s *Store) EnsureGroup(ctx context.Context) error {
	opCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	err := s.client.XGroupCreateMkStream(opCtx, s.stream, s.group, "0-0").Err()
	if err != nil && !strings.Contains(err.Error(), "BUSYGROUP") {
		return fmt.Errorf("create stream consumer group: %w", err)
	}
	return nil
}

func (s *Store) ReadGroup(ctx context.Context, consumer string, count int64, block time.Duration) ([]StreamMessage, error) {
	streams, err := s.client.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group: s.group, Consumer: consumer,
		Streams: []string{s.stream, ">"}, Count: count, Block: block,
	}).Result()
	if errors.Is(err, redis.Nil) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return convertMessages(streams), nil
}

func (s *Store) ClaimStale(ctx context.Context, consumer string, minIdle time.Duration, count int64) ([]StreamMessage, error) {
	messages, _, err := s.client.XAutoClaim(ctx, &redis.XAutoClaimArgs{
		Stream: s.stream, Group: s.group, Consumer: consumer,
		MinIdle: minIdle, Start: "0-0", Count: count,
	}).Result()
	if errors.Is(err, redis.Nil) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	result := make([]StreamMessage, 0, len(messages))
	for _, message := range messages {
		value := message.Values["event"]
		result = append(result, StreamMessage{ID: message.ID, Event: []byte(text(value))})
	}
	return result, nil
}

func (s *Store) Ack(ctx context.Context, ids ...string) error {
	if len(ids) == 0 {
		return nil
	}
	opCtx, cancel := context.WithTimeout(ctx, s.opTimeout)
	defer cancel()
	pipeline := s.client.TxPipeline()
	pipeline.XAck(opCtx, s.stream, s.group, ids...)
	pipeline.XDel(opCtx, s.stream, ids...)
	_, err := pipeline.Exec(opCtx)
	return err
}

func convertMessages(streams []redis.XStream) []StreamMessage {
	var result []StreamMessage
	for _, stream := range streams {
		for _, message := range stream.Messages {
			value := message.Values["event"]
			result = append(result, StreamMessage{ID: message.ID, Event: []byte(text(value))})
		}
	}
	return result
}

func integer(value any) (int64, error) {
	switch typed := value.(type) {
	case int64:
		return typed, nil
	case int:
		return int64(typed), nil
	case string:
		return strconv.ParseInt(typed, 10, 64)
	case []byte:
		return strconv.ParseInt(string(typed), 10, 64)
	default:
		return 0, fmt.Errorf("expected integer, got %T", value)
	}
}

func text(value any) string {
	switch typed := value.(type) {
	case string:
		return typed
	case []byte:
		return string(typed)
	default:
		return fmt.Sprint(value)
	}
}

func digest(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}

func stockKey(productID string) string      { return "ff:stock:" + productID }
func productKey(productID string) string    { return "ff:product:" + productID }
func orderKey(orderID string) string        { return "ff:order:" + orderID }
func compensationKey(orderID string) string { return "ff:compensated:" + orderID }
func rateKey(principal string) string       { return "ff:rate:" + digest(principal) }
func idempotencyKey(userID, key string) string {
	return "ff:idem:" + digest(userID+"\x00"+key)
}
