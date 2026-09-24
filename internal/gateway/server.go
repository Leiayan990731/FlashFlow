package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/example/flashflow/internal/config"
	"github.com/example/flashflow/internal/domain"
	"github.com/example/flashflow/internal/idgen"
	"github.com/example/flashflow/internal/observability"
	"github.com/example/flashflow/internal/postgres"
	"github.com/example/flashflow/internal/redisstore"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"
)

type Cache interface {
	Reserve(context.Context, redisstore.ReserveInput) (redisstore.ReserveOutput, error)
	Allow(context.Context, string, float64, int) (bool, error)
	GetOrder(context.Context, string) ([]byte, error)
	CacheOrder(context.Context, string, []byte) error
	GetProduct(context.Context, string) ([]byte, error)
	CacheProduct(context.Context, string, []byte) error
	Stock(context.Context, string) (int64, error)
	Ping(context.Context) error
}

type Repository interface {
	GetOrder(context.Context, string) (domain.Order, error)
	GetProduct(context.Context, string) (domain.Product, error)
	Ping(context.Context) error
}

type Server struct {
	cfg     config.Gateway
	cache   Cache
	db      Repository
	metrics *observability.Metrics
	log     *slog.Logger
	sem     chan struct{}
}

var safeID = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`)

func New(cfg config.Gateway, cache Cache, db Repository, metrics *observability.Metrics, log *slog.Logger) *Server {
	return &Server{cfg: cfg, cache: cache, db: db, metrics: metrics, log: log, sem: make(chan struct{}, cfg.MaxInFlight)}
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health/live", s.live)
	mux.HandleFunc("GET /health/ready", s.ready)
	mux.Handle("GET /metrics", s.metrics.Handler())
	mux.HandleFunc("POST /v1/orders", s.createOrder)
	mux.HandleFunc("GET /v1/orders/{orderID}", s.getOrder)
	mux.HandleFunc("GET /v1/products/{productID}", s.getProduct)
	return s.recover(s.requestID(s.observe(s.limitConcurrency(mux))))
}

type createOrderRequest struct {
	ProductID string `json:"productId"`
	Quantity  int    `json:"quantity"`
}

func (s *Server) createOrder(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), s.cfg.RequestTimeout)
	defer cancel()
	userID := strings.TrimSpace(r.Header.Get("X-User-ID"))
	idempotencyKey := strings.TrimSpace(r.Header.Get("Idempotency-Key"))
	if userID == "" || len(userID) > 128 {
		writeError(w, http.StatusUnauthorized, "invalid_user", "X-User-ID is required and must be at most 128 characters")
		return
	}
	if idempotencyKey == "" || len(idempotencyKey) > 256 {
		writeError(w, http.StatusBadRequest, "invalid_idempotency_key", "Idempotency-Key is required and must be at most 256 characters")
		return
	}

	var request createOrderRequest
	if err := decodeJSON(w, r, s.cfg.MaxBodyBytes, &request); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	if !safeID.MatchString(request.ProductID) || request.Quantity < 1 || request.Quantity > 10 {
		writeError(w, http.StatusBadRequest, "invalid_request", "productId is invalid or quantity is outside 1..10")
		return
	}

	principal := userID + "|" + clientIP(r)
	allowed, err := s.cache.Allow(ctx, principal, s.cfg.RatePerSecond, s.cfg.RateBurst)
	if err != nil {
		s.metrics.DependencyUp.WithLabelValues("redis").Set(0)
		s.log.Error("rate limiter unavailable", "error", err)
		writeError(w, http.StatusServiceUnavailable, "dependency_unavailable", "request admission is temporarily unavailable")
		return
	}
	if !allowed {
		w.Header().Set("Retry-After", "1")
		writeError(w, http.StatusTooManyRequests, "rate_limited", "request rate exceeded")
		return
	}

	orderID, err := idgen.New()
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal_error", "could not allocate order id")
		return
	}
	eventID, err := idgen.New()
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal_error", "could not allocate event id")
		return
	}
	now := time.Now().UTC()
	event := domain.OrderEvent{
		EventID: eventID, EventType: domain.EventOrderRequested, OrderID: orderID,
		UserID: userID, ProductID: request.ProductID, Quantity: request.Quantity,
		IdempotencyKey: idempotencyKey, CorrelationID: requestIDFrom(r.Context()), RequestedAt: now,
	}
	pending := domain.Order{
		OrderID: orderID, UserID: userID, ProductID: request.ProductID,
		Quantity: request.Quantity, Status: domain.OrderPending, RequestedAt: now, UpdatedAt: now,
	}
	eventJSON, _ := json.Marshal(event)
	statusJSON, _ := json.Marshal(pending)
	result, err := s.cache.Reserve(ctx, redisstore.ReserveInput{
		OrderID: orderID, UserID: userID, ProductID: request.ProductID,
		Quantity: request.Quantity, IdempotencyKey: idempotencyKey,
		EventJSON: eventJSON, StatusJSON: statusJSON,
	})
	if err != nil {
		s.metrics.DependencyUp.WithLabelValues("redis").Set(0)
		s.metrics.Reservations.WithLabelValues("error").Inc()
		s.log.Error("inventory reservation failed", "error", err, "product_id", request.ProductID)
		writeError(w, http.StatusServiceUnavailable, "dependency_unavailable", "inventory is temporarily unavailable")
		return
	}
	s.metrics.DependencyUp.WithLabelValues("redis").Set(1)
	s.metrics.Reservations.WithLabelValues(string(result.Result)).Inc()
	switch result.Result {
	case redisstore.Reserved:
		w.Header().Set("Location", "/v1/orders/"+result.OrderID)
		writeJSON(w, http.StatusAccepted, pending)
	case redisstore.Duplicate:
		s.writeExistingOrder(ctx, w, userID, result.OrderID)
	case redisstore.OutOfStock:
		writeError(w, http.StatusConflict, "out_of_stock", "the requested quantity is not available")
	case redisstore.ProductNotFound:
		writeError(w, http.StatusNotFound, "product_not_found", "product does not exist")
	default:
		writeError(w, http.StatusInternalServerError, "internal_error", "unexpected reservation result")
	}
}

func (s *Server) writeExistingOrder(ctx context.Context, w http.ResponseWriter, userID, orderID string) {
	order, err := s.lookupOrder(ctx, orderID)
	if err == nil && order.UserID == userID {
		w.Header().Set("Location", "/v1/orders/"+orderID)
		status := http.StatusOK
		if order.Status == domain.OrderPending {
			status = http.StatusAccepted
		}
		writeJSON(w, status, order)
		return
	}
	// The idempotency record is authoritative even if its short-lived status
	// cache was evicted before the worker created the database row.
	w.Header().Set("Location", "/v1/orders/"+orderID)
	writeJSON(w, http.StatusAccepted, domain.Order{OrderID: orderID, UserID: userID, Status: domain.OrderPending})
}

func (s *Server) getOrder(w http.ResponseWriter, r *http.Request) {
	userID := strings.TrimSpace(r.Header.Get("X-User-ID"))
	if userID == "" || len(userID) > 128 {
		writeError(w, http.StatusUnauthorized, "invalid_user", "X-User-ID is required")
		return
	}
	orderID := r.PathValue("orderID")
	if !safeID.MatchString(orderID) {
		writeError(w, http.StatusBadRequest, "invalid_order_id", "orderId is invalid")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), s.cfg.RequestTimeout)
	defer cancel()
	order, err := s.lookupOrder(ctx, orderID)
	if errors.Is(err, postgres.ErrNotFound) {
		writeError(w, http.StatusNotFound, "order_not_found", "order does not exist")
		return
	}
	if err != nil {
		s.log.Error("order lookup failed", "error", err, "order_id", orderID)
		writeError(w, http.StatusServiceUnavailable, "dependency_unavailable", "order lookup is temporarily unavailable")
		return
	}
	if order.UserID != userID {
		// Do not reveal whether another user's order exists.
		writeError(w, http.StatusNotFound, "order_not_found", "order does not exist")
		return
	}
	writeJSON(w, http.StatusOK, order)
}

func (s *Server) lookupOrder(ctx context.Context, orderID string) (domain.Order, error) {
	value, err := s.cache.GetOrder(ctx, orderID)
	if err == nil {
		var order domain.Order
		if json.Unmarshal(value, &order) == nil {
			return order, nil
		}
	} else if !errors.Is(err, redis.Nil) {
		s.log.Warn("order cache lookup failed; falling back to postgres", "error", err, "order_id", orderID)
	}
	order, err := s.db.GetOrder(ctx, orderID)
	if err != nil {
		return domain.Order{}, err
	}
	if encoded, marshalErr := json.Marshal(order); marshalErr == nil {
		_ = s.cache.CacheOrder(ctx, orderID, encoded)
	}
	return order, nil
}

func (s *Server) getProduct(w http.ResponseWriter, r *http.Request) {
	productID := r.PathValue("productID")
	if !safeID.MatchString(productID) {
		writeError(w, http.StatusBadRequest, "invalid_product_id", "productId is invalid")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), s.cfg.RequestTimeout)
	defer cancel()
	product, err := s.lookupProduct(ctx, productID)
	if errors.Is(err, postgres.ErrNotFound) {
		writeError(w, http.StatusNotFound, "product_not_found", "product does not exist")
		return
	}
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, "dependency_unavailable", "catalog is temporarily unavailable")
		return
	}
	stock, err := s.cache.Stock(ctx, productID)
	if errors.Is(err, redis.Nil) {
		writeError(w, http.StatusNotFound, "product_not_found", "product inventory does not exist")
		return
	}
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, "dependency_unavailable", "inventory is temporarily unavailable")
		return
	}
	product.Stock = stock
	writeJSON(w, http.StatusOK, product)
}

func (s *Server) lookupProduct(ctx context.Context, productID string) (domain.Product, error) {
	value, err := s.cache.GetProduct(ctx, productID)
	if err == nil {
		var product domain.Product
		if json.Unmarshal(value, &product) == nil {
			return product, nil
		}
	}
	product, err := s.db.GetProduct(ctx, productID)
	if err != nil {
		return domain.Product{}, err
	}
	if encoded, marshalErr := json.Marshal(product); marshalErr == nil {
		_ = s.cache.CacheProduct(ctx, productID, encoded)
	}
	return product, nil
}

func (s *Server) live(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "alive"})
}

func (s *Server) ready(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 800*time.Millisecond)
	defer cancel()
	redisErr := s.cache.Ping(ctx)
	dbErr := s.db.Ping(ctx)
	if redisErr != nil || dbErr != nil {
		if redisErr != nil {
			s.metrics.DependencyUp.WithLabelValues("redis").Set(0)
		}
		if dbErr != nil {
			s.metrics.DependencyUp.WithLabelValues("postgres").Set(0)
		}
		writeError(w, http.StatusServiceUnavailable, "not_ready", "a required dependency is unavailable")
		return
	}
	s.metrics.DependencyUp.WithLabelValues("redis").Set(1)
	s.metrics.DependencyUp.WithLabelValues("postgres").Set(1)
	writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
}

type contextKey string

const requestIDKey contextKey = "request-id"

func (s *Server) requestID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestID := strings.TrimSpace(r.Header.Get("X-Request-ID"))
		if requestID == "" || len(requestID) > 128 {
			requestID, _ = idgen.New()
		}
		w.Header().Set("X-Request-ID", requestID)
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), requestIDKey, requestID)))
	})
}

func requestIDFrom(ctx context.Context) string {
	value, _ := ctx.Value(requestIDKey).(string)
	return value
}

func (s *Server) limitConcurrency(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasPrefix(r.URL.Path, "/v1/") {
			next.ServeHTTP(w, r)
			return
		}
		select {
		case s.sem <- struct{}{}:
			s.metrics.HTTPInFlight.Inc()
			defer func() {
				<-s.sem
				s.metrics.HTTPInFlight.Dec()
			}()
			next.ServeHTTP(w, r)
		default:
			w.Header().Set("Retry-After", "1")
			writeError(w, http.StatusServiceUnavailable, "overloaded", "this instance is at its concurrency limit")
		}
	})
}

type statusWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusWriter) WriteHeader(status int) {
	if w.status != 0 {
		return
	}
	w.status = status
	w.ResponseWriter.WriteHeader(status)
}

func (w *statusWriter) Write(body []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	return w.ResponseWriter.Write(body)
}

func (s *Server) observe(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		recorder := &statusWriter{ResponseWriter: w}
		timer := prometheus.NewTimer(prometheus.ObserverFunc(func(seconds float64) {
			route := r.Pattern
			if route == "" {
				route = "unmatched"
			}
			s.metrics.HTTPDuration.WithLabelValues(route, r.Method).Observe(seconds)
		}))
		next.ServeHTTP(recorder, r)
		timer.ObserveDuration()
		status := recorder.status
		if status == 0 {
			status = http.StatusOK
		}
		route := r.Pattern
		if route == "" {
			route = "unmatched"
		}
		s.metrics.HTTPRequests.WithLabelValues(route, r.Method, strconv.Itoa(status)).Inc()
		s.log.Info("http request",
			"request_id", requestIDFrom(r.Context()), "method", r.Method,
			"path", r.URL.Path, "route", route, "status", status,
		)
	})
}

func (s *Server) recover(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if recovered := recover(); recovered != nil {
				s.log.Error("http handler panic", "panic", recovered, "request_id", requestIDFrom(r.Context()))
				writeError(w, http.StatusInternalServerError, "internal_error", "internal server error")
			}
		}()
		next.ServeHTTP(w, r)
	})
}

func decodeJSON(w http.ResponseWriter, r *http.Request, maxBytes int64, destination any) error {
	r.Body = http.MaxBytesReader(w, r.Body, maxBytes)
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		return fmt.Errorf("invalid JSON body: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return errors.New("request body must contain exactly one JSON object")
	}
	return nil
}

func clientIP(r *http.Request) string {
	// The Kubernetes ingress is expected to sanitize X-Forwarded-For.
	if forwarded := strings.TrimSpace(strings.Split(r.Header.Get("X-Forwarded-For"), ",")[0]); forwarded != "" {
		return forwarded
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err == nil {
		return host
	}
	return r.RemoteAddr
}

func writeError(w http.ResponseWriter, status int, code, message string) {
	writeJSON(w, status, map[string]any{
		"error": map[string]string{"code": code, "message": message},
	})
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
