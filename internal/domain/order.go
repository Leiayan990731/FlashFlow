package domain

import (
	"errors"
	"regexp"
	"strings"
	"time"
)

var uuidPattern = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[1-5][0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}$`)

const (
	EventOrderRequested = "order.requested"

	OrderPending   = "PENDING"
	OrderConfirmed = "CONFIRMED"
	OrderRejected  = "REJECTED"
)

type OrderEvent struct {
	EventID        string    `json:"eventId"`
	EventType      string    `json:"eventType"`
	OrderID        string    `json:"orderId"`
	UserID         string    `json:"userId"`
	ProductID      string    `json:"productId"`
	Quantity       int       `json:"quantity"`
	IdempotencyKey string    `json:"idempotencyKey"`
	CorrelationID  string    `json:"correlationId,omitempty"`
	RequestedAt    time.Time `json:"requestedAt"`
}

func (e OrderEvent) Validate() error {
	switch {
	case e.EventType != EventOrderRequested:
		return errors.New("unsupported event type")
	case !uuidPattern.MatchString(e.EventID):
		return errors.New("eventId must be a UUID")
	case !uuidPattern.MatchString(e.OrderID):
		return errors.New("orderId must be a UUID")
	case strings.TrimSpace(e.UserID) == "":
		return errors.New("userId is required")
	case strings.TrimSpace(e.ProductID) == "":
		return errors.New("productId is required")
	case e.Quantity < 1 || e.Quantity > 10:
		return errors.New("quantity must be between 1 and 10")
	case strings.TrimSpace(e.IdempotencyKey) == "":
		return errors.New("idempotencyKey is required")
	case e.RequestedAt.IsZero():
		return errors.New("requestedAt is required")
	default:
		return nil
	}
}

type Order struct {
	OrderID       string    `json:"orderId"`
	UserID        string    `json:"userId"`
	ProductID     string    `json:"productId"`
	Quantity      int       `json:"quantity"`
	Status        string    `json:"status"`
	FailureReason string    `json:"failureReason,omitempty"`
	RequestedAt   time.Time `json:"requestedAt"`
	CreatedAt     time.Time `json:"createdAt,omitempty"`
	UpdatedAt     time.Time `json:"updatedAt"`
}

type Product struct {
	ID           string `json:"id"`
	Name         string `json:"name"`
	PriceCents   int64  `json:"priceCents"`
	InitialStock int64  `json:"-"`
	Stock        int64  `json:"stock"`
}
