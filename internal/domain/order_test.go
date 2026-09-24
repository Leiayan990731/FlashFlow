package domain

import (
	"testing"
	"time"
)

func TestOrderEventValidate(t *testing.T) {
	valid := OrderEvent{
		EventID: "ac4c2f48-e20e-4ad0-89cc-c70ad9b1452d", EventType: EventOrderRequested,
		OrderID: "2cfb8730-e2bc-45ce-942b-fc18624d43bb",
		UserID:  "user", ProductID: "product", Quantity: 1,
		IdempotencyKey: "idem", RequestedAt: time.Now(),
	}
	if err := valid.Validate(); err != nil {
		t.Fatalf("valid event rejected: %v", err)
	}

	invalid := valid
	invalid.Quantity = 11
	if err := invalid.Validate(); err == nil {
		t.Fatal("quantity above the API limit was accepted")
	}
}
