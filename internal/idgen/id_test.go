package idgen

import (
	"regexp"
	"testing"
)

func TestNewUUIDv4(t *testing.T) {
	a, err := New()
	if err != nil {
		t.Fatal(err)
	}
	b, err := New()
	if err != nil {
		t.Fatal(err)
	}
	if a == b {
		t.Fatal("generated duplicate identifiers")
	}
	pattern := regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)
	if !pattern.MatchString(a) {
		t.Fatalf("invalid UUID v4: %q", a)
	}
}
