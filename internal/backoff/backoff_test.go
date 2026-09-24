package backoff

import (
	"testing"
	"time"
)

func TestDurationIsCapped(t *testing.T) {
	for i := 0; i < 100; i++ {
		d := Duration(30, 10*time.Millisecond, time.Second)
		if d < 0 || d > time.Second {
			t.Fatalf("duration outside bounds: %v", d)
		}
	}
}
