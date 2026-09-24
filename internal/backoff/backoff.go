package backoff

import (
	"math/rand/v2"
	"time"
)

func Duration(attempt int, base, maximum time.Duration) time.Duration {
	if attempt < 0 {
		attempt = 0
	}
	d := base
	for i := 0; i < attempt && d < maximum/2; i++ {
		d *= 2
	}
	if d > maximum {
		d = maximum
	}
	if d <= 1 {
		return d
	}
	// Full jitter prevents synchronized retry storms after a dependency outage.
	return time.Duration(rand.Int64N(int64(d)))
}
