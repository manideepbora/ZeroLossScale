package autoscale

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"time"
)

// Retry executes fn up to maxAttempts times with exponential backoff and jitter.
// Respects context cancellation. Returns the last error if all attempts fail.
func Retry(ctx context.Context, name string, maxAttempts int, baseDelay time.Duration, fn func() error) error {
	var lastErr error
	delay := baseDelay

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		lastErr = fn()
		if lastErr == nil {
			return nil
		}

		if attempt == maxAttempts {
			break
		}

		// Add jitter: 75%-125% of delay.
		jitter := time.Duration(float64(delay) * (0.75 + rand.Float64()*0.5))
		if jitter > 30*time.Second {
			jitter = 30 * time.Second
		}

		log.Printf("[retry] %s attempt %d/%d failed: %v (retrying in %v)", name, attempt, maxAttempts, lastErr, jitter)

		select {
		case <-ctx.Done():
			return fmt.Errorf("%s: %w (after %d attempts, last error: %v)", name, ctx.Err(), attempt, lastErr)
		case <-time.After(jitter):
		}

		delay *= 2
	}

	return fmt.Errorf("%s: all %d attempts failed: %w", name, maxAttempts, lastErr)
}
