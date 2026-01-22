package middleware

import (
	"fmt"
	"net/http"
	"sync"
	"time"
)

// RateLimitConfig configures a global token bucket limiter.
type RateLimitConfig struct {
	Enabled bool
	RPS     float64
	Burst   int
}

// RateLimitMiddleware enforces a global rate limit for all requests through the handler.
func RateLimitMiddleware(cfg RateLimitConfig) func(http.Handler) http.Handler {
	if !cfg.Enabled {
		return func(next http.Handler) http.Handler {
			return next
		}
	}

	limiter := newTokenBucket(cfg.RPS, cfg.Burst)

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if !limiter.Allow() {
				w.Header().Set("Content-Type", "application/json")
				w.Header().Set("Retry-After", "1")
				w.WriteHeader(http.StatusTooManyRequests)
				_, _ = fmt.Fprint(w, `{"error":"rate limit exceeded"}`)
				return
			}

			next.ServeHTTP(w, r)
		})
	}
}

type tokenBucket struct {
	mu     sync.Mutex
	rate   float64
	burst  float64
	tokens float64
	last   time.Time
}

func newTokenBucket(rps float64, burst int) *tokenBucket {
	if rps <= 0 || burst <= 0 {
		return &tokenBucket{rate: 0, burst: 0, tokens: 0, last: time.Now()}
	}

	now := time.Now()
	return &tokenBucket{
		rate:   rps,
		burst: float64(burst),
		tokens: float64(burst),
		last:   now,
	}
}

func (b *tokenBucket) Allow() bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.rate <= 0 || b.burst <= 0 {
		return true
	}

	now := time.Now()
	elapsed := now.Sub(b.last).Seconds()
	if elapsed > 0 {
		b.tokens = minFloat(b.burst, b.tokens+elapsed*b.rate)
		b.last = now
	}

	if b.tokens < 1 {
		return false
	}

	b.tokens -= 1
	return true
}

func minFloat(a, b float64) float64 {
	if a < b {
		return a
	}
	return b
}
