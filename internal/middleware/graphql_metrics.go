package middleware

import (
	"net/http"
	"time"

	"tidb-graphql/internal/observability"
)

// GraphQLMetricsMiddleware wraps a GraphQL handler and records metrics
func GraphQLMetricsMiddleware(metrics *observability.GraphQLMetrics) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Skip metrics for non-POST requests (GraphiQL page loads, etc.)
			if r.Method != http.MethodPost {
				next.ServeHTTP(w, r)
				return
			}

			ctx := r.Context()

			// Increment active requests
			metrics.IncrementActiveRequests(ctx)
			defer metrics.DecrementActiveRequests(ctx)

			// Record start time
			start := time.Now()

			// Wrap response writer to capture response
			wrapped := &metricsResponseWriter{
				ResponseWriter: w,
				statusCode:     http.StatusOK,
			}

			// Serve the request
			next.ServeHTTP(wrapped, r)

			// Calculate duration
			duration := time.Since(start)

			// Determine if there were errors (status >= 400 indicates errors)
			hasErrors := wrapped.statusCode >= 400

			// For now, we'll mark all operations as "query" since we can't easily
			// parse the operation type without modifying the GraphQL handler
			// In a production system, you might use a custom GraphQL executor
			operationType := "query"

			// Record the request metrics
			metrics.RecordRequest(ctx, duration, hasErrors, operationType)
		})
	}
}

// metricsResponseWriter wraps http.ResponseWriter to capture status code
type metricsResponseWriter struct {
	http.ResponseWriter
	statusCode int
	written    bool
}

func (w *metricsResponseWriter) WriteHeader(statusCode int) {
	if !w.written {
		w.statusCode = statusCode
		w.written = true
		w.ResponseWriter.WriteHeader(statusCode)
	}
}

func (w *metricsResponseWriter) Write(b []byte) (int, error) {
	if !w.written {
		w.WriteHeader(http.StatusOK)
	}
	return w.ResponseWriter.Write(b)
}

// Note: Additional GraphQL-specific metrics like query depth and results count
// would require deeper integration with the GraphQL execution pipeline.
// The current middleware provides basic HTTP-level metrics for GraphQL operations.
