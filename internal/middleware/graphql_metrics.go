package middleware

import (
	"bytes"
	"encoding/json"
	"net/http"
	"strings"
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

			ctx := observability.ContextWithGraphQLMetrics(r.Context(), metrics)
			r = r.WithContext(ctx)

			// Increment active requests
			metrics.IncrementActiveRequests(ctx)
			defer metrics.DecrementActiveRequests(ctx)

			// Record start time
			start := time.Now()

			operationType := "unknown"
			query, operationName := extractGraphQLRequest(r)
			metadata, err := extractQueryMetadata(query, operationName)
			if err == nil && metadata != nil && strings.TrimSpace(metadata.operationType) != "" {
				operationType = metadata.operationType
			}

			// Wrap response writer to capture response
			wrapped := &metricsResponseWriter{
				ResponseWriter: w,
				statusCode:     http.StatusOK,
			}

			// Serve the request
			next.ServeHTTP(wrapped, r)

			// Calculate duration
			duration := time.Since(start)

			// Determine if there were errors.
			hasErrors := wrapped.statusCode >= 400 || responseHasGraphQLErrors(wrapped.body.Bytes())

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
	body       bytes.Buffer
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
	if len(b) > 0 {
		_, _ = w.body.Write(b)
	}
	return w.ResponseWriter.Write(b)
}

func responseHasGraphQLErrors(body []byte) bool {
	trimmed := bytes.TrimSpace(body)
	if len(trimmed) == 0 {
		return false
	}

	var payload struct {
		Errors json.RawMessage `json:"errors"`
	}
	if err := json.Unmarshal(trimmed, &payload); err != nil {
		return false
	}
	if len(payload.Errors) == 0 {
		return false
	}

	errorsValue := bytes.TrimSpace(payload.Errors)
	if len(errorsValue) == 0 || bytes.Equal(errorsValue, []byte("null")) {
		return false
	}

	var errorsList []json.RawMessage
	if err := json.Unmarshal(errorsValue, &errorsList); err != nil {
		return false
	}
	return len(errorsList) > 0
}
