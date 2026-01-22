// Package middleware applies cross-cutting HTTP policies like auth, roles, and logging.
// See docs/explanation/middleware-architecture.md.
package middleware

import (
	"log/slog"
	"net/http"
	"time"

	"tidb-graphql/internal/logging"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// RequestIDHeader is the HTTP header name for request IDs
const RequestIDHeader = "X-Request-ID"

// LoggingMiddleware wraps an HTTP handler with request logging and correlation IDs
func LoggingMiddleware(logger *logging.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()

			// Extract or generate request ID
			requestID := r.Header.Get(RequestIDHeader)
			if requestID == "" {
				requestID = uuid.New().String()
			}

			// Add request ID to response header for traceability
			w.Header().Set(RequestIDHeader, requestID)

			// Create request-scoped logger with request ID
			reqLogger := logger.WithRequestID(requestID).WithFields(slog.String("component", "http"))

			// Add logger and request ID to context
			ctx := logging.WithLogger(r.Context(), reqLogger)
			ctx = logging.WithRequestIDContext(ctx, requestID)

			span := trace.SpanFromContext(ctx)
			if span.SpanContext().IsValid() {
				span.SetAttributes(attribute.String("http.request_id", requestID))
			}

			// Wrap response writer to capture status code
			wrapped := &responseWriter{
				ResponseWriter: w,
				statusCode:     http.StatusOK,
			}

			// Log request start
			reqLogger.Log(ctx, slog.LevelInfo, "request started",
				slog.String("method", r.Method),
				slog.String("path", r.URL.Path),
				slog.String("remote_addr", r.RemoteAddr),
			)

			// Call next handler
			next.ServeHTTP(wrapped, r.WithContext(ctx))

			// Log request completion
			duration := time.Since(start)
			logLevel := slog.LevelInfo
			if wrapped.statusCode >= 500 {
				logLevel = slog.LevelError
			} else if wrapped.statusCode >= 400 {
				logLevel = slog.LevelWarn
			}

			reqLogger.Log(r.Context(), logLevel, "request completed",
				slog.String("method", r.Method),
				slog.String("path", r.URL.Path),
				slog.Int("status", wrapped.statusCode),
				slog.Duration("duration", duration),
				slog.Int64("duration_ms", duration.Milliseconds()),
			)
		})
	}
}

// responseWriter wraps http.ResponseWriter to capture status code
type responseWriter struct {
	http.ResponseWriter
	statusCode int
	written    bool
}

func (rw *responseWriter) WriteHeader(statusCode int) {
	if !rw.written {
		rw.statusCode = statusCode
		rw.written = true
		rw.ResponseWriter.WriteHeader(statusCode)
	}
}

func (rw *responseWriter) Write(b []byte) (int, error) {
	if !rw.written {
		rw.WriteHeader(http.StatusOK)
	}
	return rw.ResponseWriter.Write(b)
}
