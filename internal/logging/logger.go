// Package logging provides structured logging helpers for the server.
package logging

import (
	"context"
	"log/slog"
	"os"

	"go.opentelemetry.io/contrib/bridges/otelslog"
	"go.opentelemetry.io/otel/sdk/log"
)

type contextKey string

const (
	loggerKey    contextKey = "logger"
	requestIDKey contextKey = "request_id"
)

// Logger wraps slog.Logger with convenience methods
type Logger struct {
	*slog.Logger
}

// Config holds logging configuration
type Config struct {
	Level          string              // debug, info, warn, error
	Format         string              // json, text
	LoggerProvider *log.LoggerProvider // Optional OTLP logger provider for exporting logs
}

// NewLogger creates a new structured logger based on configuration
func NewLogger(cfg Config) *Logger {
	var level slog.Level
	switch cfg.Level {
	case "debug":
		level = slog.LevelDebug
	case "info":
		level = slog.LevelInfo
	case "warn":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	default:
		level = slog.LevelInfo
	}

	opts := &slog.HandlerOptions{
		Level: level,
		// Add source location for error and above
		AddSource: level <= slog.LevelError,
	}

	var handler slog.Handler

	// If OTLP logger provider is provided, use otelslog bridge with multi-handler
	if cfg.LoggerProvider != nil {
		// Create base handler (stdout) for local viewing
		var stdoutHandler slog.Handler
		if cfg.Format == "json" {
			stdoutHandler = slog.NewJSONHandler(os.Stdout, opts)
		} else {
			stdoutHandler = slog.NewTextHandler(os.Stdout, opts)
		}

		// Create OTLP handler using otelslog bridge
		otlpHandler := otelslog.NewHandler("tidb-graphql", otelslog.WithLoggerProvider(cfg.LoggerProvider))

		// Wrap both handlers in a multi-handler that writes to both stdout and OTLP
		handler = newMultiHandler(stdoutHandler, otlpHandler)
	} else {
		// No OTLP - just use stdout handler
		if cfg.Format == "json" {
			handler = slog.NewJSONHandler(os.Stdout, opts)
		} else {
			handler = slog.NewTextHandler(os.Stdout, opts)
		}
	}

	return &Logger{
		Logger: slog.New(handler),
	}
}

// multiHandler wraps multiple slog handlers to write to multiple destinations
type multiHandler struct {
	handlers []slog.Handler
}

func newMultiHandler(handlers ...slog.Handler) *multiHandler {
	return &multiHandler{handlers: handlers}
}

func (m *multiHandler) Enabled(ctx context.Context, level slog.Level) bool {
	// Enable if any handler is enabled
	for _, h := range m.handlers {
		if h.Enabled(ctx, level) {
			return true
		}
	}
	return false
}

func (m *multiHandler) Handle(ctx context.Context, record slog.Record) error {
	// Write to all handlers
	for _, h := range m.handlers {
		if h.Enabled(ctx, record.Level) {
			if err := h.Handle(ctx, record); err != nil {
				return err
			}
		}
	}
	return nil
}

func (m *multiHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	newHandlers := make([]slog.Handler, len(m.handlers))
	for i, h := range m.handlers {
		newHandlers[i] = h.WithAttrs(attrs)
	}
	return &multiHandler{handlers: newHandlers}
}

func (m *multiHandler) WithGroup(name string) slog.Handler {
	newHandlers := make([]slog.Handler, len(m.handlers))
	for i, h := range m.handlers {
		newHandlers[i] = h.WithGroup(name)
	}
	return &multiHandler{handlers: newHandlers}
}

// WithRequestID returns a new logger with the request ID field attached
func (l *Logger) WithRequestID(requestID string) *Logger {
	return &Logger{
		Logger: l.With(slog.String("request_id", requestID)),
	}
}

// WithFields returns a new logger with additional fields
func (l *Logger) WithFields(fields ...any) *Logger {
	return &Logger{
		Logger: l.With(fields...),
	}
}

// FromContext retrieves the logger from context, or returns a default logger
func FromContext(ctx context.Context) *Logger {
	if logger, ok := ctx.Value(loggerKey).(*Logger); ok {
		return logger
	}
	// Return default logger if not found in context
	return &Logger{
		Logger: slog.Default(),
	}
}

// WithLogger adds a logger to the context
func WithLogger(ctx context.Context, logger *Logger) context.Context {
	return context.WithValue(ctx, loggerKey, logger)
}

// GetRequestID retrieves the request ID from context
func GetRequestID(ctx context.Context) string {
	if requestID, ok := ctx.Value(requestIDKey).(string); ok {
		return requestID
	}
	return ""
}

// WithRequestIDContext adds a request ID to the context
func WithRequestIDContext(ctx context.Context, requestID string) context.Context {
	return context.WithValue(ctx, requestIDKey, requestID)
}
