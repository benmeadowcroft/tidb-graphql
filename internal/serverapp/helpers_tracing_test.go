package serverapp

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"tidb-graphql/internal/config"

	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func TestWrapHTTPHandler_UsesHTTPRootSpanName(t *testing.T) {
	recorder := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSampler(sdktrace.AlwaysSample()))
	tp.RegisterSpanProcessor(recorder)
	originalTP := otel.GetTracerProvider()
	otel.SetTracerProvider(tp)
	t.Cleanup(func() {
		_ = tp.Shutdown(context.Background())
		otel.SetTracerProvider(originalTP)
	})

	cfg := &config.Config{
		Observability: config.ObservabilityConfig{
			TracingEnabled: true,
		},
	}
	handler := wrapHTTPHandler(cfg, testLogger(), http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusNoContent {
		t.Fatalf("expected status %d, got %d", http.StatusNoContent, rec.Code)
	}

	for _, span := range recorder.Ended() {
		if span.Name() == "GET /health" {
			return
		}
	}
	t.Fatalf("expected GET /health span")
}

func TestNormalizeHTTPSpanRoute(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{name: "graphql", input: "/graphql", expected: "/graphql"},
		{name: "health", input: "/health", expected: "/health"},
		{name: "metrics", input: "/metrics", expected: "/metrics"},
		{name: "admin", input: "/admin/reload-schema", expected: "/admin/reload-schema"},
		{name: "root", input: "/", expected: "/"},
		{name: "unknown", input: "/users/123", expected: "/*"},
		{name: "empty", input: "", expected: "/*"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := normalizeHTTPSpanRoute(tt.input)
			if got != tt.expected {
				t.Fatalf("normalizeHTTPSpanRoute(%q) = %q, want %q", tt.input, got, tt.expected)
			}
		})
	}
}
