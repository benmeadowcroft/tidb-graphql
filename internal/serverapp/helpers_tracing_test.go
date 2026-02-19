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
		if span.Name() == "tidb-graphql-http" {
			return
		}
	}
	t.Fatalf("expected tidb-graphql-http span")
}
