package middleware

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"tidb-graphql/internal/gqlrequest"
	"tidb-graphql/internal/logging"

	"github.com/graphql-go/graphql/language/ast"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func TestGraphQLTracingMiddleware_SetsCanonicalAttributes(t *testing.T) {
	recorder := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSampler(sdktrace.AlwaysSample()))
	tp.RegisterSpanProcessor(recorder)
	old := otel.GetTracerProvider()
	otel.SetTracerProvider(tp)
	t.Cleanup(func() {
		_ = tp.Shutdown(context.Background())
		otel.SetTracerProvider(old)
	})

	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	handler := GraphQLTracingMiddleware()(next)

	analysis := &gqlrequest.Analysis{
		Envelope: gqlrequest.Envelope{
			Query:             "query GetUsers { users { id } }",
			DocumentSizeBytes: len("query GetUsers { users { id } }"),
		},
		RequestedOperationName: "GetUsers",
		OperationName:          "GetUsers",
		OperationType:          "query",
		OperationHash:          "abc123",
		FieldCount:             2,
		SelectionDepth:         2,
		VariableCount:          0,
		Operation:              &ast.OperationDefinition{},
	}
	meta := gqlrequest.ExecMeta{
		Role:        "app_viewer",
		Fingerprint: "fp123",
	}

	ctx := gqlrequest.WithAnalysis(context.Background(), analysis)
	ctx = gqlrequest.WithExecMeta(ctx, meta)
	req := httptest.NewRequest(http.MethodPost, "/graphql", nil).WithContext(ctx)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	var spanAttrs []attribute.KeyValue
	for _, span := range recorder.Ended() {
		if span.Name() == "graphql.execute" {
			spanAttrs = span.Attributes()
		}
	}
	if len(spanAttrs) == 0 {
		t.Fatalf("expected graphql.execute span")
	}

	assertAttrString(t, spanAttrs, "graphql.operation.name", "GetUsers")
	assertAttrString(t, spanAttrs, "graphql.operation.requested_name", "GetUsers")
	assertAttrString(t, spanAttrs, "graphql.operation.type", "query")
	assertAttrString(t, spanAttrs, "graphql.operation.hash", "abc123")
	assertAttrString(t, spanAttrs, "auth.role", "app_viewer")
	assertAttrString(t, spanAttrs, "schema.fingerprint", "fp123")
}

func TestGraphQLTracingMiddleware_UsesRequestedNameWhenUnresolved(t *testing.T) {
	recorder := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSampler(sdktrace.AlwaysSample()))
	tp.RegisterSpanProcessor(recorder)
	old := otel.GetTracerProvider()
	otel.SetTracerProvider(tp)
	t.Cleanup(func() {
		_ = tp.Shutdown(context.Background())
		otel.SetTracerProvider(old)
	})

	handler := GraphQLTracingMiddleware()(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	analysis := &gqlrequest.Analysis{
		Envelope: gqlrequest.Envelope{
			Query: "query Bad(",
		},
		RequestedOperationName: "BadQuery",
	}
	ctx := gqlrequest.WithAnalysis(context.Background(), analysis)
	req := httptest.NewRequest(http.MethodPost, "/graphql", nil).WithContext(ctx)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	var spanAttrs []attribute.KeyValue
	for _, span := range recorder.Ended() {
		if span.Name() == "graphql.execute" {
			spanAttrs = span.Attributes()
		}
	}
	if len(spanAttrs) == 0 {
		t.Fatalf("expected graphql.execute span")
	}
	assertAttrString(t, spanAttrs, "graphql.operation.requested_name", "BadQuery")
	assertNoAttr(t, spanAttrs, "graphql.operation.name")
}

func TestGraphQLTracingMiddleware_SkipsWhenNoGraphQLQuery(t *testing.T) {
	recorder := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSampler(sdktrace.AlwaysSample()))
	tp.RegisterSpanProcessor(recorder)
	old := otel.GetTracerProvider()
	otel.SetTracerProvider(tp)
	t.Cleanup(func() {
		_ = tp.Shutdown(context.Background())
		otel.SetTracerProvider(old)
	})

	handler := GraphQLTracingMiddleware()(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))

	req := httptest.NewRequest(http.MethodPost, "/graphql", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	for _, span := range recorder.Ended() {
		if span.Name() == "graphql.execute" {
			t.Fatalf("did not expect graphql.execute span when query is absent")
		}
	}
}

func TestGraphQLTracingMiddleware_LoggerIncludesInnerSpanID(t *testing.T) {
	recorder := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSampler(sdktrace.AlwaysSample()))
	tp.RegisterSpanProcessor(recorder)
	old := otel.GetTracerProvider()
	otel.SetTracerProvider(tp)
	t.Cleanup(func() {
		_ = tp.Shutdown(context.Background())
		otel.SetTracerProvider(old)
	})

	var buf bytes.Buffer
	baseLogger := &logging.Logger{
		Logger: slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo})),
	}

	handler := GraphQLTracingMiddleware()(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		logging.FromContext(r.Context()).Info("inside-graphql")
		w.WriteHeader(http.StatusOK)
	}))

	analysis := &gqlrequest.Analysis{
		Envelope: gqlrequest.Envelope{
			Query: "query Q { users { id } }",
		},
		RequestedOperationName: "Q",
		OperationName:          "Q",
		OperationType:          "query",
		Operation:              &ast.OperationDefinition{},
	}
	ctx := gqlrequest.WithAnalysis(context.Background(), analysis)
	ctx = logging.WithLogger(ctx, baseLogger)

	req := httptest.NewRequest(http.MethodPost, "/graphql", nil).WithContext(ctx)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if len(lines) == 0 || lines[0] == "" {
		t.Fatalf("expected log output")
	}

	var logRecord map[string]any
	if err := json.Unmarshal([]byte(lines[0]), &logRecord); err != nil {
		t.Fatalf("failed to decode log JSON: %v", err)
	}
	if _, ok := logRecord["trace_id"]; !ok {
		t.Fatalf("expected trace_id field on log record")
	}
	if _, ok := logRecord["span_id"]; !ok {
		t.Fatalf("expected span_id field on log record")
	}
}

func assertAttrString(t *testing.T, attrs []attribute.KeyValue, key, want string) {
	t.Helper()
	for _, attr := range attrs {
		if string(attr.Key) == key {
			if got := attr.Value.AsString(); got != want {
				t.Fatalf("%s = %q, want %q", key, got, want)
			}
			return
		}
	}
	t.Fatalf("missing attribute %s", key)
}

func assertNoAttr(t *testing.T, attrs []attribute.KeyValue, key string) {
	t.Helper()
	for _, attr := range attrs {
		if string(attr.Key) == key {
			t.Fatalf("did not expect attribute %s", key)
		}
	}
}
