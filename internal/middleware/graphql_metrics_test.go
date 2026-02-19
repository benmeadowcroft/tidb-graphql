package middleware

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"tidb-graphql/internal/observability"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

func TestGraphQLMetricsMiddleware_OperationTypeMutation(t *testing.T) {
	handler, reader := setupGraphQLMetricsMiddleware(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"data":{"createUser":{"id":"1"}}}`))
	}))

	req := httptest.NewRequest(http.MethodPost, "/graphql", strings.NewReader(`{"query":"mutation CreateUser { createUser(input: {}) { id } }","operationName":"CreateUser"}`))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	rm := collectMetrics(t, reader)
	if got := sumInt64Value(rm, "graphql.requests.total", "mutation", boolPtr(false)); got != 1 {
		t.Fatalf("graphql.requests.total mutation=false = %d, want 1", got)
	}
}

func TestGraphQLMetricsMiddleware_OperationTypeSubscription(t *testing.T) {
	handler, reader := setupGraphQLMetricsMiddleware(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"data":{"userUpdated":{"id":"1"}}}`))
	}))

	req := httptest.NewRequest(http.MethodPost, "/graphql", strings.NewReader(`{"query":"subscription OnUserUpdated { userUpdated { id } }","operationName":"OnUserUpdated"}`))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	rm := collectMetrics(t, reader)
	if got := sumInt64Value(rm, "graphql.requests.total", "subscription", boolPtr(false)); got != 1 {
		t.Fatalf("graphql.requests.total subscription=false = %d, want 1", got)
	}
}

func TestGraphQLMetricsMiddleware_HTTP200WithGraphQLErrors(t *testing.T) {
	handler, reader := setupGraphQLMetricsMiddleware(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"errors":[{"message":"boom"}]}`))
	}))

	req := httptest.NewRequest(http.MethodPost, "/graphql", strings.NewReader(`{"query":"query { users { id } }"}`))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	rm := collectMetrics(t, reader)
	if got := sumInt64Value(rm, "graphql.requests.total", "query", boolPtr(true)); got != 1 {
		t.Fatalf("graphql.requests.total query=true = %d, want 1", got)
	}
	if got := sumInt64Value(rm, "graphql.errors.total", "query", nil); got != 1 {
		t.Fatalf("graphql.errors.total query = %d, want 1", got)
	}
}

func TestGraphQLMetricsMiddleware_FallbackToUnknownOperationType(t *testing.T) {
	handler, reader := setupGraphQLMetricsMiddleware(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"data":{"ok":true}}`))
	}))

	req := httptest.NewRequest(http.MethodPost, "/graphql", strings.NewReader(`{"query":`))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	rm := collectMetrics(t, reader)
	if got := sumInt64Value(rm, "graphql.requests.total", "unknown", boolPtr(false)); got != 1 {
		t.Fatalf("graphql.requests.total unknown=false = %d, want 1", got)
	}
}

func setupGraphQLMetricsMiddleware(t *testing.T, next http.Handler) (http.Handler, *sdkmetric.ManualReader) {
	t.Helper()

	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	oldProvider := otel.GetMeterProvider()
	otel.SetMeterProvider(provider)
	t.Cleanup(func() {
		_ = provider.Shutdown(context.Background())
		otel.SetMeterProvider(oldProvider)
	})

	metrics, err := observability.InitGraphQLMetrics()
	if err != nil {
		t.Fatalf("failed to initialize GraphQL metrics: %v", err)
	}
	return GraphQLMetricsMiddleware(metrics)(next), reader
}

func collectMetrics(t *testing.T, reader *sdkmetric.ManualReader) metricdata.ResourceMetrics {
	t.Helper()

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("failed to collect metrics: %v", err)
	}
	return rm
}

func sumInt64Value(rm metricdata.ResourceMetrics, metricName, operationType string, hasErrors *bool) int64 {
	var total int64
	for _, scope := range rm.ScopeMetrics {
		for _, metric := range scope.Metrics {
			if metric.Name != metricName {
				continue
			}
			sum, ok := metric.Data.(metricdata.Sum[int64])
			if !ok {
				continue
			}
			for _, point := range sum.DataPoints {
				if !matchOperation(point.Attributes, operationType) {
					continue
				}
				if hasErrors != nil && !matchHasErrors(point.Attributes, *hasErrors) {
					continue
				}
				total += point.Value
			}
		}
	}
	return total
}

func matchOperation(attrs attribute.Set, operationType string) bool {
	for _, kv := range attrs.ToSlice() {
		if string(kv.Key) == "operation_type" {
			return kv.Value.AsString() == operationType
		}
	}
	return false
}

func matchHasErrors(attrs attribute.Set, hasErrors bool) bool {
	for _, kv := range attrs.ToSlice() {
		if string(kv.Key) == "has_errors" {
			return kv.Value.AsBool() == hasErrors
		}
	}
	return false
}

func boolPtr(v bool) *bool {
	return &v
}
