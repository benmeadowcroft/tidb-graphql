package observability

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// GraphQLMetrics holds custom metrics for GraphQL operations
type GraphQLMetrics struct {
	requestDuration   metric.Float64Histogram
	requestCounter    metric.Int64Counter
	errorCounter      metric.Int64Counter
	activeRequests    metric.Int64UpDownCounter
	queryDepth        metric.Int64Histogram
	resultsCount      metric.Int64Histogram
}

// InitGraphQLMetrics initializes GraphQL-specific metrics
func InitGraphQLMetrics() (*GraphQLMetrics, error) {
	meter := otel.Meter("tidb-graphql")

	requestDuration, err := meter.Float64Histogram(
		"graphql.request.duration",
		metric.WithDescription("Duration of GraphQL requests in milliseconds"),
		metric.WithUnit("ms"),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create request duration histogram: %w", err)
	}

	requestCounter, err := meter.Int64Counter(
		"graphql.requests.total",
		metric.WithDescription("Total number of GraphQL requests"),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create request counter: %w", err)
	}

	errorCounter, err := meter.Int64Counter(
		"graphql.errors.total",
		metric.WithDescription("Total number of GraphQL errors"),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create error counter: %w", err)
	}

	activeRequests, err := meter.Int64UpDownCounter(
		"graphql.requests.active",
		metric.WithDescription("Number of active GraphQL requests"),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create active requests counter: %w", err)
	}

	queryDepth, err := meter.Int64Histogram(
		"graphql.query.depth",
		metric.WithDescription("Depth of GraphQL queries"),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create query depth histogram: %w", err)
	}

	resultsCount, err := meter.Int64Histogram(
		"graphql.results.count",
		metric.WithDescription("Number of results returned by GraphQL queries"),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create results count histogram: %w", err)
	}

	return &GraphQLMetrics{
		requestDuration: requestDuration,
		requestCounter:  requestCounter,
		errorCounter:    errorCounter,
		activeRequests:  activeRequests,
		queryDepth:      queryDepth,
		resultsCount:    resultsCount,
	}, nil
}

// RecordRequest records a GraphQL request with its duration and outcome
func (m *GraphQLMetrics) RecordRequest(ctx context.Context, duration time.Duration, hasErrors bool, operationType string) {
	attrs := []attribute.KeyValue{
		attribute.String("operation_type", operationType),
		attribute.Bool("has_errors", hasErrors),
	}

	// Record duration in milliseconds
	m.requestDuration.Record(ctx, float64(duration.Milliseconds()), metric.WithAttributes(attrs...))

	// Increment total request counter
	m.requestCounter.Add(ctx, 1, metric.WithAttributes(attrs...))

	// Increment error counter if there were errors
	if hasErrors {
		m.errorCounter.Add(ctx, 1, metric.WithAttributes(
			attribute.String("operation_type", operationType),
		))
	}
}

// RecordQueryDepth records the depth of a GraphQL query
func (m *GraphQLMetrics) RecordQueryDepth(ctx context.Context, depth int64, operationType string) {
	m.queryDepth.Record(ctx, depth, metric.WithAttributes(
		attribute.String("operation_type", operationType),
	))
}

// RecordResultsCount records the number of results returned
func (m *GraphQLMetrics) RecordResultsCount(ctx context.Context, count int64, operationType string) {
	m.resultsCount.Record(ctx, count, metric.WithAttributes(
		attribute.String("operation_type", operationType),
	))
}

// IncrementActiveRequests increments the active requests counter
func (m *GraphQLMetrics) IncrementActiveRequests(ctx context.Context) {
	m.activeRequests.Add(ctx, 1)
}

// DecrementActiveRequests decrements the active requests counter
func (m *GraphQLMetrics) DecrementActiveRequests(ctx context.Context) {
	m.activeRequests.Add(ctx, -1)
}

// InitMetrics initializes all custom metrics and returns the GraphQLMetrics instance
func InitMetrics(logger *slog.Logger) (*GraphQLMetrics, error) {
	metrics, err := InitGraphQLMetrics()
	if err != nil {
		return nil, fmt.Errorf("failed to initialize GraphQL metrics: %w", err)
	}

	logger.Info("custom GraphQL metrics initialized")
	return metrics, nil
}
