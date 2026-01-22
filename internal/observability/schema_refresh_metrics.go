package observability

import (
	"context"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// SchemaRefreshMetrics holds custom metrics for schema refresh behavior.
type SchemaRefreshMetrics struct {
	refreshCounter  metric.Int64Counter
	errorCounter    metric.Int64Counter
	durationHist    metric.Float64Histogram
	lastSuccessUnix atomic.Int64
}

// InitSchemaRefreshMetrics initializes schema refresh metrics.
func InitSchemaRefreshMetrics(logger *slog.Logger) (*SchemaRefreshMetrics, error) {
	meter := otel.Meter("tidb-graphql")

	refreshCounter, err := meter.Int64Counter(
		"schema.refresh.total",
		metric.WithDescription("Total number of schema refresh attempts"),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create schema refresh counter: %w", err)
	}

	errorCounter, err := meter.Int64Counter(
		"schema.refresh.errors.total",
		metric.WithDescription("Total number of failed schema refresh attempts"),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create schema refresh error counter: %w", err)
	}

	durationHist, err := meter.Float64Histogram(
		"schema.refresh.duration",
		metric.WithDescription("Duration of schema refresh attempts in milliseconds"),
		metric.WithUnit("ms"),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create schema refresh duration histogram: %w", err)
	}

	lastSuccessGauge, err := meter.Int64ObservableGauge(
		"schema.refresh.last_success_unix",
		metric.WithDescription("Unix timestamp of the last successful schema refresh"),
		metric.WithUnit("s"),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create schema refresh last success gauge: %w", err)
	}

	metrics := &SchemaRefreshMetrics{
		refreshCounter: refreshCounter,
		errorCounter:   errorCounter,
		durationHist:   durationHist,
	}

	_, err = meter.RegisterCallback(
		func(ctx context.Context, observer metric.Observer) error {
			value := metrics.lastSuccessUnix.Load()
			if value > 0 {
				observer.ObserveInt64(lastSuccessGauge, value)
			}
			return nil
		},
		lastSuccessGauge,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to register schema refresh gauge callback: %w", err)
	}

	logger.Info("schema refresh metrics initialized")
	return metrics, nil
}

// RecordRefresh records a schema refresh attempt.
func (m *SchemaRefreshMetrics) RecordRefresh(ctx context.Context, duration time.Duration, success bool, trigger string) {
	attrs := []attribute.KeyValue{
		attribute.String("trigger", trigger),
		attribute.Bool("success", success),
	}

	m.refreshCounter.Add(ctx, 1, metric.WithAttributes(attrs...))
	m.durationHist.Record(ctx, float64(duration.Milliseconds()), metric.WithAttributes(attrs...))

	if !success {
		m.errorCounter.Add(ctx, 1, metric.WithAttributes(attribute.String("trigger", trigger)))
		return
	}

	m.lastSuccessUnix.Store(time.Now().Unix())
}
