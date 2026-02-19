package observability

import (
	"context"
	"log/slog"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

func TestInitMeterProvider(t *testing.T) {
	cfg := Config{
		ServiceName:    "test-service",
		ServiceVersion: "1.0.0",
		Environment:    "test",
	}

	mp, err := InitMeterProvider(cfg)
	require.NoError(t, err, "Should initialize meter provider without error")
	require.NotNil(t, mp, "Meter provider should not be nil")
	require.NotNil(t, mp.provider, "Provider should not be nil")
	require.NotNil(t, mp.exporter, "Exporter should not be nil")

	// Clean up
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	err = mp.Shutdown(context.Background(), logger)
	assert.NoError(t, err, "Should shutdown without error")
}

func TestInitMetrics(t *testing.T) {
	// First initialize meter provider
	cfg := Config{
		ServiceName:    "test-service",
		ServiceVersion: "1.0.0",
		Environment:    "test",
	}

	mp, err := InitMeterProvider(cfg)
	require.NoError(t, err)
	defer func() {
		logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
		mp.Shutdown(context.Background(), logger)
	}()

	// Initialize metrics
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	metrics, err := InitMetrics(logger)
	require.NoError(t, err, "Should initialize metrics without error")
	require.NotNil(t, metrics, "Metrics should not be nil")

	// Verify all metrics are initialized
	require.NotNil(t, metrics.requestDuration, "Request duration metric should be initialized")
	require.NotNil(t, metrics.requestCounter, "Request counter should be initialized")
	require.NotNil(t, metrics.errorCounter, "Error counter should be initialized")
	require.NotNil(t, metrics.activeRequests, "Active requests counter should be initialized")
}

func TestBuildTLSConfig_FileNotFound(t *testing.T) {
	// Missing CA file should surface a clear error.
	_, err := buildTLSConfig(OTLPExporterConfig{
		TLSCertFile: "/nonexistent/ca.pem",
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to read OTLP TLS CA file")
}

func TestBuildTLSConfig_InvalidCertFormat(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/ca.pem"

	// Write a non-PEM payload to trigger parse failure.
	require.NoError(t, os.WriteFile(path, []byte("not-a-cert"), 0600))

	_, err := buildTLSConfig(OTLPExporterConfig{
		TLSCertFile: path,
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to parse OTLP TLS CA file")
}

func TestBuildTLSConfig_MissingClientKeyPair(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/client.crt"

	// Only set the cert path to ensure missing key is rejected.
	require.NoError(t, os.WriteFile(path, []byte("not-a-cert"), 0600))

	_, err := buildTLSConfig(OTLPExporterConfig{
		TLSClientCertFile: path,
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "OTLP TLS client cert and key must both be set")
}

func TestTraceSamplerForRatio_Boundaries(t *testing.T) {
	never := traceSamplerForRatio(0)
	always := traceSamplerForRatio(1)

	decisionNever := never.ShouldSample(sdktrace.SamplingParameters{
		ParentContext: context.Background(),
		TraceID:       trace.TraceID{1},
		Name:          "test",
	}).Decision
	assert.Equal(t, sdktrace.Drop, decisionNever)

	decisionAlways := always.ShouldSample(sdktrace.SamplingParameters{
		ParentContext: context.Background(),
		TraceID:       trace.TraceID{2},
		Name:          "test",
	}).Decision
	assert.Equal(t, sdktrace.RecordAndSample, decisionAlways)
}

func TestTraceSamplerForRatio_ParentAwareMidRange(t *testing.T) {
	sampler := traceSamplerForRatio(0.5)

	parentSampled := trace.ContextWithSpanContext(context.Background(), trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    trace.TraceID{3},
		SpanID:     trace.SpanID{1},
		TraceFlags: trace.FlagsSampled,
		Remote:     true,
	}))
	decisionSampledParent := sampler.ShouldSample(sdktrace.SamplingParameters{
		ParentContext: parentSampled,
		TraceID:       trace.TraceID{4},
		Name:          "child",
	}).Decision
	assert.Equal(t, sdktrace.RecordAndSample, decisionSampledParent)

	parentNotSampled := trace.ContextWithSpanContext(context.Background(), trace.NewSpanContext(trace.SpanContextConfig{
		TraceID: trace.TraceID{5},
		SpanID:  trace.SpanID{2},
		Remote:  true,
	}))
	decisionUnsampledParent := sampler.ShouldSample(sdktrace.SamplingParameters{
		ParentContext: parentNotSampled,
		TraceID:       trace.TraceID{6},
		Name:          "child",
	}).Decision
	assert.Equal(t, sdktrace.Drop, decisionUnsampledParent)
}
