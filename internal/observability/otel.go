// Package observability provides OpenTelemetry integration for metrics, tracing, and logging.
// It supports OTLP exporters (gRPC and HTTP) for traces and logs, and Prometheus for metrics.
package observability

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploggrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploghttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/sdk/log"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"google.golang.org/grpc/credentials"
)

// Config holds OpenTelemetry configuration
type Config struct {
	ServiceName      string
	ServiceVersion   string
	Environment      string
	TraceSampleRatio float64
	OTLPConfig       OTLPExporterConfig
}

// OTLPExporterConfig holds OTLP exporter configuration options
type OTLPExporterConfig struct {
	Endpoint          string
	Protocol          string
	Insecure          bool
	TLSCertFile       string
	TLSClientCertFile string
	TLSClientKeyFile  string
	Headers           map[string]string
	Timeout           time.Duration
	Compression       string
	RetryEnabled      bool
	RetryMaxAttempts  int
}

// MeterProvider wraps the OpenTelemetry meter provider
type MeterProvider struct {
	provider *metric.MeterProvider
	exporter *prometheus.Exporter
}

// InitMeterProvider initializes OpenTelemetry metrics with Prometheus exporter
func InitMeterProvider(cfg Config) (*MeterProvider, error) {
	// Create resource with service information (without schema URL to avoid conflicts)
	res, err := resource.Merge(
		resource.Default(),
		resource.NewWithAttributes(
			"",
			attribute.String("service.name", cfg.ServiceName),
			attribute.String("service.version", cfg.ServiceVersion),
			attribute.String("deployment.environment", cfg.Environment),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create resource: %w", err)
	}

	// Create Prometheus exporter
	exporter, err := prometheus.New()
	if err != nil {
		return nil, fmt.Errorf("failed to create prometheus exporter: %w", err)
	}

	// Create meter provider with Prometheus exporter
	provider := metric.NewMeterProvider(
		metric.WithResource(res),
		metric.WithReader(exporter),
	)

	// Set global meter provider
	otel.SetMeterProvider(provider)

	return &MeterProvider{
		provider: provider,
		exporter: exporter,
	}, nil
}

// Shutdown gracefully shuts down the meter provider
func (mp *MeterProvider) Shutdown(ctx context.Context, logger *slog.Logger) error {
	shutdownCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	if err := mp.provider.Shutdown(shutdownCtx); err != nil {
		logger.Error("failed to shutdown meter provider", slog.String("error", err.Error()))
		return err
	}

	logger.Info("meter provider shutdown successfully")
	return nil
}

// Exporter returns the Prometheus exporter for metrics HTTP handler
func (mp *MeterProvider) Exporter() *prometheus.Exporter {
	return mp.exporter
}

// TracerProvider wraps the OpenTelemetry tracer provider
type TracerProvider struct {
	provider *sdktrace.TracerProvider
}

type otlpProtocol string

const (
	otlpProtocolGRPC otlpProtocol = "grpc"
	otlpProtocolHTTP otlpProtocol = "http/protobuf"
)

func parseOTLPProtocol(value string) (otlpProtocol, error) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "", string(otlpProtocolGRPC):
		return otlpProtocolGRPC, nil
	case "http", string(otlpProtocolHTTP):
		return otlpProtocolHTTP, nil
	default:
		return "", fmt.Errorf("unsupported OTLP protocol %q (use grpc or http/protobuf)", value)
	}
}

func buildTLSConfig(cfg OTLPExporterConfig) (*tls.Config, error) {
	tlsConfig := &tls.Config{MinVersion: tls.VersionTLS12}

	// Load CA certificate for server verification
	if cfg.TLSCertFile != "" {
		certPool := x509.NewCertPool()
		caCert, err := os.ReadFile(cfg.TLSCertFile)
		if err != nil {
			return nil, fmt.Errorf("failed to read OTLP TLS CA file: %w", err)
		}
		if !certPool.AppendCertsFromPEM(caCert) {
			return nil, fmt.Errorf("failed to parse OTLP TLS CA file")
		}
		tlsConfig.RootCAs = certPool
	}

	// Load client certificate for mTLS
	if cfg.TLSClientCertFile != "" || cfg.TLSClientKeyFile != "" {
		if cfg.TLSClientCertFile == "" || cfg.TLSClientKeyFile == "" {
			return nil, fmt.Errorf("OTLP TLS client cert and key must both be set")
		}
		cert, err := tls.LoadX509KeyPair(cfg.TLSClientCertFile, cfg.TLSClientKeyFile)
		if err != nil {
			return nil, fmt.Errorf("failed to load OTLP TLS client certificate: %w", err)
		}
		tlsConfig.Certificates = []tls.Certificate{cert}
	}

	return tlsConfig, nil
}

func isHTTPEndpointURL(endpoint string) bool {
	return strings.HasPrefix(endpoint, "http://") || strings.HasPrefix(endpoint, "https://")
}

// buildTracerExporterOptions builds OTLP trace exporter options from config
func buildTracerExporterOptions(cfg OTLPExporterConfig) ([]otlptracegrpc.Option, error) {
	opts := []otlptracegrpc.Option{
		otlptracegrpc.WithEndpoint(cfg.Endpoint),
	}

	// TLS Configuration
	if cfg.Insecure {
		opts = append(opts, otlptracegrpc.WithInsecure())
	} else {
		// Configure TLS credentials
		tlsConfig, err := buildTLSConfig(cfg)
		if err != nil {
			return nil, err
		}
		opts = append(opts, otlptracegrpc.WithTLSCredentials(credentials.NewTLS(tlsConfig)))
	}

	// Headers
	if len(cfg.Headers) > 0 {
		opts = append(opts, otlptracegrpc.WithHeaders(cfg.Headers))
	}

	// Timeout
	if cfg.Timeout > 0 {
		opts = append(opts, otlptracegrpc.WithTimeout(cfg.Timeout))
	}

	// Compression
	if cfg.Compression == "gzip" {
		opts = append(opts, otlptracegrpc.WithCompressor("gzip"))
	}

	// Retry configuration
	if cfg.RetryEnabled && cfg.RetryMaxAttempts > 0 {
		opts = append(opts, otlptracegrpc.WithRetry(otlptracegrpc.RetryConfig{
			Enabled:         true,
			MaxElapsedTime:  30 * time.Second,
			MaxInterval:     5 * time.Second,
			InitialInterval: 1 * time.Second,
		}))
	}

	return opts, nil
}

// buildHTTPTracerExporterOptions builds OTLP HTTP trace exporter options from config
func buildHTTPTracerExporterOptions(cfg OTLPExporterConfig) ([]otlptracehttp.Option, error) {
	opts := []otlptracehttp.Option{}

	if isHTTPEndpointURL(cfg.Endpoint) {
		opts = append(opts, otlptracehttp.WithEndpointURL(cfg.Endpoint))
	} else {
		opts = append(opts, otlptracehttp.WithEndpoint(cfg.Endpoint))
	}

	// TLS Configuration
	if cfg.Insecure {
		opts = append(opts, otlptracehttp.WithInsecure())
	} else {
		tlsConfig, err := buildTLSConfig(cfg)
		if err != nil {
			return nil, err
		}
		opts = append(opts, otlptracehttp.WithTLSClientConfig(tlsConfig))
	}

	// Headers
	if len(cfg.Headers) > 0 {
		opts = append(opts, otlptracehttp.WithHeaders(cfg.Headers))
	}

	// Timeout
	if cfg.Timeout > 0 {
		opts = append(opts, otlptracehttp.WithTimeout(cfg.Timeout))
	}

	// Compression
	if cfg.Compression == "gzip" {
		opts = append(opts, otlptracehttp.WithCompression(otlptracehttp.GzipCompression))
	}

	// Retry configuration
	if cfg.RetryEnabled && cfg.RetryMaxAttempts > 0 {
		opts = append(opts, otlptracehttp.WithRetry(otlptracehttp.RetryConfig{
			Enabled:         true,
			MaxElapsedTime:  30 * time.Second,
			MaxInterval:     5 * time.Second,
			InitialInterval: 1 * time.Second,
		}))
	}

	return opts, nil
}

// InitTracerProvider initializes OpenTelemetry tracing with OTLP exporter
func InitTracerProvider(cfg Config) (*TracerProvider, error) {
	// Create resource with service information
	res, err := resource.Merge(
		resource.Default(),
		resource.NewWithAttributes(
			"",
			attribute.String("service.name", cfg.ServiceName),
			attribute.String("service.version", cfg.ServiceVersion),
			attribute.String("deployment.environment", cfg.Environment),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create resource: %w", err)
	}

	// Create OTLP trace exporter with gRPC
	ctx := context.Background()

	protocol, err := parseOTLPProtocol(cfg.OTLPConfig.Protocol)
	if err != nil {
		return nil, err
	}

	var traceExporter sdktrace.SpanExporter
	switch protocol {
	case otlpProtocolGRPC:
		// Build exporter options from config
		exporterOpts, err := buildTracerExporterOptions(cfg.OTLPConfig)
		if err != nil {
			return nil, err
		}
		traceExporter, err = otlptracegrpc.New(ctx, exporterOpts...)
		if err != nil {
			return nil, fmt.Errorf("failed to create OTLP trace exporter: %w", err)
		}
	case otlpProtocolHTTP:
		exporterOpts, err := buildHTTPTracerExporterOptions(cfg.OTLPConfig)
		if err != nil {
			return nil, err
		}
		traceExporter, err = otlptracehttp.New(ctx, exporterOpts...)
		if err != nil {
			return nil, fmt.Errorf("failed to create OTLP trace exporter: %w", err)
		}
	default:
		return nil, fmt.Errorf("unsupported OTLP trace protocol %q", cfg.OTLPConfig.Protocol)
	}

	// Create tracer provider with batch span processor
	provider := sdktrace.NewTracerProvider(
		sdktrace.WithResource(res),
		sdktrace.WithBatcher(traceExporter),
		sdktrace.WithSampler(traceSamplerForRatio(cfg.TraceSampleRatio)),
	)

	// Set global tracer provider
	otel.SetTracerProvider(provider)

	return &TracerProvider{
		provider: provider,
	}, nil
}

func traceSamplerForRatio(ratio float64) sdktrace.Sampler {
	switch {
	case ratio <= 0:
		return sdktrace.NeverSample()
	case ratio >= 1:
		return sdktrace.AlwaysSample()
	default:
		return sdktrace.ParentBased(sdktrace.TraceIDRatioBased(ratio))
	}
}

// Shutdown gracefully shuts down the tracer provider
func (tp *TracerProvider) Shutdown(ctx context.Context, logger *slog.Logger) error {
	shutdownCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	if err := tp.provider.Shutdown(shutdownCtx); err != nil {
		logger.Error("failed to shutdown tracer provider", slog.String("error", err.Error()))
		return err
	}

	logger.Info("tracer provider shutdown successfully")
	return nil
}

// LoggerProvider wraps the OpenTelemetry logger provider
type LoggerProvider struct {
	provider *log.LoggerProvider
}

// buildLoggerExporterOptions builds OTLP log exporter options from config
func buildLoggerExporterOptions(cfg OTLPExporterConfig) ([]otlploggrpc.Option, error) {
	opts := []otlploggrpc.Option{
		otlploggrpc.WithEndpoint(cfg.Endpoint),
	}

	// TLS Configuration
	if cfg.Insecure {
		opts = append(opts, otlploggrpc.WithInsecure())
	} else {
		// Configure TLS credentials
		tlsConfig, err := buildTLSConfig(cfg)
		if err != nil {
			return nil, err
		}
		opts = append(opts, otlploggrpc.WithTLSCredentials(credentials.NewTLS(tlsConfig)))
	}

	// Headers
	if len(cfg.Headers) > 0 {
		opts = append(opts, otlploggrpc.WithHeaders(cfg.Headers))
	}

	// Timeout
	if cfg.Timeout > 0 {
		opts = append(opts, otlploggrpc.WithTimeout(cfg.Timeout))
	}

	// Compression
	if cfg.Compression == "gzip" {
		opts = append(opts, otlploggrpc.WithCompressor("gzip"))
	}

	// Retry configuration
	if cfg.RetryEnabled && cfg.RetryMaxAttempts > 0 {
		opts = append(opts, otlploggrpc.WithRetry(otlploggrpc.RetryConfig{
			Enabled:         true,
			MaxElapsedTime:  30 * time.Second,
			MaxInterval:     5 * time.Second,
			InitialInterval: 1 * time.Second,
		}))
	}

	return opts, nil
}

// buildHTTPLoggerExporterOptions builds OTLP HTTP log exporter options from config
func buildHTTPLoggerExporterOptions(cfg OTLPExporterConfig) ([]otlploghttp.Option, error) {
	opts := []otlploghttp.Option{}

	if isHTTPEndpointURL(cfg.Endpoint) {
		opts = append(opts, otlploghttp.WithEndpointURL(cfg.Endpoint))
	} else {
		opts = append(opts, otlploghttp.WithEndpoint(cfg.Endpoint))
	}

	// TLS Configuration
	if cfg.Insecure {
		opts = append(opts, otlploghttp.WithInsecure())
	} else {
		tlsConfig, err := buildTLSConfig(cfg)
		if err != nil {
			return nil, err
		}
		opts = append(opts, otlploghttp.WithTLSClientConfig(tlsConfig))
	}

	// Headers
	if len(cfg.Headers) > 0 {
		opts = append(opts, otlploghttp.WithHeaders(cfg.Headers))
	}

	// Timeout
	if cfg.Timeout > 0 {
		opts = append(opts, otlploghttp.WithTimeout(cfg.Timeout))
	}

	// Compression
	if cfg.Compression == "gzip" {
		opts = append(opts, otlploghttp.WithCompression(otlploghttp.GzipCompression))
	}

	// Retry configuration
	if cfg.RetryEnabled && cfg.RetryMaxAttempts > 0 {
		opts = append(opts, otlploghttp.WithRetry(otlploghttp.RetryConfig{
			Enabled:         true,
			MaxElapsedTime:  30 * time.Second,
			MaxInterval:     5 * time.Second,
			InitialInterval: 1 * time.Second,
		}))
	}

	return opts, nil
}

// InitLoggerProvider initializes OpenTelemetry logging with OTLP exporter
func InitLoggerProvider(cfg Config) (*LoggerProvider, error) {
	// Create resource with service information
	res, err := resource.Merge(
		resource.Default(),
		resource.NewWithAttributes(
			"",
			attribute.String("service.name", cfg.ServiceName),
			attribute.String("service.version", cfg.ServiceVersion),
			attribute.String("deployment.environment", cfg.Environment),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create resource: %w", err)
	}

	// Create OTLP log exporter with gRPC
	ctx := context.Background()

	protocol, err := parseOTLPProtocol(cfg.OTLPConfig.Protocol)
	if err != nil {
		return nil, err
	}

	var logExporter log.Exporter
	switch protocol {
	case otlpProtocolGRPC:
		// Build exporter options from config
		exporterOpts, err := buildLoggerExporterOptions(cfg.OTLPConfig)
		if err != nil {
			return nil, err
		}
		logExporter, err = otlploggrpc.New(ctx, exporterOpts...)
		if err != nil {
			return nil, fmt.Errorf("failed to create OTLP log exporter: %w", err)
		}
	case otlpProtocolHTTP:
		exporterOpts, err := buildHTTPLoggerExporterOptions(cfg.OTLPConfig)
		if err != nil {
			return nil, err
		}
		logExporter, err = otlploghttp.New(ctx, exporterOpts...)
		if err != nil {
			return nil, fmt.Errorf("failed to create OTLP log exporter: %w", err)
		}
	default:
		return nil, fmt.Errorf("unsupported OTLP log protocol %q", cfg.OTLPConfig.Protocol)
	}

	// Create logger provider with batch log record processor
	provider := log.NewLoggerProvider(
		log.WithResource(res),
		log.WithProcessor(log.NewBatchProcessor(logExporter)),
	)

	return &LoggerProvider{
		provider: provider,
	}, nil
}

// Shutdown gracefully shuts down the logger provider
func (lp *LoggerProvider) Shutdown(ctx context.Context, logger *slog.Logger) error {
	shutdownCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	if err := lp.provider.Shutdown(shutdownCtx); err != nil {
		logger.Error("failed to shutdown logger provider", slog.String("error", err.Error()))
		return err
	}

	logger.Info("logger provider shutdown successfully")
	return nil
}

// Provider returns the underlying logger provider
func (lp *LoggerProvider) Provider() *log.LoggerProvider {
	return lp.provider
}
