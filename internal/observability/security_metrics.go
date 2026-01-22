package observability

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// SecurityMetrics holds security-related metrics for monitoring authentication and authorization
type SecurityMetrics struct {
	authAttempts          metric.Int64Counter
	authFailures          metric.Int64Counter
	authSuccesses         metric.Int64Counter
	adminEndpointAccess   metric.Int64Counter
	unauthorizedAttempts  metric.Int64Counter
	tokenValidationErrors metric.Int64Counter
}

// InitSecurityMetrics initializes security-specific metrics
func InitSecurityMetrics() (*SecurityMetrics, error) {
	meter := otel.Meter("tidb-graphql/security")

	authAttempts, err := meter.Int64Counter(
		"security.auth.attempts.total",
		metric.WithDescription("Total number of authentication attempts"),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create auth attempts counter: %w", err)
	}

	authFailures, err := meter.Int64Counter(
		"security.auth.failures.total",
		metric.WithDescription("Total number of authentication failures"),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create auth failures counter: %w", err)
	}

	authSuccesses, err := meter.Int64Counter(
		"security.auth.successes.total",
		metric.WithDescription("Total number of successful authentications"),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create auth successes counter: %w", err)
	}

	adminEndpointAccess, err := meter.Int64Counter(
		"security.admin.access.total",
		metric.WithDescription("Total number of admin endpoint access attempts"),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create admin endpoint access counter: %w", err)
	}

	unauthorizedAttempts, err := meter.Int64Counter(
		"security.unauthorized.attempts.total",
		metric.WithDescription("Total number of unauthorized access attempts"),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create unauthorized attempts counter: %w", err)
	}

	tokenValidationErrors, err := meter.Int64Counter(
		"security.token.validation_errors.total",
		metric.WithDescription("Total number of token validation errors"),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create token validation errors counter: %w", err)
	}

	return &SecurityMetrics{
		authAttempts:          authAttempts,
		authFailures:          authFailures,
		authSuccesses:         authSuccesses,
		adminEndpointAccess:   adminEndpointAccess,
		unauthorizedAttempts:  unauthorizedAttempts,
		tokenValidationErrors: tokenValidationErrors,
	}, nil
}

// RecordAuthAttempt records an authentication attempt
func (m *SecurityMetrics) RecordAuthAttempt(ctx context.Context, endpoint string) {
	m.authAttempts.Add(ctx, 1, metric.WithAttributes(
		attribute.String("endpoint", endpoint),
	))
}

// RecordAuthFailure records a failed authentication attempt
func (m *SecurityMetrics) RecordAuthFailure(ctx context.Context, endpoint, reason string) {
	m.authFailures.Add(ctx, 1, metric.WithAttributes(
		attribute.String("endpoint", endpoint),
		attribute.String("reason", reason),
	))
}

// RecordAuthSuccess records a successful authentication
func (m *SecurityMetrics) RecordAuthSuccess(ctx context.Context, endpoint, issuer string) {
	m.authSuccesses.Add(ctx, 1, metric.WithAttributes(
		attribute.String("endpoint", endpoint),
		attribute.String("issuer", issuer),
	))
}

// RecordAdminEndpointAccess records access to admin endpoints
func (m *SecurityMetrics) RecordAdminEndpointAccess(ctx context.Context, operation string, authenticated bool, success bool) {
	attrs := []attribute.KeyValue{
		attribute.String("operation", operation),
		attribute.Bool("authenticated", authenticated),
		attribute.Bool("success", success),
	}
	m.adminEndpointAccess.Add(ctx, 1, metric.WithAttributes(attrs...))
}

// RecordUnauthorizedAttempt records an unauthorized access attempt
func (m *SecurityMetrics) RecordUnauthorizedAttempt(ctx context.Context, endpoint, reason string) {
	m.unauthorizedAttempts.Add(ctx, 1, metric.WithAttributes(
		attribute.String("endpoint", endpoint),
		attribute.String("reason", reason),
	))
}

// RecordTokenValidationError records a token validation error
func (m *SecurityMetrics) RecordTokenValidationError(ctx context.Context, errorType string) {
	m.tokenValidationErrors.Add(ctx, 1, metric.WithAttributes(
		attribute.String("error_type", errorType),
	))
}
