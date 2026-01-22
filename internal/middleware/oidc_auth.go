package middleware

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"tidb-graphql/internal/logging"
	"tidb-graphql/internal/observability"

	"github.com/coreos/go-oidc/v3/oidc"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/oauth2"
)

// OIDCAuthConfig controls OIDC/JWKS validation behavior.
type OIDCAuthConfig struct {
	Enabled       bool
	IssuerURL     string
	Audience      string
	ClockSkew     time.Duration
	SkipTLSVerify bool
}

type authContextKey struct{}

// AuthContext carries validated JWT claims.
type AuthContext struct {
	Subject  string
	Issuer   string
	Audience []string
	Claims   map[string]interface{}
}

// AuthFromContext returns the auth context from a request context.
func AuthFromContext(ctx context.Context) (AuthContext, bool) {
	value := ctx.Value(authContextKey{})
	if value == nil {
		return AuthContext{}, false
	}
	auth, ok := value.(AuthContext)
	return auth, ok
}

// OIDCAuthMiddleware validates Bearer tokens when enabled.
// Optional securityMetrics parameter enables security monitoring; pass nil to disable.
func OIDCAuthMiddleware(cfg OIDCAuthConfig, logger *logging.Logger, securityMetrics ...*observability.SecurityMetrics) (func(http.Handler) http.Handler, error) {
	if !cfg.Enabled {
		return func(next http.Handler) http.Handler { return next }, nil
	}

	// Extract optional security metrics
	var metrics *observability.SecurityMetrics
	if len(securityMetrics) > 0 {
		metrics = securityMetrics[0]
	}

	if cfg.IssuerURL == "" || cfg.Audience == "" {
		return nil, errors.New("oidc auth enabled but issuer/audience not configured")
	}

	if cfg.ClockSkew == 0 {
		cfg.ClockSkew = 2 * time.Minute
	}

	issuerURL, err := url.Parse(cfg.IssuerURL)
	if err != nil {
		return nil, fmt.Errorf("invalid oidc issuer url: %w", err)
	}
	if issuerURL.Scheme != "https" {
		return nil, errors.New("oidc issuer url must use https")
	}
	if logger != nil && cfg.SkipTLSVerify {
		logger.Warn("oidc tls verification is disabled; enable only for local development",
			"issuer", cfg.IssuerURL,
		)
	}

	httpClient := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: cfg.SkipTLSVerify},
		},
		Timeout: 10 * time.Second,
	}
	ctx := context.WithValue(context.Background(), oauth2.HTTPClient, httpClient)

	provider, err := oidc.NewProvider(ctx, cfg.IssuerURL)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize oidc provider: %w", err)
	}

	verifier := provider.Verifier(&oidc.Config{
		ClientID:        cfg.Audience,
		SkipIssuerCheck: false,
	})

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			endpoint := r.URL.Path

			// Record authentication attempt
			if metrics != nil {
				metrics.RecordAuthAttempt(r.Context(), endpoint)
			}

			tokenString := bearerToken(r.Header.Get("Authorization"))
			if tokenString == "" {
				// Record auth failure
				if metrics != nil {
					metrics.RecordAuthFailure(r.Context(), endpoint, "missing_token")
					metrics.RecordUnauthorizedAttempt(r.Context(), endpoint, "missing_token")
				}
				if logger != nil {
					reqLogger := logging.FromContext(r.Context())
					reqLogger.Warn("authentication failed: missing bearer token",
						slog.String("endpoint", endpoint),
						slog.String("remote_addr", r.RemoteAddr),
					)
				}
				writeUnauthorized(w, "missing bearer token")
				return
			}

			idToken, err := verifier.Verify(r.Context(), tokenString)
			if err != nil {
				// Record auth failure
				if metrics != nil {
					metrics.RecordAuthFailure(r.Context(), endpoint, "token_verification_failed")
					metrics.RecordTokenValidationError(r.Context(), "verification_failed")
					metrics.RecordUnauthorizedAttempt(r.Context(), endpoint, "invalid_token")
				}
				if logger != nil {
					reqLogger := logging.FromContext(r.Context())
					reqLogger.Warn("oidc token validation failed",
						slog.String("error", err.Error()),
						slog.String("endpoint", endpoint),
						slog.String("remote_addr", r.RemoteAddr),
					)
				}
				writeUnauthorized(w, "invalid token")
				return
			}

			claims := map[string]interface{}{}
			if err := idToken.Claims(&claims); err != nil {
				// Record auth failure
				if metrics != nil {
					metrics.RecordAuthFailure(r.Context(), endpoint, "claims_parse_failed")
					metrics.RecordTokenValidationError(r.Context(), "claims_parse_failed")
				}
				if logger != nil {
					reqLogger := logging.FromContext(r.Context())
					reqLogger.Warn("oidc token claims parse failed",
						slog.String("error", err.Error()),
						slog.String("endpoint", endpoint),
					)
				}
				writeUnauthorized(w, "invalid token claims")
				return
			}

			if err := validateTimeClaims(claims, cfg.ClockSkew); err != nil {
				// Record auth failure
				if metrics != nil {
					metrics.RecordAuthFailure(r.Context(), endpoint, "time_validation_failed")
					metrics.RecordTokenValidationError(r.Context(), "time_validation_failed")
				}
				if logger != nil {
					reqLogger := logging.FromContext(r.Context())
					reqLogger.Warn("oidc token time validation failed",
						slog.String("error", err.Error()),
						slog.String("endpoint", endpoint),
					)
				}
				writeUnauthorized(w, "invalid token")
				return
			}

			subject, _ := claims["sub"].(string)
			aud := extractAudience(claims)

			// Record successful authentication
			if metrics != nil {
				metrics.RecordAuthSuccess(r.Context(), endpoint, cfg.IssuerURL)
			}

			// Add authentication context to logger
			if logger != nil {
				reqLogger := logging.FromContext(r.Context())
				reqLogger.Debug("authentication successful",
					slog.String("subject", subject),
					slog.String("issuer", cfg.IssuerURL),
					slog.String("endpoint", endpoint),
				)
			}

			// Add trace attributes for authenticated user
			if span := trace.SpanFromContext(r.Context()); span.IsRecording() {
				span.SetAttributes(
					attribute.String("auth.subject", subject),
					attribute.String("auth.issuer", cfg.IssuerURL),
					attribute.Bool("auth.authenticated", true),
				)
				if len(aud) > 0 {
					span.SetAttributes(attribute.StringSlice("auth.audience", aud))
				}
			}

			ctx := context.WithValue(r.Context(), authContextKey{}, AuthContext{
				Subject:  subject,
				Issuer:   cfg.IssuerURL,
				Audience: aud,
				Claims:   claims,
			})

			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}, nil
}

func bearerToken(value string) string {
	parts := strings.SplitN(value, " ", 2)
	if len(parts) != 2 {
		return ""
	}
	if !strings.EqualFold(parts[0], "Bearer") {
		return ""
	}
	return strings.TrimSpace(parts[1])
}

func writeUnauthorized(w http.ResponseWriter, message string) {
	w.Header().Set("WWW-Authenticate", "Bearer")
	w.WriteHeader(http.StatusUnauthorized)
	_, _ = fmt.Fprintf(w, `{"error":"%s"}`, message)
}

func validateTimeClaims(claims map[string]interface{}, skew time.Duration) error {
	if skew <= 0 {
		return nil
	}

	now := time.Now()
	if exp, ok := numericDate(claims["exp"]); ok {
		if now.After(exp.Add(skew)) {
			return errors.New("token expired")
		}
	}
	if nbf, ok := numericDate(claims["nbf"]); ok {
		if now.Add(skew).Before(nbf) {
			return errors.New("token not valid yet")
		}
	}
	return nil
}

func numericDate(value interface{}) (time.Time, bool) {
	switch v := value.(type) {
	case float64:
		return time.Unix(int64(v), 0), true
	case int64:
		return time.Unix(v, 0), true
	case int:
		return time.Unix(int64(v), 0), true
	case json.Number:
		parsed, err := v.Int64()
		if err != nil {
			return time.Time{}, false
		}
		return time.Unix(parsed, 0), true
	case string:
		parsed, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return time.Time{}, false
		}
		return time.Unix(parsed, 0), true
	default:
		return time.Time{}, false
	}
}

func extractAudience(claims map[string]interface{}) []string {
	raw, ok := claims["aud"]
	if !ok {
		return nil
	}

	switch val := raw.(type) {
	case string:
		return []string{val}
	case []string:
		return val
	case []interface{}:
		result := make([]string, 0, len(val))
		for _, item := range val {
			if str, ok := item.(string); ok {
				result = append(result, str)
			}
		}
		return result
	default:
		return nil
	}
}
