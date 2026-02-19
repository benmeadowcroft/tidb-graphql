package middleware

import (
	"crypto/sha256"
	"crypto/subtle"
	"errors"
	"fmt"
	"net/http"
	"strings"
)

const defaultAdminTokenHeader = "X-Admin-Token"

// AdminTokenAuthConfig controls shared-token authentication for admin endpoints.
type AdminTokenAuthConfig struct {
	Token      string
	HeaderName string
}

// AdminTokenAuthMiddleware validates a shared admin token from request headers.
func AdminTokenAuthMiddleware(cfg AdminTokenAuthConfig) (func(http.Handler) http.Handler, error) {
	token := strings.TrimSpace(cfg.Token)
	if token == "" {
		return nil, errors.New("admin auth token is required")
	}
	headerName := strings.TrimSpace(cfg.HeaderName)
	if headerName == "" {
		headerName = defaultAdminTokenHeader
	}

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			provided := strings.TrimSpace(r.Header.Get(headerName))
			if !constantTimeTokenMatch(provided, token) {
				writeAdminUnauthorized(w)
				return
			}

			ctx := WithAuthContext(r.Context(), AuthContext{
				Subject: "admin_token",
				Issuer:  "admin_token",
				Claims: map[string]interface{}{
					"auth_method": "admin_token",
				},
			})
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}, nil
}

func constantTimeTokenMatch(provided string, expected string) bool {
	providedDigest := sha256.Sum256([]byte(provided))
	expectedDigest := sha256.Sum256([]byte(expected))
	return subtle.ConstantTimeCompare(providedDigest[:], expectedDigest[:]) == 1
}

func writeAdminUnauthorized(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusUnauthorized)
	_, _ = fmt.Fprint(w, `{"error":"unauthorized"}`)
}
