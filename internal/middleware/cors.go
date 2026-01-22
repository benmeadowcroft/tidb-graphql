package middleware

import (
	"fmt"
	"net/http"
	"strings"
)

// CORSConfig configures Cross-Origin Resource Sharing (CORS) policies.
type CORSConfig struct {
	Enabled          bool
	AllowedOrigins   []string
	AllowedMethods   []string
	AllowedHeaders   []string
	ExposeHeaders    []string
	AllowCredentials bool
	MaxAge           int
}

// CORSMiddleware adds CORS headers and handles preflight requests.
func CORSMiddleware(cfg CORSConfig) func(http.Handler) http.Handler {
	if !cfg.Enabled {
		return func(next http.Handler) http.Handler {
			return next
		}
	}

	allowAllOrigins := false
	allowedOrigins := make(map[string]struct{})
	for _, origin := range cfg.AllowedOrigins {
		origin = strings.TrimSpace(origin)
		if origin == "" {
			continue
		}
		if origin == "*" {
			allowAllOrigins = true
			break
		}
		allowedOrigins[origin] = struct{}{}
	}

	methodsHeader := strings.Join(cfg.AllowedMethods, ", ")
	headersHeader := strings.Join(cfg.AllowedHeaders, ", ")
	exposeHeader := ""
	if len(cfg.ExposeHeaders) > 0 {
		exposeHeader = strings.Join(cfg.ExposeHeaders, ", ")
	}
	maxAgeHeader := ""
	if cfg.MaxAge > 0 {
		maxAgeHeader = fmt.Sprintf("%d", cfg.MaxAge)
	}

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			origin := r.Header.Get("Origin")
			if origin == "" {
				next.ServeHTTP(w, r)
				return
			}

			allowOrigin := allowAllOrigins
			if !allowAllOrigins {
				_, allowOrigin = allowedOrigins[origin]
			}

			if allowOrigin {
				if allowAllOrigins {
					w.Header().Set("Access-Control-Allow-Origin", "*")
				} else {
					w.Header().Set("Access-Control-Allow-Origin", origin)
					w.Header().Add("Vary", "Origin")
				}

				if cfg.AllowCredentials && !allowAllOrigins {
					w.Header().Set("Access-Control-Allow-Credentials", "true")
				}

				if exposeHeader != "" {
					w.Header().Set("Access-Control-Expose-Headers", exposeHeader)
				}
			}

			if r.Method == http.MethodOptions {
				if allowOrigin {
					if methodsHeader != "" {
						w.Header().Set("Access-Control-Allow-Methods", methodsHeader)
					}
					if headersHeader != "" {
						w.Header().Set("Access-Control-Allow-Headers", headersHeader)
					}
					if maxAgeHeader != "" {
						w.Header().Set("Access-Control-Max-Age", maxAgeHeader)
					}
				}

				w.WriteHeader(http.StatusNoContent)
				return
			}

			next.ServeHTTP(w, r)
		})
	}
}
