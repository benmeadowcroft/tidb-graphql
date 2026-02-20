package serverapp

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"tidb-graphql/internal/config"
)

func TestBuildRouter_AdminRouteDisabledReturnsNotFound(t *testing.T) {
	cfg := &config.Config{
		Server: config.ServerConfig{
			HealthCheckTimeout: time.Second,
			Admin: config.AdminConfig{
				SchemaReloadEnabled: false,
			},
		},
	}
	graphqlHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	adminHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})

	mux := buildRouter(cfg, testLogger(), nil, graphqlHandler, adminHandler, nil)

	req := httptest.NewRequest(http.MethodPost, "/admin/reload-schema", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected status %d, got %d", http.StatusNotFound, rec.Code)
	}
}

func TestBuildRouter_AdminRouteEnabledInvokesHandler(t *testing.T) {
	cfg := &config.Config{
		Server: config.ServerConfig{
			HealthCheckTimeout: time.Second,
			Admin: config.AdminConfig{
				SchemaReloadEnabled: true,
			},
		},
	}
	graphqlHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	adminHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	mux := buildRouter(cfg, testLogger(), nil, graphqlHandler, adminHandler, nil)

	req := httptest.NewRequest(http.MethodPost, "/admin/reload-schema", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d", http.StatusOK, rec.Code)
	}
}

func TestBuildAdminHandler_TokenModeMissingHeaderUnauthorized(t *testing.T) {
	cfg := &config.Config{
		Server: config.ServerConfig{
			Admin: config.AdminConfig{
				SchemaReloadEnabled: true,
				AuthToken:           "secret-token",
			},
			Auth: config.AuthConfig{
				OIDCEnabled: false,
			},
		},
	}

	adminHandler, err := buildAdminHandler(cfg, testLogger(), nil, nil)
	if err != nil {
		t.Fatalf("unexpected buildAdminHandler error: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/admin/reload-schema", nil)
	rec := httptest.NewRecorder()
	adminHandler.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected status %d, got %d", http.StatusUnauthorized, rec.Code)
	}
}

func TestBuildAdminHandler_TokenModeValidHeaderReachesHandler(t *testing.T) {
	cfg := &config.Config{
		Server: config.ServerConfig{
			Admin: config.AdminConfig{
				SchemaReloadEnabled: true,
				AuthToken:           "secret-token",
			},
			Auth: config.AuthConfig{
				OIDCEnabled: false,
			},
		},
	}

	adminHandler, err := buildAdminHandler(cfg, testLogger(), nil, nil)
	if err != nil {
		t.Fatalf("unexpected buildAdminHandler error: %v", err)
	}

	// GET verifies token auth passes through to schemaReloadHandler without invoking manager refresh.
	req := httptest.NewRequest(http.MethodGet, "/admin/reload-schema", nil)
	req.Header.Set("X-Admin-Token", "secret-token")
	rec := httptest.NewRecorder()
	adminHandler.ServeHTTP(rec, req)

	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected status %d, got %d", http.StatusMethodNotAllowed, rec.Code)
	}
}

func TestBuildAdminHandler_OIDCModeUsesOIDCMiddlewarePath(t *testing.T) {
	cfg := &config.Config{
		Server: config.ServerConfig{
			Admin: config.AdminConfig{
				SchemaReloadEnabled: true,
			},
			Auth: config.AuthConfig{
				OIDCEnabled: true,
				// Missing issuer/audience should fail during OIDC middleware setup.
			},
		},
	}

	_, err := buildAdminHandler(cfg, testLogger(), nil, nil)
	if err == nil {
		t.Fatalf("expected OIDC setup error, got nil")
	}
	if !strings.Contains(err.Error(), "oidc auth enabled but issuer/audience not configured") {
		t.Fatalf("unexpected error: %v", err)
	}
}
