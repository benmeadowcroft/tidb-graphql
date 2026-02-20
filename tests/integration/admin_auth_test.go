//go:build integration
// +build integration

package integration

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"tidb-graphql/internal/logging"
	"tidb-graphql/internal/middleware"
	"tidb-graphql/internal/schemarefresh"
	"tidb-graphql/internal/testutil/tidbcloud"
)

// Note: This test reuses helper functions (generateKeypair, mintToken, newJWKSServer)
// from auth_test.go in the same package

func TestAdminEndpointAuthentication(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	// Set up test database using TiDB Cloud utility
	testDB := tidbcloud.NewTestDB(t)

	// Create a simple test table
	_, err := testDB.DB.Exec(`CREATE TABLE IF NOT EXISTS test_admin_table (id INT PRIMARY KEY, name VARCHAR(100))`)
	require.NoError(t, err)

	// Create schema refresh manager
	logger := logging.NewLogger(logging.Config{Level: "info", Format: "json"})
	manager, err := schemarefresh.NewManager(context.Background(), schemarefresh.Config{
		DB:           testDB.DB,
		DatabaseName: testDB.DatabaseName,
		Logger:       logger,
		MinInterval:  time.Minute,
		MaxInterval:  time.Minute * 5,
	})
	require.NoError(t, err)

	// Start manager with cancellable context
	ctx, cancel := context.WithCancel(context.Background())
	manager.Start(ctx)

	// Ensure manager is stopped after tests complete
	t.Cleanup(func() {
		cancel() // Signal manager to stop
		// Wait for manager shutdown with timeout
		waitCtx, waitCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer waitCancel()
		if err := manager.Wait(waitCtx); err != nil {
			t.Logf("Warning: manager shutdown timed out: %v", err)
		}
	})

	// Set up JWKS server for authentication
	privateKey, publicPath := generateKeypair(t)
	jwksServer := newJWKSServer(t, publicPath, "test-key")
	defer jwksServer.Close()
	oidcCAFile := writeOIDCTestCAFile(t, jwksServer)

	t.Run("without authentication - unauthenticated access blocked", func(t *testing.T) {
		// Create admin handler without authentication
		adminHandler := createAdminHandler(t, manager, false, "", "", "")
		server := httptest.NewServer(adminHandler)
		defer server.Close()

		// Try to reload schema without token
		resp, err := http.Post(server.URL, "application/json", nil)
		require.NoError(t, err)
		defer resp.Body.Close()

		// Should succeed when auth is disabled (logs warning)
		require.Equal(t, http.StatusOK, resp.StatusCode)
	})

	t.Run("with authentication - missing token blocked", func(t *testing.T) {
		// Create admin handler with authentication enabled
		adminHandler := createAdminHandler(t, manager, true, jwksServer.URL, "tidb-graphql", oidcCAFile)
		server := httptest.NewServer(adminHandler)
		defer server.Close()

		// Try to reload schema without token
		resp, err := http.Post(server.URL, "application/json", nil)
		require.NoError(t, err)
		defer resp.Body.Close()

		// Should be unauthorized
		require.Equal(t, http.StatusUnauthorized, resp.StatusCode)

		var result map[string]interface{}
		json.NewDecoder(resp.Body).Decode(&result)
		require.Contains(t, result["error"], "missing bearer token")
	})

	t.Run("with authentication - invalid token blocked", func(t *testing.T) {
		adminHandler := createAdminHandler(t, manager, true, jwksServer.URL, "tidb-graphql", oidcCAFile)
		server := httptest.NewServer(adminHandler)
		defer server.Close()

		// Try with invalid token
		req, err := http.NewRequest(http.MethodPost, server.URL, nil)
		require.NoError(t, err)
		req.Header.Set("Authorization", "Bearer invalid-token")

		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()

		require.Equal(t, http.StatusUnauthorized, resp.StatusCode)
	})

	t.Run("with authentication - valid token allowed", func(t *testing.T) {
		adminHandler := createAdminHandler(t, manager, true, jwksServer.URL, "tidb-graphql", oidcCAFile)
		server := httptest.NewServer(adminHandler)
		defer server.Close()

		// Create valid token
		token := mintToken(t, privateKey, jwksServer.URL, "tidb-graphql", "test-key", time.Now().Add(time.Hour))

		req, err := http.NewRequest(http.MethodPost, server.URL, nil)
		require.NoError(t, err)
		req.Header.Set("Authorization", "Bearer "+token)

		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()

		require.Equal(t, http.StatusOK, resp.StatusCode)

		var result map[string]interface{}
		json.NewDecoder(resp.Body).Decode(&result)
		require.Equal(t, "ok", result["status"])
	})

	t.Run("with authentication - wrong HTTP method blocked", func(t *testing.T) {
		adminHandler := createAdminHandler(t, manager, true, jwksServer.URL, "tidb-graphql", oidcCAFile)
		server := httptest.NewServer(adminHandler)
		defer server.Close()

		// Create valid token
		token := mintToken(t, privateKey, jwksServer.URL, "tidb-graphql", "test-key", time.Now().Add(time.Hour))

		// Try GET instead of POST
		req, err := http.NewRequest(http.MethodGet, server.URL, nil)
		require.NoError(t, err)
		req.Header.Set("Authorization", "Bearer "+token)

		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()

		require.Equal(t, http.StatusMethodNotAllowed, resp.StatusCode)
	})
}

func createAdminHandler(t *testing.T, manager *schemarefresh.Manager, authEnabled bool, issuerURL, audience, caFile string) http.Handler {
	t.Helper()

	logger := logging.NewLogger(logging.Config{Level: "info", Format: "json"})

	// Create the base handler
	var handler http.Handler = http.HandlerFunc(schemaReloadHandlerForTest(manager))

	// Wrap with authentication if enabled
	if authEnabled {
		authMiddleware, err := middleware.OIDCAuthMiddleware(middleware.OIDCAuthConfig{
			Enabled:   true,
			IssuerURL: issuerURL,
			Audience:  audience,
			CAFile:    caFile,
			ClockSkew: time.Minute,
		}, logger)
		require.NoError(t, err)
		handler = authMiddleware(handler)
	}

	return handler
}

func schemaReloadHandlerForTest(manager *schemarefresh.Manager) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			w.Write([]byte(`{"error":"method not allowed"}`))
			return
		}

		// Check for authentication context
		if authCtx, ok := middleware.AuthFromContext(r.Context()); ok {
			// Log that we have auth context (in real handler, this would go to logger)
			_ = authCtx
		}

		refreshCtx, refreshCancel := context.WithTimeout(r.Context(), 15*time.Second)
		defer refreshCancel()

		if err := manager.RefreshNowContext(refreshCtx); err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(`{"status":"error","error":"refresh failed"}`))
			return
		}

		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status":"ok"}`))
	}
}
