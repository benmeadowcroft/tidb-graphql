//go:build integration
// +build integration

package integration

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"tidb-graphql/internal/testutil/tidbcloud"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMetricsEndpoint(t *testing.T) {
	requireIntegrationEnv(t)

	// This test verifies that the /metrics endpoint exposes Prometheus metrics

	testDB := tidbcloud.NewTestDB(t)

	// Build the server binary
	cmd, _ := startTestServer(
		t,
		"../../bin/tidb-graphql-metrics-test",
		18081,
		fmt.Sprintf("TIGQL_DATABASE_DATABASE=%s", testDB.DatabaseName),
		"TIGQL_OBSERVABILITY_METRICS_ENABLED=true",
		"TIGQL_OBSERVABILITY_LOGGING_FORMAT=text",
	)

	// Test 1: Verify /metrics endpoint exists and returns Prometheus format
	t.Run("metrics endpoint accessible", func(t *testing.T) {
		resp, err := http.Get("http://localhost:18081/metrics")
		require.NoError(t, err)
		defer resp.Body.Close()

		assert.Equal(t, http.StatusOK, resp.StatusCode, "Metrics endpoint should return 200")

		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)

		metricsOutput := string(body)

		// Verify Prometheus format (should contain HELP and TYPE comments)
		assert.Contains(t, metricsOutput, "# HELP", "Should contain Prometheus HELP comments")
		assert.Contains(t, metricsOutput, "# TYPE", "Should contain Prometheus TYPE comments")
	})

	// Test 2: Verify HTTP instrumentation metrics
	t.Run("http instrumentation metrics", func(t *testing.T) {
		// Make a request to generate HTTP metrics
			resp, err := http.Get("http://localhost:18081/health")
			require.NoError(t, err)
			resp.Body.Close()

		// Wait a bit for metrics to be recorded
		time.Sleep(100 * time.Millisecond)

		// Fetch metrics
		resp, err = http.Get("http://localhost:18081/metrics")
		require.NoError(t, err)
		defer resp.Body.Close()

		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)

		metricsOutput := string(body)

		// Verify HTTP metrics from otelhttp are present
		// The exact metric names depend on otelhttp version, but should include http_ prefix
		hasHttpMetrics := strings.Contains(metricsOutput, "http_server") ||
			strings.Contains(metricsOutput, "http_request") ||
			strings.Contains(metricsOutput, "target_info")

		assert.True(t, hasHttpMetrics, "Should contain HTTP instrumentation metrics")
	})

	// Test 3: Make a GraphQL query and verify custom metrics
	t.Run("graphql custom metrics", func(t *testing.T) {
		// Make a simple GraphQL query
		graphqlQuery := `{"query":"{ __schema { queryType { name } } }"}`
		resp, err := http.Post(
			"http://localhost:18081/graphql",
			"application/json",
			strings.NewReader(graphqlQuery),
		)
		require.NoError(t, err)
		resp.Body.Close()

		// Wait for metrics to be recorded
		time.Sleep(100 * time.Millisecond)

		// Fetch metrics
		resp, err = http.Get("http://localhost:18081/metrics")
		require.NoError(t, err)
		defer resp.Body.Close()

		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)

		metricsOutput := string(body)

		// Verify custom GraphQL metrics are present
		assert.Contains(t, metricsOutput, "graphql_request_duration", "Should contain GraphQL request duration metric")
		assert.Contains(t, metricsOutput, "graphql_requests_total", "Should contain GraphQL request counter")
	})

	// Test 4: Verify database instrumentation metrics
	t.Run("database instrumentation metrics", func(t *testing.T) {
		// Database connection should already be established by server startup
		// Fetch metrics
		resp, err := http.Get("http://localhost:18081/metrics")
		require.NoError(t, err)
		defer resp.Body.Close()

		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)

		metricsOutput := string(body)

		// Verify database metrics from otelsql are present
		hasDatabaseMetrics := strings.Contains(metricsOutput, "db_sql") ||
			strings.Contains(metricsOutput, "sql")

		assert.True(t, hasDatabaseMetrics, "Should contain database instrumentation metrics")
	})

	// Gracefully stop the server
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := cmd.Process.Signal(os.Interrupt); err != nil {
		t.Logf("Failed to send interrupt signal: %v", err)
	}

	// Wait for graceful shutdown
	doneChan := make(chan error, 1)
	go func() {
		doneChan <- cmd.Wait()
	}()

	select {
	case <-doneChan:
		// Server stopped successfully
	case <-ctx.Done():
		// Force kill if graceful shutdown takes too long
		cmd.Process.Kill()
	}

}
