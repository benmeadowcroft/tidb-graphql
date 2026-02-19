//go:build integration
// +build integration

package integration

import (
	"fmt"
	"io"
	"net/http"
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

	_, _, _ = startTestApp(
		t,
		18081,
		fmt.Sprintf("TIGQL_DATABASE_DATABASE=%s", testDB.DatabaseName),
		"TIGQL_OBSERVABILITY_METRICS_ENABLED=true",
		"TIGQL_OBSERVABILITY_LOGGING_FORMAT=text",
	)

	// Test 1: Verify /metrics endpoint exists and returns Prometheus format
	t.Run("metrics endpoint accessible", func(t *testing.T) {
		metricsOutput := fetchMetrics(t, 18081)

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

		metricsOutput := waitForMetrics(t, 18081, 2*time.Second, func(output string) bool {
			// The exact metric names depend on otelhttp version, but should include http_ prefix.
			return strings.Contains(output, "http_server") ||
				strings.Contains(output, "http_request") ||
				strings.Contains(output, "target_info")
		})

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

		metricsOutput := waitForMetrics(t, 18081, 2*time.Second, func(output string) bool {
			return strings.Contains(output, "graphql_request_duration") &&
				strings.Contains(output, "graphql_requests_total")
		})

		// Verify custom GraphQL metrics are present
		assert.Contains(t, metricsOutput, "graphql_request_duration", "Should contain GraphQL request duration metric")
		assert.Contains(t, metricsOutput, "graphql_requests_total", "Should contain GraphQL request counter")
	})

	// Test 4: Verify database instrumentation metrics
	t.Run("database instrumentation metrics", func(t *testing.T) {
		// Database connection should already be established by server startup
		metricsOutput := waitForMetrics(t, 18081, 2*time.Second, func(output string) bool {
			return strings.Contains(output, "db_sql") || strings.Contains(output, "sql")
		})

		// Verify database metrics from otelsql are present
		hasDatabaseMetrics := strings.Contains(metricsOutput, "db_sql") ||
			strings.Contains(metricsOutput, "sql")

		assert.True(t, hasDatabaseMetrics, "Should contain database instrumentation metrics")
	})

}

func fetchMetrics(t *testing.T, port int) string {
	t.Helper()
	resp, err := http.Get(fmt.Sprintf("http://localhost:%d/metrics", port))
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode, "Metrics endpoint should return 200")

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	return string(body)
}

func waitForMetrics(t *testing.T, port int, timeout time.Duration, predicate func(string) bool) string {
	t.Helper()

	deadline := time.Now().Add(timeout)
	latest := ""
	for {
		latest = fetchMetrics(t, port)
		if predicate(latest) {
			return latest
		}
		if time.Now().After(deadline) {
			t.Fatalf("expected metrics condition was not met within %s", timeout)
		}
		time.Sleep(25 * time.Millisecond)
	}
}
