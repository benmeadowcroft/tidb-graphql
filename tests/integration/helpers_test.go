//go:build integration
// +build integration

package integration

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"

	"tidb-graphql/internal/config"
	"tidb-graphql/internal/dbexec"
	"tidb-graphql/internal/introspection"
	"tidb-graphql/internal/naming"
	"tidb-graphql/internal/nodeid"
	"tidb-graphql/internal/schemafilter"
	"tidb-graphql/internal/schemarefresh"
	"tidb-graphql/internal/serverapp"
	"tidb-graphql/internal/testutil/tidbcloud"

	"github.com/graphql-go/graphql"
	"github.com/stretchr/testify/require"
)

func requireIntegrationEnv(t *testing.T) {
	t.Helper()
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}
	if os.Getenv("TIDB_HOST") == "" {
		t.Skip("TiDB credentials not set")
	}
}

func requireSnapshotReadSupport(t *testing.T, db *sql.DB) {
	t.Helper()

	const skipReason = "@asOf integration tests require TiKV/PD-backed TiDB with tikv_gc_safe_point available"

	var safePoint sql.NullString
	err := db.QueryRow(`SELECT variable_value FROM mysql.tidb WHERE variable_name = 'tikv_gc_safe_point'`).Scan(&safePoint)
	switch {
	case err == sql.ErrNoRows:
		t.Skip(skipReason)
	case err != nil:
		if isSnapshotUnsupportedError(err) {
			t.Skip(skipReason)
		}
		require.NoError(t, err, "failed to probe snapshot read support")
	case strings.TrimSpace(safePoint.String) == "":
		t.Skip(skipReason)
	}

	var snapshotTime time.Time
	err = db.QueryRow(`SELECT NOW(6)`).Scan(&snapshotTime)
	require.NoError(t, err, "failed to read current database time for snapshot probe")

	// Use a small past offset rather than the exact current database timestamp.
	// TiDB can reject boundary timestamps that land slightly ahead of currentTS.
	sessionValue := snapshotTime.Add(-1 * time.Second).Format("2006-01-02 15:04:05.999999")
	if _, err := db.Exec(`SET @@tidb_snapshot = ?`, sessionValue); err != nil {
		if isSnapshotUnsupportedError(err) {
			t.Skip(skipReason)
		}
		require.NoError(t, err, "failed to enable snapshot reads during probe")
	}

	_, err = db.Exec(`SET @@tidb_snapshot = ''`)
	require.NoError(t, err, "failed to clear tidb_snapshot after snapshot probe")
}

func isSnapshotUnsupportedError(err error) bool {
	if err == nil {
		return false
	}

	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "can not get 'tikv_gc_safe_point'") ||
		strings.Contains(msg, "can't get 'tikv_gc_safe_point'")
}

func cloudDSN() string {
	return fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?parseTime=true&tls=%s",
		cloudUserWithPrefix(),
		os.Getenv("TIDB_PASSWORD"),
		os.Getenv("TIDB_HOST"),
		getEnvOrDefault("TIDB_PORT", "4000"),
		getEnvOrDefault("TIDB_DATABASE", "test"),
		getEnvOrDefault("TIDB_TLS_MODE", "skip-verify"),
	)
}

func startTestServer(t *testing.T, binaryName string, port int, extraEnv ...string) (*exec.Cmd, func()) {
	t.Helper()

	buildCmd := exec.Command("go", "build", "-o", binaryName, "../../cmd/server")
	err := buildCmd.Run()
	require.NoError(t, err, "Failed to build server")

	cmd := exec.Command(binaryName)
	baseEnv := append(os.Environ(), baseServerEnv()...)
	baseEnv = append(baseEnv, fmt.Sprintf("TIGQL_SERVER_PORT=%d", port))
	cmd.Env = mergeEnv(baseEnv, extraEnv...)
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	err = cmd.Start()
	require.NoError(t, err)

	cleanup := func() {
		if cmd.Process != nil {
			_ = cmd.Process.Kill()
		}
		_ = os.Remove(binaryName)
	}
	t.Cleanup(cleanup)

	waitForHealthyWithLogs(t, port, &stdout, &stderr, cmd.Env)

	return cmd, cleanup
}

func startTestApp(t *testing.T, port int, extraEnv ...string) (*serverapp.App, <-chan error, func()) {
	t.Helper()

	env := mergeEnv(baseServerEnv(), append([]string{fmt.Sprintf("TIGQL_SERVER_PORT=%d", port)}, extraEnv...)...)
	cfg := buildTestConfigFromEnv(port, env)
	return startTestAppWithConfig(t, cfg)
}

func startTestAppWithConfig(t *testing.T, cfg *config.Config) (*serverapp.App, <-chan error, func()) {
	t.Helper()

	validationResult := cfg.Validate()
	require.False(t, validationResult.HasErrors(), "test app config should validate: %v", validationResult.Errors)

	logger, loggerProvider, err := serverapp.InitLogger(cfg)
	require.NoError(t, err)

	app, err := serverapp.New(cfg, logger)
	require.NoError(t, err)
	app.AttachLoggerProvider(loggerProvider)

	err = app.Init(context.Background())
	require.NoError(t, err)

	serverErrors, err := app.Start()
	require.NoError(t, err)

	waitForHealthy(t, cfg.Server.Port, serverErrors)

	cleanup := func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.Server.ShutdownTimeout)
		defer cancel()
		_ = app.Shutdown(shutdownCtx)
	}
	t.Cleanup(cleanup)

	return app, serverErrors, cleanup
}

func buildBaseTestConfig(port int) *config.Config {
	return buildTestConfigFromEnv(port, append(baseServerEnv(), fmt.Sprintf("TIGQL_SERVER_PORT=%d", port)))
}

func executeGraphQLHTTP(t *testing.T, port int, query string) map[string]interface{} {
	t.Helper()

	result := executeGraphQLHTTPRaw(t, port, query, nil, "")
	require.Equal(t, http.StatusOK, result.StatusCode, "unexpected GraphQL HTTP status: %s", result.RawBody)
	require.Empty(t, result.Errors, "GraphQL returned errors: %s", result.RawBody)
	return result.Data
}

type graphQLHTTPResult struct {
	StatusCode int
	Data       map[string]interface{}
	Errors     []map[string]interface{}
	RawBody    string
}

func executeGraphQLHTTPRaw(t *testing.T, port int, query string, variables map[string]interface{}, operationName string) graphQLHTTPResult {
	t.Helper()

	payloadMap := map[string]interface{}{"query": query}
	if variables != nil {
		payloadMap["variables"] = variables
	}
	if operationName != "" {
		payloadMap["operationName"] = operationName
	}

	payload, err := json.Marshal(payloadMap)
	require.NoError(t, err)

	req, err := http.NewRequest(http.MethodPost, fmt.Sprintf("http://localhost:%d/graphql", port), bytes.NewReader(payload))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	var decoded struct {
		Data   map[string]interface{}   `json:"data"`
		Errors []map[string]interface{} `json:"errors"`
	}
	require.NoError(t, json.Unmarshal(body, &decoded), "failed to parse GraphQL response: %s", string(body))
	return graphQLHTTPResult{
		StatusCode: resp.StatusCode,
		Data:       decoded.Data,
		Errors:     decoded.Errors,
		RawBody:    string(body),
	}
}

func waitForHealthyWithLogs(t *testing.T, port int, stdout, stderr *bytes.Buffer, env []string) {
	t.Helper()
	if err := waitForHTTPStatus(port, "/health", http.StatusOK, 10*time.Second, 50*time.Millisecond, nil); err == nil {
		return
	}
	t.Fatalf("Server did not become ready within 10 seconds.\n%s", formatServerDebugInfo(stdout, stderr, env))
}

func waitForHealthy(t *testing.T, port int, serverErrors <-chan error) {
	t.Helper()
	if err := waitForHTTPStatus(port, "/health", http.StatusOK, 10*time.Second, 50*time.Millisecond, serverErrors); err == nil {
		return
	}
	t.Fatalf("Server did not become ready within 10 seconds on port %d", port)
}

func waitForHTTPStatus(port int, path string, expectedStatus int, timeout, interval time.Duration, serverErrors <-chan error) error {
	client := &http.Client{Timeout: 300 * time.Millisecond}
	url := fmt.Sprintf("http://localhost:%d%s", port, path)
	deadline := time.Now().Add(timeout)

	for {
		if serverErrors != nil {
			select {
			case err := <-serverErrors:
				if err == nil {
					return fmt.Errorf("server stopped unexpectedly while waiting for %s", path)
				}
				return fmt.Errorf("server failed while waiting for %s: %w", path, err)
			default:
			}
		}

		resp, err := client.Get(url)
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == expectedStatus {
				return nil
			}
		}

		if time.Now().After(deadline) {
			return fmt.Errorf("timed out waiting for %s to return %d", path, expectedStatus)
		}
		time.Sleep(interval)
	}
}

func buildTestConfigFromEnv(port int, env []string) *config.Config {
	values := map[string]string{}
	for _, kv := range env {
		parts := strings.SplitN(kv, "=", 2)
		if len(parts) != 2 {
			continue
		}
		values[parts[0]] = parts[1]
	}

	metricsEnabled := false
	if raw, ok := values["TIGQL_OBSERVABILITY_METRICS_ENABLED"]; ok {
		enabled, err := strconv.ParseBool(raw)
		if err == nil {
			metricsEnabled = enabled
		}
	}

	logFormat := values["TIGQL_OBSERVABILITY_LOGGING_FORMAT"]
	if strings.TrimSpace(logFormat) == "" {
		logFormat = "text"
	}

	return &config.Config{
		Database: config.DatabaseConfig{
			Host:     values["TIGQL_DATABASE_HOST"],
			Port:     atoiDefault(values["TIGQL_DATABASE_PORT"], 4000),
			User:     values["TIGQL_DATABASE_USER"],
			Password: values["TIGQL_DATABASE_PASSWORD"],
			Database: values["TIGQL_DATABASE_DATABASE"],
			TLS: config.DatabaseTLSConfig{
				Mode: values["TIGQL_DATABASE_TLS_MODE"],
			},
			Pool: config.PoolConfig{
				MaxOpen:     10,
				MaxIdle:     5,
				MaxLifetime: 30 * time.Minute,
			},
			ConnectionTimeout:       10 * time.Second,
			ConnectionRetryInterval: 200 * time.Millisecond,
		},
		Server: config.ServerConfig{
			Port:                     port,
			GraphQLDefaultLimit:      100,
			SchemaRefreshMinInterval: time.Second,
			SchemaRefreshMaxInterval: 5 * time.Second,
			GraphiQLEnabled:          false,
			Search: config.SearchConfig{
				VectorRequireIndex: true,
				VectorMaxTopK:      100,
			},
			ReadTimeout:        10 * time.Second,
			WriteTimeout:       10 * time.Second,
			IdleTimeout:        30 * time.Second,
			ShutdownTimeout:    30 * time.Second,
			HealthCheckTimeout: 5 * time.Second,
			TLSMode:            "off",
		},
		Observability: config.ObservabilityConfig{
			ServiceName:    "tidb-graphql",
			ServiceVersion: "test",
			Environment:    "integration",
			MetricsEnabled: metricsEnabled,
			TracingEnabled: false,
			Logging: config.LoggingConfig{
				Level:          "info",
				Format:         logFormat,
				ExportsEnabled: false,
			},
		},
		SchemaFilters: schemafilter.Config{},
		TypeMappings: config.TypeMappingsConfig{
			UUIDColumns:            map[string][]string{},
			TinyInt1BooleanColumns: map[string][]string{},
			TinyInt1IntColumns:     map[string][]string{},
		},
		Naming: naming.DefaultConfig(),
	}
}

func atoiDefault(raw string, fallback int) int {
	if raw == "" {
		return fallback
	}
	value, err := strconv.Atoi(raw)
	if err != nil {
		return fallback
	}
	return value
}

func openCloudDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("mysql", cloudDSN())
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, db.PingContext(ctx), "Database should be accessible for health check test")
	return db
}

func nodeIDForTable(tableName string, pkValues ...interface{}) string {
	return nodeid.Encode(introspection.ToGraphQLTypeName(tableName), pkValues...)
}

func buildGraphQLSchema(t *testing.T, testDB *tidbcloud.TestDB) graphql.Schema {
	t.Helper()

	schema, _ := buildSchemaWithConfig(t, testDB, nil)
	return schema
}

func buildGraphQLSchemaWithUUIDMappings(t *testing.T, testDB *tidbcloud.TestDB, uuidColumns map[string][]string) graphql.Schema {
	t.Helper()

	schema, _ := buildSchemaWithConfig(t, testDB, uuidColumns)
	return schema
}

func buildMutationSchemaWithUUIDMappings(t *testing.T, testDB *tidbcloud.TestDB, uuidColumns map[string][]string) (graphql.Schema, *dbexec.StandardExecutor) {
	t.Helper()

	schema, executor := buildSchemaWithConfig(t, testDB, uuidColumns)
	return schema, executor
}

func buildSchemaWithConfig(t *testing.T, testDB *tidbcloud.TestDB, uuidColumns map[string][]string) (graphql.Schema, *dbexec.StandardExecutor) {
	t.Helper()

	executor := dbexec.NewStandardExecutor(testDB.DB)
	result, err := schemarefresh.BuildSchema(context.Background(), schemarefresh.BuildSchemaConfig{
		Queryer:                testDB.DB,
		Executor:               executor,
		Databases:              []schemarefresh.DatabaseBuildEntry{{Name: testDB.DatabaseName}},
		GlobalFilters:          schemafilter.Config{},
		UUIDColumns:            uuidColumns,
		TinyInt1BooleanColumns: nil,
		TinyInt1IntColumns:     nil,
		Naming:                 naming.DefaultConfig(),
	})
	require.NoError(t, err)

	return result.GraphQLSchema, executor
}

func mergeEnv(base []string, overrides ...string) []string {
	if len(overrides) == 0 {
		return base
	}

	overrideKeys := make(map[string]struct{}, len(overrides))
	for _, kv := range overrides {
		key := strings.SplitN(kv, "=", 2)[0]
		overrideKeys[key] = struct{}{}
	}

	merged := make([]string, 0, len(base)+len(overrides))
	for _, kv := range base {
		key := strings.SplitN(kv, "=", 2)[0]
		if _, exists := overrideKeys[key]; exists {
			continue
		}
		merged = append(merged, kv)
	}
	merged = append(merged, overrides...)
	return merged
}

func formatServerDebugInfo(stdout, stderr *bytes.Buffer, env []string) string {
	envLines := filterEnv(env, "TIGQL_DATABASE_", "TIGQL_SERVER_", "TIGQL_OBSERVABILITY_")
	return fmt.Sprintf("Environment:\n%s\nSTDOUT:\n%s\nSTDERR:\n%s",
		strings.Join(envLines, "\n"),
		tailString(stdout, 4000),
		tailString(stderr, 4000),
	)
}

func filterEnv(env []string, prefixes ...string) []string {
	if len(env) == 0 {
		return nil
	}
	var filtered []string
	for _, kv := range env {
		for _, prefix := range prefixes {
			if strings.HasPrefix(kv, prefix) {
				filtered = append(filtered, kv)
				break
			}
		}
	}
	return filtered
}

func tailString(buf *bytes.Buffer, max int) string {
	if buf == nil {
		return ""
	}
	s := buf.String()
	if len(s) <= max {
		return s
	}
	return s[len(s)-max:]
}

func requireDecimalAsFloat64(t *testing.T, value interface{}) float64 {
	t.Helper()
	switch v := value.(type) {
	case float64:
		return v
	case string:
		parsed, err := strconv.ParseFloat(v, 64)
		require.NoError(t, err)
		return parsed
	case []byte:
		parsed, err := strconv.ParseFloat(string(v), 64)
		require.NoError(t, err)
		return parsed
	default:
		t.Fatalf("unexpected decimal value type: %T", value)
		return 0
	}
}
