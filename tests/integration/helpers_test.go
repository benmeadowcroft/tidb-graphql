//go:build integration
// +build integration

package integration

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"

	"tidb-graphql/internal/dbexec"
	"tidb-graphql/internal/introspection"
	"tidb-graphql/internal/naming"
	"tidb-graphql/internal/nodeid"
	"tidb-graphql/internal/schemafilter"
	"tidb-graphql/internal/schemarefresh"
	"tidb-graphql/internal/testutil/tidbcloud"

	"github.com/graphql-go/graphql"
	"github.com/stretchr/testify/require"
)

func requireIntegrationEnv(t *testing.T) {
	t.Helper()
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}
	if os.Getenv("TIDB_CLOUD_HOST") == "" {
		t.Skip("TiDB Cloud credentials not set")
	}
}

func cloudDSN() string {
	return fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?parseTime=true&tls=%s",
		cloudUserWithPrefix(),
		os.Getenv("TIDB_CLOUD_PASSWORD"),
		os.Getenv("TIDB_CLOUD_HOST"),
		getEnvOrDefault("TIDB_CLOUD_PORT", "4000"),
		getEnvOrDefault("TIDB_CLOUD_DATABASE", "test"),
		getEnvOrDefault("TIDB_CLOUD_TLS_MODE", "skip-verify"),
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

func waitForHealthyWithLogs(t *testing.T, port int, stdout, stderr *bytes.Buffer, env []string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		time.Sleep(200 * time.Millisecond)
		resp, err := http.Get(fmt.Sprintf("http://localhost:%d/health", port))
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return
			}
		}
	}
	t.Fatalf("Server did not become ready within 10 seconds.\n%s", formatServerDebugInfo(stdout, stderr, env))
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
		Queryer:      testDB.DB,
		Executor:     executor,
		DatabaseName: testDB.DatabaseName,
		Filters:      schemafilter.Config{},
		UUIDColumns:  uuidColumns,
		Naming:       naming.DefaultConfig(),
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
