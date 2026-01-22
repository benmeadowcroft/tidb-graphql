//go:build integration
// +build integration

package integration

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"testing"
	"time"

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
	cmd.Env = append(os.Environ(), baseServerEnv()...)
	cmd.Env = append(cmd.Env, fmt.Sprintf("TIGQL_SERVER_PORT=%d", port))
	cmd.Env = append(cmd.Env, extraEnv...)

	err = cmd.Start()
	require.NoError(t, err)

	cleanup := func() {
		if cmd.Process != nil {
			_ = cmd.Process.Kill()
		}
		_ = os.Remove(binaryName)
	}
	t.Cleanup(cleanup)

	waitForHealthy(t, port)

	return cmd, cleanup
}

func waitForHealthy(t *testing.T, port int) {
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
	t.Fatalf("Server did not become ready within 10 seconds")
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
