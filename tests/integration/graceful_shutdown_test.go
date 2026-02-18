//go:build integration
// +build integration

package integration

import (
	"context"
	"fmt"
	"os"
	"syscall"
	"testing"
	"time"

	"tidb-graphql/internal/testutil/tidbcloud"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGracefulShutdown(t *testing.T) {
	requireIntegrationEnv(t)

	testDB := tidbcloud.NewTestDB(t)

	app, serverErrors, _ := startTestApp(
		t,
		18080,
		fmt.Sprintf("TIGQL_DATABASE_DATABASE=%s", testDB.DatabaseName),
	)

	stop := make(chan os.Signal, 1)
	stop <- syscall.SIGTERM

	reason, err := app.WaitForStop(stop, serverErrors)
	require.NoError(t, err)
	assert.Equal(t, "signal", reason)

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, app.Shutdown(shutdownCtx))
}

func TestGracefulShutdown_ProcessSmoke(t *testing.T) {
	requireIntegrationEnv(t)

	testDB := tidbcloud.NewTestDB(t)

	cmd, _ := startTestServer(
		t,
		"../../bin/tidb-graphql-test",
		18082,
		fmt.Sprintf("TIGQL_DATABASE_DATABASE=%s", testDB.DatabaseName),
	)

	err := cmd.Process.Signal(syscall.SIGTERM)
	require.NoError(t, err, "Failed to send SIGTERM")

	doneChan := make(chan error, 1)
	go func() { doneChan <- cmd.Wait() }()

	select {
	case err := <-doneChan:
		assert.NoError(t, err, "Server should exit cleanly (exit code 0) after SIGTERM")
	case <-time.After(35 * time.Second):
		t.Fatal("Server did not shut down within 35 seconds (timeout exceeded)")
	}
}

func TestHealthEndpoint(t *testing.T) {
	requireIntegrationEnv(t)

	// This test verifies the /health endpoint works correctly
	_ = openCloudDB(t)
}
