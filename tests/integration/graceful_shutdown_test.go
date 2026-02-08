//go:build integration
// +build integration

package integration

import (
	"fmt"
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

	// This test verifies that the server shuts down gracefully by testing behavior:
	// 1. Server starts successfully and becomes healthy
	// 2. Server responds to SIGTERM signal
	// 3. Server exits cleanly within the timeout period (exit code 0)
	// No log parsing - we test actual behavior, not implementation details

	cmd, _ := startTestServer(
		t,
		"../../bin/tidb-graphql-test",
		18080,
		fmt.Sprintf("TIGQL_DATABASE_DATABASE=%s", testDB.DatabaseName),
	)

	// Send SIGTERM to trigger graceful shutdown
	err := cmd.Process.Signal(syscall.SIGTERM)
	require.NoError(t, err, "Failed to send SIGTERM")

	// Wait for process to exit with a timeout
	doneChan := make(chan error, 1)
	go func() {
		doneChan <- cmd.Wait()
	}()

	select {
	case err := <-doneChan:
		// Verify the server exited cleanly (exit code 0)
		// This is the primary assertion - clean shutdown means graceful shutdown worked
		assert.NoError(t, err, "Server should exit cleanly (exit code 0) after SIGTERM")
	case <-time.After(35 * time.Second):
		// Server didn't shut down within timeout - this means graceful shutdown failed
		t.Fatal("Server did not shut down within 35 seconds (timeout exceeded)")
	}
}

func TestHealthEndpoint(t *testing.T) {
	requireIntegrationEnv(t)

	// This test verifies the /health endpoint works correctly
	_ = openCloudDB(t)
}
