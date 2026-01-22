//go:build integration
// +build integration

package integration

import (
	"fmt"
	"os"
	"strings"
)

func cloudUserWithPrefix() string {
	user := os.Getenv("TIDB_CLOUD_USER")
	prefix := os.Getenv("TIDB_CLOUD_USER_PREFIX")
	if prefix != "" && user != "" && !strings.HasPrefix(user, prefix) {
		return prefix + user
	}
	return user
}

func getEnvOrDefault(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func baseServerEnv() []string {
	return []string{
		fmt.Sprintf("TIGQL_DATABASE_HOST=%s", os.Getenv("TIDB_CLOUD_HOST")),
		fmt.Sprintf("TIGQL_DATABASE_PORT=%s", getEnvOrDefault("TIDB_CLOUD_PORT", "4000")),
		fmt.Sprintf("TIGQL_DATABASE_USER=%s", cloudUserWithPrefix()),
		fmt.Sprintf("TIGQL_DATABASE_PASSWORD=%s", os.Getenv("TIDB_CLOUD_PASSWORD")),
		fmt.Sprintf("TIGQL_DATABASE_DATABASE=%s", getEnvOrDefault("TIDB_CLOUD_DATABASE", "test")),
		fmt.Sprintf("TIGQL_DATABASE_TLS_MODE=%s", getEnvOrDefault("TIDB_CLOUD_TLS_MODE", "skip-verify")),
	}
}
