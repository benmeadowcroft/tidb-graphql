//go:build integration
// +build integration

package integration

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"tidb-graphql/internal/introspection"
	"tidb-graphql/internal/logging"
	"tidb-graphql/internal/schemarefresh"
	"tidb-graphql/internal/testutil/tidbcloud"
)

func TestSchemaRefresh_ReintrospectAfterDDL(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	testDB := tidbcloud.NewTestDB(t)

	testDB.LoadSchema(t, "../fixtures/schema_refresh_v1.sql")

	logger := logging.NewLogger(logging.Config{Level: "info", Format: "text"})
	manager, err := schemarefresh.NewManager(schemarefresh.Config{
		DB:           testDB.DB,
		DatabaseName: testDB.DatabaseName,
		Logger:       logger,
	})
	require.NoError(t, err)

	snapshot := manager.CurrentSnapshot()
	require.NotNil(t, snapshot)

	table := findTable(t, snapshot.DBSchema, "products")
	require.True(t, hasColumn(table, "name"))
	require.False(t, hasColumn(table, "sku"))

	testDB.LoadSchema(t, "../fixtures/schema_refresh_v2.sql")

	refreshCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	require.NoError(t, manager.RefreshNowContext(refreshCtx))

	snapshot = manager.CurrentSnapshot()
	require.NotNil(t, snapshot)

	table = findTable(t, snapshot.DBSchema, "products")
	require.True(t, hasColumn(table, "sku"))
}

func findTable(t *testing.T, schema *introspection.Schema, name string) introspection.Table {
	t.Helper()

	for _, table := range schema.Tables {
		if table.Name == name {
			return table
		}
	}
	t.Fatalf("table %s not found", name)
	return introspection.Table{}
}

func hasColumn(table introspection.Table, name string) bool {
	for _, column := range table.Columns {
		if column.Name == name {
			return true
		}
	}
	return false
}
