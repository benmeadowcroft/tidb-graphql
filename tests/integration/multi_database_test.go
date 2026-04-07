//go:build integration
// +build integration

package integration

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"tidb-graphql/internal/config"
	"tidb-graphql/internal/introspection"
	"tidb-graphql/internal/nodeid"
	"tidb-graphql/internal/schemarefresh"
	"tidb-graphql/internal/testutil/tidbcloud"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMultiDatabaseApp_QueryNamespacesAndCrossDatabaseRelationship(t *testing.T) {
	requireIntegrationEnv(t)

	storeDB, shippingDB := setupMultiDatabaseFixture(t)

	cfg := buildBaseTestConfig(18083)
	cfg.Database.Database = storeDB.DatabaseName
	cfg.Database.Databases = []config.DatabaseEntryConfig{
		{Name: storeDB.DatabaseName, Namespace: "store"},
		{Name: shippingDB.DatabaseName, Namespace: "shipping"},
	}

	_, _, _ = startTestAppWithConfig(t, cfg)

	data := executeGraphQLHTTP(t, cfg.Server.Port, `
		{
			store {
				users(first: 10) {
					nodes {
						id
						name
					}
				}
			}
			shipping {
				shipments(first: 10) {
					nodes {
						id
						carrier
						order {
							id
							status
							user {
								id
								name
							}
						}
					}
				}
			}
		}
	`)

	store := asMap(t, data["store"])
	storeUsers := asConnectionNodes(t, store["users"])
	require.Len(t, storeUsers, 1)
	firstUser := asMap(t, storeUsers[0])
	assert.Equal(t, "Alice", firstUser["name"])
	requireNodeID(t, firstUser["id"], "Store_User", float64(1))

	shipping := asMap(t, data["shipping"])
	shipments := asConnectionNodes(t, shipping["shipments"])
	require.Len(t, shipments, 1)
	firstShipment := asMap(t, shipments[0])
	assert.Equal(t, "UPS", firstShipment["carrier"])
	requireNodeID(t, firstShipment["id"], "Shipping_Shipment", float64(100))

	order := asMap(t, firstShipment["order"])
	assert.Equal(t, "PAID", order["status"])
	requireNodeID(t, order["id"], "Store_Order", float64(10))

	user := asMap(t, order["user"])
	assert.Equal(t, "Alice", user["name"])
	requireNodeID(t, user["id"], "Store_User", float64(1))
}

func TestMultiDatabaseSchemaRefresh_ReintrospectsChangedSecondaryDatabase(t *testing.T) {
	requireIntegrationEnv(t)

	storeDB, shippingDB := setupMultiDatabaseFixture(t)

	manager, err := schemarefresh.NewManager(context.Background(), schemarefresh.Config{
		DB:           storeDB.DB,
		DatabaseName: storeDB.DatabaseName,
		SchemaEntries: []schemarefresh.DatabaseBuildEntry{
			{Name: storeDB.DatabaseName, Namespace: "store"},
			{Name: shippingDB.DatabaseName, Namespace: "shipping"},
		},
		MinInterval: time.Minute,
		MaxInterval: 5 * time.Minute,
	})
	require.NoError(t, err)

	initial := manager.CurrentSnapshot()
	require.NotNil(t, initial)
	require.False(t, schemaHasColumn(initial.DBSchema, shippingDB.DatabaseName, "shipments", "tracking_number"))
	require.True(t, schemaHasColumn(initial.DBSchema, storeDB.DatabaseName, "users", "name"))

	_, err = shippingDB.DB.Exec(`ALTER TABLE shipments ADD COLUMN tracking_number VARCHAR(64) NULL`)
	require.NoError(t, err)

	refreshCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	require.NoError(t, manager.RefreshNowContext(refreshCtx))

	refreshed := manager.CurrentSnapshot()
	require.NotNil(t, refreshed)
	require.True(t, schemaHasColumn(refreshed.DBSchema, shippingDB.DatabaseName, "shipments", "tracking_number"))
	require.True(t, schemaHasColumn(refreshed.DBSchema, storeDB.DatabaseName, "users", "name"))
}

func setupMultiDatabaseFixture(t *testing.T) (*tidbcloud.TestDB, *tidbcloud.TestDB) {
	t.Helper()

	storeDB := tidbcloud.NewTestDB(t)
	shippingDB := tidbcloud.NewTestDB(t)

	execStatements(t, storeDB.DB,
		`CREATE TABLE users (
			id BIGINT PRIMARY KEY,
			name VARCHAR(100) NOT NULL
		)`,
		`CREATE TABLE orders (
			id BIGINT PRIMARY KEY,
			user_id BIGINT NOT NULL,
			status VARCHAR(32) NOT NULL,
			CONSTRAINT fk_orders_user FOREIGN KEY (user_id) REFERENCES users(id)
		)`,
		`INSERT INTO users (id, name) VALUES (1, 'Alice')`,
		`INSERT INTO orders (id, user_id, status) VALUES (10, 1, 'PAID')`,
	)

	execStatements(t, shippingDB.DB,
		fmt.Sprintf(`CREATE TABLE shipments (
			id BIGINT PRIMARY KEY,
			order_id BIGINT NOT NULL,
			carrier VARCHAR(64) NOT NULL,
			CONSTRAINT fk_shipments_order FOREIGN KEY (order_id) REFERENCES %s.orders(id)
		)`, quoteDatabaseName(storeDB.DatabaseName)),
		`INSERT INTO shipments (id, order_id, carrier) VALUES (100, 10, 'UPS')`,
	)

	return storeDB, shippingDB
}

func execStatements(t *testing.T, db *sql.DB, statements ...string) {
	t.Helper()

	for _, stmt := range statements {
		_, err := db.Exec(stmt)
		require.NoError(t, err, "failed SQL: %s", stmt)
	}
}

func quoteDatabaseName(name string) string {
	return "`" + name + "`"
}

func asMap(t *testing.T, value interface{}) map[string]interface{} {
	t.Helper()
	result, ok := value.(map[string]interface{})
	require.True(t, ok, "expected map, got %T", value)
	return result
}

func asConnectionNodes(t *testing.T, value interface{}) []interface{} {
	t.Helper()
	connection := asMap(t, value)
	nodes, ok := connection["nodes"].([]interface{})
	require.True(t, ok, "expected nodes array, got %T", connection["nodes"])
	return nodes
}

func schemaHasColumn(schema *introspection.Schema, databaseName, tableName, columnName string) bool {
	if schema == nil {
		return false
	}
	for _, table := range schema.Tables {
		if table.Key.Database != databaseName || table.Name != tableName {
			continue
		}
		for _, column := range table.Columns {
			if column.Name == columnName {
				return true
			}
		}
	}
	return false
}

func requireNodeID(t *testing.T, value interface{}, wantType string, wantPK interface{}) {
	t.Helper()

	nodeID, ok := value.(string)
	require.True(t, ok, "expected node id string, got %T", value)

	typeName, values, err := nodeid.Decode(nodeID)
	require.NoError(t, err)
	require.Equal(t, wantType, typeName)
	require.Len(t, values, 1)
	require.Equal(t, normalizeNodeIDValue(t, wantPK), normalizeNodeIDValue(t, values[0]))
}

func normalizeNodeIDValue(t *testing.T, value interface{}) interface{} {
	t.Helper()

	switch v := value.(type) {
	case json.Number:
		if i, err := v.Int64(); err == nil {
			return i
		}
		if f, err := v.Float64(); err == nil {
			return f
		}
		return v.String()
	case int:
		return int64(v)
	case int8:
		return int64(v)
	case int16:
		return int64(v)
	case int32:
		return int64(v)
	case int64:
		return v
	case uint:
		return uint64(v)
	case uint8:
		return uint64(v)
	case uint16:
		return uint64(v)
	case uint32:
		return uint64(v)
	case uint64:
		return v
	case float32:
		return float64(v)
	case float64:
		if v == float64(int64(v)) {
			return int64(v)
		}
		return v
	default:
		return value
	}
}
