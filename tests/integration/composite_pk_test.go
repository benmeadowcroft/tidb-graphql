//go:build integration
// +build integration

package integration

import (
	"context"
	"testing"

	"tidb-graphql/internal/introspection"
	"tidb-graphql/internal/testutil/tidbcloud"

	"github.com/graphql-go/graphql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCompositePK_TwoColumns(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	testDB := tidbcloud.NewTestDB(t)
	testDB.LoadSchema(t, "../fixtures/composite_pk_schema.sql")
	testDB.LoadFixtures(t, "../fixtures/composite_pk_seed.sql")

	schema := buildGraphQLSchema(t, testDB)

	// Test: Lookup order item by composite PK (order_id, product_id)
	query := `
		{
			orderItem(orderId: 100, productId: 2) {
				orderId
				productId
				quantity
				unitPrice
			}
		}
	`
	result := graphql.Do(graphql.Params{
		Schema:        schema,
		RequestString: query,
		Context:       context.Background(),
	})
	require.Empty(t, result.Errors, "Query should not return errors: %v", result.Errors)
	require.NotNil(t, result.Data, "Result data should not be nil")

	data := result.Data.(map[string]interface{})
	item := data["orderItem"].(map[string]interface{})

	assert.EqualValues(t, 100, item["orderId"])
	assert.EqualValues(t, 2, item["productId"])
	assert.EqualValues(t, 1, item["quantity"])
	assert.EqualValues(t, 49.99, item["unitPrice"])
}

func TestCompositePK_ThreeColumns(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	testDB := tidbcloud.NewTestDB(t)
	testDB.LoadSchema(t, "../fixtures/composite_pk_schema.sql")
	testDB.LoadFixtures(t, "../fixtures/composite_pk_seed.sql")

	schema := buildGraphQLSchema(t, testDB)

	// Test: Lookup inventory location by composite PK (warehouse_id, aisle, shelf)
	query := `
		{
			inventoryLocation(warehouseId: 1, aisle: "B", shelf: 2) {
				warehouseId
				aisle
				shelf
				productId
				quantity
			}
		}
	`
	result := graphql.Do(graphql.Params{
		Schema:        schema,
		RequestString: query,
		Context:       context.Background(),
	})
	require.Empty(t, result.Errors, "Query should not return errors: %v", result.Errors)
	require.NotNil(t, result.Data, "Result data should not be nil")

	data := result.Data.(map[string]interface{})
	location := data["inventoryLocation"].(map[string]interface{})

	assert.EqualValues(t, 1, location["warehouseId"])
	assert.Equal(t, "B", location["aisle"])
	assert.EqualValues(t, 2, location["shelf"])
	assert.EqualValues(t, 4, location["productId"])
	assert.EqualValues(t, 75, location["quantity"])
}

func TestCompositePK_NotFound(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	testDB := tidbcloud.NewTestDB(t)
	testDB.LoadSchema(t, "../fixtures/composite_pk_schema.sql")
	testDB.LoadFixtures(t, "../fixtures/composite_pk_seed.sql")

	schema := buildGraphQLSchema(t, testDB)

	// Test: Lookup non-existent order item
	query := `
		{
			orderItem(orderId: 999, productId: 999) {
				orderId
				productId
			}
		}
	`
	result := graphql.Do(graphql.Params{
		Schema:        schema,
		RequestString: query,
		Context:       context.Background(),
	})
	require.Empty(t, result.Errors, "Query should not return errors")
	require.NotNil(t, result.Data, "Result data should not be nil")

	data := result.Data.(map[string]interface{})
	item := data["orderItem"]
	assert.Nil(t, item, "Non-existent item should return null")
}

func TestCompositePK_SingleColumnStillWorks(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	testDB := tidbcloud.NewTestDB(t)
	testDB.LoadSchema(t, "../fixtures/composite_pk_schema.sql")
	testDB.LoadFixtures(t, "../fixtures/composite_pk_seed.sql")

	schema := buildGraphQLSchema(t, testDB)

	// Test: Single-column PK still works as expected
	query := `
		{
			warehouse(id: 1) {
				id
				name
				location
			}
		}
	`
	result := graphql.Do(graphql.Params{
		Schema:        schema,
		RequestString: query,
		Context:       context.Background(),
	})
	require.Empty(t, result.Errors, "Query should not return errors: %v", result.Errors)
	require.NotNil(t, result.Data, "Result data should not be nil")

	data := result.Data.(map[string]interface{})
	warehouse := data["warehouse"].(map[string]interface{})

	assert.EqualValues(t, 1, warehouse["id"])
	assert.Equal(t, "Main Warehouse", warehouse["name"])
	assert.Equal(t, "New York", warehouse["location"])
}

func TestCompositePK_Introspection(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	testDB := tidbcloud.NewTestDB(t)
	testDB.LoadSchema(t, "../fixtures/composite_pk_schema.sql")

	// Introspect database to verify composite primary keys are discovered
	dbSchema, err := introspection.IntrospectDatabase(testDB.DB, testDB.DatabaseName)
	require.NoError(t, err)
	require.NotNil(t, dbSchema)

	// Find order_items table
	var orderItemsTable *introspection.Table
	for i := range dbSchema.Tables {
		if dbSchema.Tables[i].Name == "order_items" {
			orderItemsTable = &dbSchema.Tables[i]
			break
		}
	}
	require.NotNil(t, orderItemsTable, "order_items table should exist")

	// Verify composite primary key columns
	pkCols := introspection.PrimaryKeyColumns(*orderItemsTable)
	assert.Len(t, pkCols, 2, "order_items should have 2 primary key columns")
	assert.Equal(t, "order_id", pkCols[0].Name)
	assert.Equal(t, "product_id", pkCols[1].Name)

	// Find inventory_locations table
	var inventoryTable *introspection.Table
	for i := range dbSchema.Tables {
		if dbSchema.Tables[i].Name == "inventory_locations" {
			inventoryTable = &dbSchema.Tables[i]
			break
		}
	}
	require.NotNil(t, inventoryTable, "inventory_locations table should exist")

	// Verify three-column composite primary key
	pkCols = introspection.PrimaryKeyColumns(*inventoryTable)
	assert.Len(t, pkCols, 3, "inventory_locations should have 3 primary key columns")
	assert.Equal(t, "warehouse_id", pkCols[0].Name)
	assert.Equal(t, "aisle", pkCols[1].Name)
	assert.Equal(t, "shelf", pkCols[2].Name)
}

func TestCompositePK_ListQuery(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	testDB := tidbcloud.NewTestDB(t)
	testDB.LoadSchema(t, "../fixtures/composite_pk_schema.sql")
	testDB.LoadFixtures(t, "../fixtures/composite_pk_seed.sql")

	schema := buildGraphQLSchema(t, testDB)

	// Test: List query still works for tables with composite PK
	query := `
		{
			orderItems(where: {orderId: {eq: 100}}, limit: 10) {
				orderId
				productId
				quantity
			}
		}
	`
	result := graphql.Do(graphql.Params{
		Schema:        schema,
		RequestString: query,
		Context:       context.Background(),
	})
	require.Empty(t, result.Errors, "Query should not return errors: %v", result.Errors)
	require.NotNil(t, result.Data, "Result data should not be nil")

	data := result.Data.(map[string]interface{})
	items := data["orderItems"].([]interface{})

	assert.Len(t, items, 3, "Order 100 should have 3 items")
}
