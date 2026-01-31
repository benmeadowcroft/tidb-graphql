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

func TestUniqueKeyLookup_SingleColumn(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	testDB := tidbcloud.NewTestDB(t)
	testDB.LoadSchema(t, "../fixtures/filtering_schema.sql")
	testDB.LoadFixtures(t, "../fixtures/filtering_seed.sql")

	schema := buildGraphQLSchema(t, testDB)

	// Test: Lookup product by SKU (single-column unique index)
	query := `
		{
			product_by_sku(sku: "WIDGET-001") {
				id
				sku
				name
				price
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
	product := data["product_by_sku"].(map[string]interface{})

	assert.EqualValues(t, 1, product["id"])
	assert.Equal(t, "WIDGET-001", product["sku"])
	assert.Equal(t, "Blue Widget", product["name"])
	assert.Equal(t, 29.99, product["price"])
}

func TestUniqueKeyLookup_CompositeKey(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	testDB := tidbcloud.NewTestDB(t)
	testDB.LoadSchema(t, "../fixtures/filtering_schema.sql")
	testDB.LoadFixtures(t, "../fixtures/filtering_seed.sql")

	schema := buildGraphQLSchema(t, testDB)

	// Test: Lookup product by composite unique key (manufacturer_id + sku)
	query := `
		{
			product_by_manufacturerId_sku(manufacturerId: 1, sku: "WIDGET-001") {
				id
				sku
				name
				manufacturerId
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
	product := data["product_by_manufacturerId_sku"].(map[string]interface{})

	assert.EqualValues(t, 1, product["id"])
	assert.Equal(t, "WIDGET-001", product["sku"])
	assert.Equal(t, "Blue Widget", product["name"])
	assert.EqualValues(t, 1, product["manufacturerId"])
}

func TestUniqueKeyLookup_NotFound(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	testDB := tidbcloud.NewTestDB(t)
	testDB.LoadSchema(t, "../fixtures/filtering_schema.sql")
	testDB.LoadFixtures(t, "../fixtures/filtering_seed.sql")

	schema := buildGraphQLSchema(t, testDB)

	// Test: Lookup non-existent product
	query := `
		{
			product_by_sku(sku: "NONEXISTENT") {
				id
				sku
				name
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
	product := data["product_by_sku"]

	// Should return null for not found
	assert.Nil(t, product)
}

func TestUniqueKeyLookup_Nullable(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	testDB := tidbcloud.NewTestDB(t)
	testDB.LoadSchema(t, "../fixtures/filtering_schema.sql")
	testDB.LoadFixtures(t, "../fixtures/filtering_seed.sql")

	schema := buildGraphQLSchema(t, testDB)

	// Test: Lookup manufacturer by email (nullable unique field)
	query := `
		{
			manufacturer_by_email(email: "contact@acme.com") {
				id
				name
				email
				country
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
	manufacturer := data["manufacturer_by_email"].(map[string]interface{})

	assert.EqualValues(t, 1, manufacturer["id"])
	assert.Equal(t, "Acme Corp", manufacturer["name"])
	assert.Equal(t, "contact@acme.com", manufacturer["email"])
	assert.Equal(t, "USA", manufacturer["country"])
}

func TestUniqueKeyLookup_WithRelationships(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	testDB := tidbcloud.NewTestDB(t)
	testDB.LoadSchema(t, "../fixtures/filtering_schema.sql")
	testDB.LoadFixtures(t, "../fixtures/filtering_seed.sql")

	schema := buildGraphQLSchema(t, testDB)

	// Test: Lookup order by order number and traverse relationship
	query := `
		{
			order_by_orderNumber(orderNumber: "ORD-2023-0001") {
				id
				orderNumber
				customerEmail
				totalPrice
				status
				product {
					name
					sku
				}
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
	order := data["order_by_orderNumber"].(map[string]interface{})

	assert.EqualValues(t, 1, order["id"])
	assert.Equal(t, "ORD-2023-0001", order["orderNumber"])
	assert.Equal(t, "alice@example.com", order["customerEmail"])
	assert.Equal(t, 59.98, order["totalPrice"])
	assert.Equal(t, "delivered", order["status"])

	// Check relationship
	product := order["product"].(map[string]interface{})
	assert.Equal(t, "Blue Widget", product["name"])
	assert.Equal(t, "WIDGET-001", product["sku"])
}

func TestUniqueKeyLookup_MultipleQueries(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	testDB := tidbcloud.NewTestDB(t)
	testDB.LoadSchema(t, "../fixtures/filtering_schema.sql")
	testDB.LoadFixtures(t, "../fixtures/filtering_seed.sql")

	schema := buildGraphQLSchema(t, testDB)

	// Test: Multiple unique key lookups in one query
	query := `
		{
			widget1: product_by_sku(sku: "WIDGET-001") {
				id
				name
			}
			widget2: product_by_sku(sku: "WIDGET-002") {
				id
				name
			}
			acme: manufacturer_by_name(name: "Acme Corp") {
				id
				name
				country
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

	widget1 := data["widget1"].(map[string]interface{})
	assert.EqualValues(t, 1, widget1["id"])
	assert.Equal(t, "Blue Widget", widget1["name"])

	widget2 := data["widget2"].(map[string]interface{})
	assert.EqualValues(t, 2, widget2["id"])
	assert.Equal(t, "Red Widget", widget2["name"])

	acme := data["acme"].(map[string]interface{})
	assert.EqualValues(t, 1, acme["id"])
	assert.Equal(t, "Acme Corp", acme["name"])
	assert.Equal(t, "USA", acme["country"])
}

func TestUniqueKeyLookup_CategorySlug(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	testDB := tidbcloud.NewTestDB(t)
	testDB.LoadSchema(t, "../fixtures/filtering_schema.sql")
	testDB.LoadFixtures(t, "../fixtures/filtering_seed.sql")

	schema := buildGraphQLSchema(t, testDB)

	// Test: Lookup category by slug
	query := `
		{
			category_by_slug(slug: "electronics") {
				id
				slug
				name
				isVisible
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
	category := data["category_by_slug"].(map[string]interface{})

	assert.EqualValues(t, 1, category["id"])
	assert.Equal(t, "electronics", category["slug"])
	assert.Equal(t, "Electronics", category["name"])
	// TiDB returns TINYINT(1) as int, not bool
	assert.EqualValues(t, 1, category["isVisible"])
}

func TestUniqueKey_Introspection(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	testDB := tidbcloud.NewTestDB(t)
	testDB.LoadSchema(t, "../fixtures/filtering_schema.sql")

	// Introspect database to verify unique indexes are discovered
	dbSchema, err := introspection.IntrospectDatabase(testDB.DB, testDB.DatabaseName)
	require.NoError(t, err)
	require.NotNil(t, dbSchema)

	// Find products table
	var productsTable *introspection.Table
	for i := range dbSchema.Tables {
		if dbSchema.Tables[i].Name == "products" {
			productsTable = &dbSchema.Tables[i]
			break
		}
	}
	require.NotNil(t, productsTable, "products table should exist")

	// Verify unique indexes are discovered
	uniqueIndexCount := 0
	var skuIndex, compositeIndex *introspection.Index
	for i := range productsTable.Indexes {
		idx := &productsTable.Indexes[i]
		if idx.Unique && idx.Name != "PRIMARY" {
			uniqueIndexCount++
			if idx.Name == "sku" || (len(idx.Columns) == 1 && idx.Columns[0] == "sku") {
				skuIndex = idx
			}
			if idx.Name == "uk_manufacturer_sku" {
				compositeIndex = idx
			}
		}
	}

	assert.GreaterOrEqual(t, uniqueIndexCount, 2, "Should have at least 2 unique indexes (sku and uk_manufacturer_sku)")
	assert.NotNil(t, skuIndex, "SKU unique index should be discovered")
	assert.NotNil(t, compositeIndex, "Composite unique index should be discovered")

	// Verify composite index columns
	if compositeIndex != nil {
		assert.Len(t, compositeIndex.Columns, 2, "Composite index should have 2 columns")
		assert.Contains(t, compositeIndex.Columns, "manufacturer_id")
		assert.Contains(t, compositeIndex.Columns, "sku")
	}
}
