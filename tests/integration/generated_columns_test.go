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

func TestGenCol_Introspection(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	testDB := tidbcloud.NewTestDB(t)
	testDB.LoadSchema(t, "../fixtures/generated_columns_schema.sql")

	// Introspect database to verify generated columns are discovered
	dbSchema, err := introspection.IntrospectDatabase(testDB.DB, testDB.DatabaseName)
	require.NoError(t, err)
	require.NotNil(t, dbSchema)

	// Find person table
	var personTable *introspection.Table
	for i := range dbSchema.Tables {
		if dbSchema.Tables[i].Name == "person" {
			personTable = &dbSchema.Tables[i]
			break
		}
	}
	require.NotNil(t, personTable, "person table should exist")

	// Build column map for easier assertions
	columns := make(map[string]introspection.Column)
	for _, col := range personTable.Columns {
		columns[col.Name] = col
	}

	// Verify generated columns are discovered with correct types
	cityCol, exists := columns["city"]
	require.True(t, exists, "city column should exist")
	assert.Contains(t, cityCol.DataType, "varchar", "city should be varchar type")
	assert.True(t, cityCol.IsNullable, "virtual generated column should be nullable")

	countryCol, exists := columns["country"]
	require.True(t, exists, "country column should exist")
	assert.Contains(t, countryCol.DataType, "varchar", "country should be varchar type")

	// Verify JSON column is also present
	addressCol, exists := columns["address_info"]
	require.True(t, exists, "address_info column should exist")
	assert.Contains(t, addressCol.DataType, "json", "address_info should be json type")

	// Find products_computed table
	var productsTable *introspection.Table
	for i := range dbSchema.Tables {
		if dbSchema.Tables[i].Name == "products_computed" {
			productsTable = &dbSchema.Tables[i]
			break
		}
	}
	require.NotNil(t, productsTable, "products_computed table should exist")

	// Build column map
	prodColumns := make(map[string]introspection.Column)
	for _, col := range productsTable.Columns {
		prodColumns[col.Name] = col
	}

	// Verify computed numeric column
	totalValueCol, exists := prodColumns["total_value"]
	require.True(t, exists, "total_value column should exist")
	assert.Contains(t, totalValueCol.DataType, "decimal", "total_value should be decimal type")
}

func TestGenCol_VirtualQuery(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	testDB := tidbcloud.NewTestDB(t)
	testDB.LoadSchema(t, "../fixtures/generated_columns_schema.sql")
	testDB.LoadFixtures(t, "../fixtures/generated_columns_seed.sql")

	schema := buildGraphQLSchema(t, testDB)

	// Query virtual generated column (city extracted from JSON)
	query := `
		{
			people(limit: 10) {
				name
				city
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
	people := data["people"].([]interface{})
	require.Len(t, people, 4, "Should have 4 people")

	// Verify city values are extracted from JSON
	cities := make(map[string]bool)
	for _, p := range people {
		person := p.(map[string]interface{})
		if city, ok := person["city"].(string); ok {
			cities[city] = true
		}
	}
	assert.True(t, cities["New York"], "Should have New York")
	assert.True(t, cities["Los Angeles"], "Should have Los Angeles")
	assert.True(t, cities["London"], "Should have London")
	assert.True(t, cities["Paris"], "Should have Paris")
}

func TestGenCol_StoredQuery(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	testDB := tidbcloud.NewTestDB(t)
	testDB.LoadSchema(t, "../fixtures/generated_columns_schema.sql")
	testDB.LoadFixtures(t, "../fixtures/generated_columns_seed.sql")

	schema := buildGraphQLSchema(t, testDB)

	// Query stored generated column (country extracted from JSON)
	query := `
		{
			people(limit: 10) {
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
	require.Empty(t, result.Errors, "Query should not return errors: %v", result.Errors)
	require.NotNil(t, result.Data, "Result data should not be nil")

	data := result.Data.(map[string]interface{})
	people := data["people"].([]interface{})
	require.Len(t, people, 4, "Should have 4 people")

	// Verify country values are extracted from JSON
	countries := make(map[string]bool)
	for _, p := range people {
		person := p.(map[string]interface{})
		if country, ok := person["country"].(string); ok {
			countries[country] = true
		}
	}
	assert.True(t, countries["USA"], "Should have USA")
	assert.True(t, countries["UK"], "Should have UK")
	assert.True(t, countries["France"], "Should have France")
}

func TestGenCol_FilterVirtual(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	testDB := tidbcloud.NewTestDB(t)
	testDB.LoadSchema(t, "../fixtures/generated_columns_schema.sql")
	testDB.LoadFixtures(t, "../fixtures/generated_columns_seed.sql")

	schema := buildGraphQLSchema(t, testDB)

	// Filter on virtual generated column with index
	query := `
		{
			people(where: { city: { eq: "New York" } }, limit: 10) {
				name
				city
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
	people := data["people"].([]interface{})
	require.Len(t, people, 1, "Should find exactly 1 person in New York")

	person := people[0].(map[string]interface{})
	assert.Equal(t, "Alice", person["name"])
	assert.Equal(t, "New York", person["city"])
}

func TestGenCol_FilterStored(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	testDB := tidbcloud.NewTestDB(t)
	testDB.LoadSchema(t, "../fixtures/generated_columns_schema.sql")
	testDB.LoadFixtures(t, "../fixtures/generated_columns_seed.sql")

	schema := buildGraphQLSchema(t, testDB)

	// Filter on stored generated column (total_value > 1000)
	// Widget A: 10.00 * 100 = 1000.00
	// Widget B: 25.50 * 50 = 1275.00
	// Gadget C: 99.99 * 10 = 999.90
	// Gadget D: 5.00 * 500 = 2500.00
	query := `
		{
			productsComputeds(where: { totalValue: { gt: 1000 } }, limit: 10) {
				name
				price
				quantity
				totalValue
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
	products := data["productsComputeds"].([]interface{})
	require.Len(t, products, 2, "Should find 2 products with total_value > 1000")

	// Verify the products are Widget B and Gadget D
	names := make(map[string]bool)
	for _, p := range products {
		product := p.(map[string]interface{})
		names[product["name"].(string)] = true
	}
	assert.True(t, names["Widget B"], "Should include Widget B")
	assert.True(t, names["Gadget D"], "Should include Gadget D")
}

func TestGenCol_UniqueKey(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	testDB := tidbcloud.NewTestDB(t)
	testDB.LoadSchema(t, "../fixtures/generated_columns_schema.sql")
	testDB.LoadFixtures(t, "../fixtures/generated_columns_seed.sql")

	schema := buildGraphQLSchema(t, testDB)

	// Direct lookup via unique index on generated column (country, name)
	query := `
		{
			person_by_country_name(country: "USA", name: "Alice") {
				id
				name
				city
				country
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
	person := data["person_by_country_name"].(map[string]interface{})

	assert.Equal(t, "Alice", person["name"])
	assert.Equal(t, "New York", person["city"])
	assert.Equal(t, "USA", person["country"])
}

func TestGenCol_ComputedNumeric(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	testDB := tidbcloud.NewTestDB(t)
	testDB.LoadSchema(t, "../fixtures/generated_columns_schema.sql")
	testDB.LoadFixtures(t, "../fixtures/generated_columns_seed.sql")

	schema := buildGraphQLSchema(t, testDB)

	// Query computed numeric column and verify calculation
	query := `
		{
			productsComputeds(limit: 10) {
				name
				price
				quantity
				totalValue
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
	products := data["productsComputeds"].([]interface{})
	require.Len(t, products, 4, "Should have 4 products")

	// Verify computed values
	for _, p := range products {
		product := p.(map[string]interface{})
		name := product["name"].(string)
		price := requireDecimalAsFloat64(t, product["price"])
		quantity := product["quantity"].(int)
		totalValue := requireDecimalAsFloat64(t, product["totalValue"])

		expectedTotal := price * float64(quantity)
		assert.InDelta(t, expectedTotal, totalValue, 0.01,
			"total_value for %s should be price * quantity", name)
	}
}

func TestGenCol_WithJSON(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	testDB := tidbcloud.NewTestDB(t)
	testDB.LoadSchema(t, "../fixtures/generated_columns_schema.sql")
	testDB.LoadFixtures(t, "../fixtures/generated_columns_seed.sql")

	schema := buildGraphQLSchema(t, testDB)

	// Query both JSON column and generated columns in same request
	query := `
		{
			people(where: { name: { eq: "Alice" } }, limit: 1) {
				name
				addressInfo
				city
				country
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
	people := data["people"].([]interface{})
	require.Len(t, people, 1, "Should find exactly 1 person")

	person := people[0].(map[string]interface{})
	assert.Equal(t, "Alice", person["name"])

	// JSON column returns raw JSON string
	addressInfo := person["addressInfo"].(string)
	assert.Contains(t, addressInfo, "New York", "JSON should contain city")
	assert.Contains(t, addressInfo, "USA", "JSON should contain country")
	assert.Contains(t, addressInfo, "10001", "JSON should contain zip")

	// Generated columns return extracted values
	assert.Equal(t, "New York", person["city"])
	assert.Equal(t, "USA", person["country"])
}
