//go:build integration
// +build integration

package integration

import (
	"context"
	"testing"

	"tidb-graphql/internal/testutil/tidbcloud"

	"github.com/graphql-go/graphql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWhereFiltering_Eq(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	testDB := tidbcloud.NewTestDB(t)
	testDB.LoadSchema(t, "../fixtures/filtering_schema.sql")
	testDB.LoadFixtures(t, "../fixtures/filtering_seed.sql")

	schema := buildGraphQLSchema(t, testDB)

	// Test: Filter products by exact price match (uses indexed column)
	query := `
		{
			products(where: { price: { eq: 29.99 } }, limit: 10) {
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
	products := data["products"].([]interface{})

	// Should return only products with price = 29.99
	require.GreaterOrEqual(t, len(products), 1, "Should have at least one product with price 29.99")
	for _, p := range products {
		product := p.(map[string]interface{})
		assert.Equal(t, 29.99, requireDecimalAsFloat64(t, product["price"]))
	}
}

func TestWhereFiltering_Gt(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	testDB := tidbcloud.NewTestDB(t)
	testDB.LoadSchema(t, "../fixtures/filtering_schema.sql")
	testDB.LoadFixtures(t, "../fixtures/filtering_seed.sql")

	schema := buildGraphQLSchema(t, testDB)

	// Test: Filter products with price > 50 (uses indexed column)
	query := `
		{
			products(where: { price: { gt: 50 } }, limit: 10) {
				id
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
	products := data["products"].([]interface{})

	// All products should have price > 50
	for _, p := range products {
		product := p.(map[string]interface{})
		price := requireDecimalAsFloat64(t, product["price"])
		assert.Greater(t, price, 50.0)
	}
}

func TestWhereFiltering_GteLte(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	testDB := tidbcloud.NewTestDB(t)
	testDB.LoadSchema(t, "../fixtures/filtering_schema.sql")
	testDB.LoadFixtures(t, "../fixtures/filtering_seed.sql")

	schema := buildGraphQLSchema(t, testDB)

	// Test: Filter products with price between 20 and 50 (uses indexed column)
	query := `
		{
			products(where: { price: { gte: 20, lte: 50 } }, limit: 10) {
				id
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
	products := data["products"].([]interface{})

	// All products should have 20 <= price <= 50
	require.GreaterOrEqual(t, len(products), 1, "Should have at least one product in range")
	for _, p := range products {
		product := p.(map[string]interface{})
		price := requireDecimalAsFloat64(t, product["price"])
		assert.GreaterOrEqual(t, price, 20.0)
		assert.LessOrEqual(t, price, 50.0)
	}
}

func TestWhereFiltering_In(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	testDB := tidbcloud.NewTestDB(t)
	testDB.LoadSchema(t, "../fixtures/filtering_schema.sql")
	testDB.LoadFixtures(t, "../fixtures/filtering_seed.sql")

	schema := buildGraphQLSchema(t, testDB)

	// Test: Filter products by ID IN list (uses indexed PK column)
	query := `
		{
			products(where: { databaseId: { in: [1, 2, 3] } }, limit: 10) {
				databaseId
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
	products := data["products"].([]interface{})

	// Should return exactly 3 products
	require.Equal(t, 3, len(products), "Should return exactly 3 products")
	for _, p := range products {
		product := p.(map[string]interface{})
		id := int(product["databaseId"].(int))
		assert.Contains(t, []int{1, 2, 3}, id)
	}
}

func TestWhereFiltering_Like(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	testDB := tidbcloud.NewTestDB(t)
	testDB.LoadSchema(t, "../fixtures/filtering_schema.sql")
	testDB.LoadFixtures(t, "../fixtures/filtering_seed.sql")

	schema := buildGraphQLSchema(t, testDB)

	// Test: Filter products by SKU pattern (uses indexed sku column)
	query := `
		{
			products(where: { sku: { like: "WIDGET%" } }, limit: 10) {
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
	products := data["products"].([]interface{})

	// All products should have SKU starting with "WIDGET"
	require.GreaterOrEqual(t, len(products), 1, "Should have at least one Widget product")
	for _, p := range products {
		product := p.(map[string]interface{})
		sku := product["sku"].(string)
		assert.Contains(t, sku, "WIDGET")
	}
}

func TestWhereFiltering_IsNull(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	testDB := tidbcloud.NewTestDB(t)
	testDB.LoadSchema(t, "../fixtures/filtering_schema.sql")
	testDB.LoadFixtures(t, "../fixtures/filtering_seed.sql")

	schema := buildGraphQLSchema(t, testDB)

	// Test: Filter manufacturers with NULL email (uses indexed email column)
	query := `
		{
			manufacturers(where: { email: { isNull: true } }, limit: 10) {
				id
				name
				email
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
	manufacturers := data["manufacturers"].([]interface{})

	// All manufacturers should have null email
	for _, m := range manufacturers {
		manufacturer := m.(map[string]interface{})
		assert.Nil(t, manufacturer["email"])
	}
}

func TestWhereFiltering_IsNotNull(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	testDB := tidbcloud.NewTestDB(t)
	testDB.LoadSchema(t, "../fixtures/filtering_schema.sql")
	testDB.LoadFixtures(t, "../fixtures/filtering_seed.sql")

	schema := buildGraphQLSchema(t, testDB)

	// Test: Filter manufacturers with non-NULL email (uses indexed email column)
	query := `
		{
			manufacturers(where: { email: { isNull: false } }, limit: 10) {
				id
				name
				email
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
	manufacturers := data["manufacturers"].([]interface{})

	// All manufacturers should have non-null email
	require.GreaterOrEqual(t, len(manufacturers), 1, "Should have at least one manufacturer with email")
	for _, m := range manufacturers {
		manufacturer := m.(map[string]interface{})
		assert.NotNil(t, manufacturer["email"])
	}
}

func TestWhereFiltering_AND(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	testDB := tidbcloud.NewTestDB(t)
	testDB.LoadSchema(t, "../fixtures/filtering_schema.sql")
	testDB.LoadFixtures(t, "../fixtures/filtering_seed.sql")

	schema := buildGraphQLSchema(t, testDB)

	// Test: Filter products with price > 20 AND isActive = true (uses indexed columns)
	query := `
		{
			products(
				where: {
					AND: [
						{ price: { gt: 20 } },
						{ databaseId: { gte: 1 } }
					]
				},
				limit: 10
			) {
				databaseId
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
	products := data["products"].([]interface{})

	// All products should meet both conditions
	require.GreaterOrEqual(t, len(products), 1, "Should have at least one product matching both conditions")
	for _, p := range products {
		product := p.(map[string]interface{})
		price := requireDecimalAsFloat64(t, product["price"])
		id := int(product["databaseId"].(int))
		assert.Greater(t, price, 20.0)
		assert.GreaterOrEqual(t, id, 1)
	}
}

func TestWhereFiltering_OR(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	testDB := tidbcloud.NewTestDB(t)
	testDB.LoadSchema(t, "../fixtures/filtering_schema.sql")
	testDB.LoadFixtures(t, "../fixtures/filtering_seed.sql")

	schema := buildGraphQLSchema(t, testDB)

	// Test: Filter products with price < 25 OR price > 100 (uses indexed price column)
	query := `
		{
			products(
				where: {
					OR: [
						{ price: { lt: 25 } },
						{ price: { gt: 100 } }
					]
				},
				limit: 10
			) {
				id
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
	products := data["products"].([]interface{})

	// All products should meet at least one condition
	require.GreaterOrEqual(t, len(products), 1, "Should have at least one product matching either condition")
	for _, p := range products {
		product := p.(map[string]interface{})
		price := requireDecimalAsFloat64(t, product["price"])
		assert.True(t, price < 25.0 || price > 100.0, "Product price should be < 25 OR > 100")
	}
}

func TestWhereFiltering_ComplexANDOR(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	testDB := tidbcloud.NewTestDB(t)
	testDB.LoadSchema(t, "../fixtures/filtering_schema.sql")
	testDB.LoadFixtures(t, "../fixtures/filtering_seed.sql")

	schema := buildGraphQLSchema(t, testDB)

	// Test: Complex query with nested AND/OR (uses indexed columns)
	// (price >= 20 AND price <= 50) OR (price >= 100)
	query := `
		{
			products(
				where: {
					OR: [
						{
							AND: [
								{ price: { gte: 20 } },
								{ price: { lte: 50 } }
							]
						},
						{ price: { gte: 100 } }
					]
				},
				limit: 10
			) {
				id
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
	products := data["products"].([]interface{})

	// All products should be either in range [20, 50] OR >= 100
	require.GreaterOrEqual(t, len(products), 1, "Should have at least one product matching the complex condition")
	for _, p := range products {
		product := p.(map[string]interface{})
		price := requireDecimalAsFloat64(t, product["price"])
		inRange := price >= 20.0 && price <= 50.0
		highPrice := price >= 100.0
		assert.True(t, inRange || highPrice, "Product price should be in [20, 50] OR >= 100")
	}
}

func TestWhereFiltering_IndexedCol(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	testDB := tidbcloud.NewTestDB(t)
	testDB.LoadSchema(t, "../fixtures/filtering_schema.sql")
	testDB.LoadFixtures(t, "../fixtures/filtering_seed.sql")

	schema := buildGraphQLSchema(t, testDB)

	// Test: Query with non-indexed column should fail
	query := `
		{
			products(where: { name: { eq: "Blue Widget" } }, limit: 10) {
				id
				name
			}
		}
	`
	result := graphql.Do(graphql.Params{
		Schema:        schema,
		RequestString: query,
		Context:       context.Background(),
	})

	// Should return an error about requiring indexed column
	require.NotEmpty(t, result.Errors, "Query should return error for non-indexed column")
	assert.Contains(t, result.Errors[0].Message, "indexed column")
}
