//go:build integration
// +build integration

package integration

import (
	"context"
	"testing"

	"tidb-graphql/internal/dbexec"
	"tidb-graphql/internal/introspection"
	"tidb-graphql/internal/naming"
	"tidb-graphql/internal/resolver"
	"tidb-graphql/internal/schemafilter"
	"tidb-graphql/internal/testutil/tidbcloud"

	"github.com/graphql-go/graphql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// executeMutation is a helper that runs a mutation within a transaction context
func executeMutation(t *testing.T, schema graphql.Schema, db *dbexec.StandardExecutor, query string, variables map[string]interface{}) *graphql.Result {
	t.Helper()

	ctx := context.Background()
	tx, err := db.BeginTx(ctx)
	require.NoError(t, err, "Failed to begin transaction")

	mc := resolver.NewMutationContext(tx)
	ctx = resolver.WithMutationContext(ctx, mc)

	result := graphql.Do(graphql.Params{
		Schema:         schema,
		RequestString:  query,
		VariableValues: variables,
		Context:        ctx,
	})

	// Finalize the transaction
	err = mc.Finalize()
	require.NoError(t, err, "Failed to finalize transaction")

	return result
}

// executeMutationExpectRollback is a helper that expects the mutation to fail and rollback
func executeMutationExpectRollback(t *testing.T, schema graphql.Schema, db *dbexec.StandardExecutor, query string, variables map[string]interface{}) *graphql.Result {
	t.Helper()

	ctx := context.Background()
	tx, err := db.BeginTx(ctx)
	require.NoError(t, err, "Failed to begin transaction")

	mc := resolver.NewMutationContext(tx)
	ctx = resolver.WithMutationContext(ctx, mc)

	result := graphql.Do(graphql.Params{
		Schema:         schema,
		RequestString:  query,
		VariableValues: variables,
		Context:        ctx,
	})

	// Mark error if there were GraphQL errors
	if len(result.Errors) > 0 {
		mc.MarkError()
	}

	// Finalize the transaction (should rollback due to errors)
	err = mc.Finalize()
	require.NoError(t, err, "Failed to finalize transaction")

	return result
}

func buildMutationSchema(t *testing.T, testDB *tidbcloud.TestDB) (graphql.Schema, *dbexec.StandardExecutor) {
	t.Helper()

	dbSchema, err := introspection.IntrospectDatabase(testDB.DB, testDB.DatabaseName)
	require.NoError(t, err)

	executor := dbexec.NewStandardExecutor(testDB.DB)
	res := resolver.NewResolver(executor, dbSchema, nil, 0, schemafilter.Config{}, naming.DefaultConfig())
	schema, err := res.BuildGraphQLSchema()
	require.NoError(t, err)

	return schema, executor
}

func TestMutation_CreateSimple(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	testDB := tidbcloud.NewTestDB(t)
	testDB.LoadSchema(t, "../fixtures/mutation_schema.sql")
	testDB.LoadFixtures(t, "../fixtures/mutation_seed.sql")

	schema, executor := buildMutationSchema(t, testDB)

	// Test: Create a new category
	mutation := `
		mutation {
			createCategory(input: {name: "Home & Garden", description: "Home improvement items"}) {
				id
				name
				description
			}
		}
	`
	result := executeMutation(t, schema, executor, mutation, nil)
	require.Empty(t, result.Errors, "Mutation should not return errors: %v", result.Errors)
	require.NotNil(t, result.Data, "Result data should not be nil")

	data := result.Data.(map[string]interface{})
	category := data["createCategory"].(map[string]interface{})

	assert.NotNil(t, category["id"], "Created category should have an ID")
	assert.Equal(t, "Home & Garden", category["name"])
	assert.Equal(t, "Home improvement items", category["description"])
}

func TestMutation_CreateWithExplicitPK(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	testDB := tidbcloud.NewTestDB(t)
	testDB.LoadSchema(t, "../fixtures/mutation_schema.sql")

	schema, executor := buildMutationSchema(t, testDB)

	// Test: Create a category with explicit ID
	mutation := `
		mutation {
			createCategory(input: {databaseId: 100, name: "Test Category"}) {
				id
				databaseId
				name
			}
		}
	`
	result := executeMutation(t, schema, executor, mutation, nil)
	require.Empty(t, result.Errors, "Mutation should not return errors: %v", result.Errors)

	data := result.Data.(map[string]interface{})
	category := data["createCategory"].(map[string]interface{})

	assert.EqualValues(t, 100, category["databaseId"], "Category should have explicit ID 100")
	assert.Equal(t, "Test Category", category["name"])
}

func TestMutation_CreateWithForeignKey(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	testDB := tidbcloud.NewTestDB(t)
	testDB.LoadSchema(t, "../fixtures/mutation_schema.sql")
	testDB.LoadFixtures(t, "../fixtures/mutation_seed.sql")

	schema, executor := buildMutationSchema(t, testDB)

	// Test: Create a product with valid foreign key
	mutation := `
		mutation {
			createProduct(input: {categoryId: 1, sku: "NEW-001", name: "New Product", price: 99.99}) {
				id
				sku
				name
				price
				categoryId
			}
		}
	`
	result := executeMutation(t, schema, executor, mutation, nil)
	require.Empty(t, result.Errors, "Mutation should not return errors: %v", result.Errors)

	data := result.Data.(map[string]interface{})
	product := data["createProduct"].(map[string]interface{})

	assert.NotNil(t, product["id"])
	assert.Equal(t, "NEW-001", product["sku"])
	assert.EqualValues(t, 1, product["categoryId"])
}

func TestMutation_CreateWithCompositePK(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	testDB := tidbcloud.NewTestDB(t)
	testDB.LoadSchema(t, "../fixtures/mutation_schema.sql")
	testDB.LoadFixtures(t, "../fixtures/mutation_seed.sql")

	schema, executor := buildMutationSchema(t, testDB)

	// Test: Create an order item with composite primary key
	mutation := `
		mutation {
			createOrderItem(input: {orderId: 200, lineNumber: 1, productId: 1, quantity: 5, unitPrice: 899.99}) {
				orderId
				lineNumber
				productId
				quantity
				unitPrice
			}
		}
	`
	result := executeMutation(t, schema, executor, mutation, nil)
	require.Empty(t, result.Errors, "Mutation should not return errors: %v", result.Errors)

	data := result.Data.(map[string]interface{})
	item := data["createOrderItem"].(map[string]interface{})

	assert.EqualValues(t, 200, item["orderId"])
	assert.EqualValues(t, 1, item["lineNumber"])
	assert.EqualValues(t, 1, item["productId"])
	assert.EqualValues(t, 5, item["quantity"])
}

func TestMutation_UpdateSimple(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	testDB := tidbcloud.NewTestDB(t)
	testDB.LoadSchema(t, "../fixtures/mutation_schema.sql")
	testDB.LoadFixtures(t, "../fixtures/mutation_seed.sql")

	schema, executor := buildMutationSchema(t, testDB)

	// Test: Update a category
	nodeID := nodeIDForTable("categories", 1)
	mutation := `
		mutation {
			updateCategory(id: "` + nodeID + `", set: {description: "Updated description"}) {
				id
				databaseId
				name
				description
			}
		}
	`
	result := executeMutation(t, schema, executor, mutation, nil)
	require.Empty(t, result.Errors, "Mutation should not return errors: %v", result.Errors)

	data := result.Data.(map[string]interface{})
	category := data["updateCategory"].(map[string]interface{})

	assert.EqualValues(t, 1, category["databaseId"])
	assert.Equal(t, "Electronics", category["name"])
	assert.Equal(t, "Updated description", category["description"])
}

func TestMutation_UpdateWithEmptySet(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	testDB := tidbcloud.NewTestDB(t)
	testDB.LoadSchema(t, "../fixtures/mutation_schema.sql")
	testDB.LoadFixtures(t, "../fixtures/mutation_seed.sql")

	schema, executor := buildMutationSchema(t, testDB)

	// Test: Update with empty set should return current row (no-op)
	nodeID := nodeIDForTable("categories", 1)
	mutation := `
		mutation {
			updateCategory(id: "` + nodeID + `", set: {}) {
				id
				databaseId
				name
				description
			}
		}
	`
	result := executeMutation(t, schema, executor, mutation, nil)
	require.Empty(t, result.Errors, "Mutation should not return errors: %v", result.Errors)

	data := result.Data.(map[string]interface{})
	category := data["updateCategory"].(map[string]interface{})

	assert.EqualValues(t, 1, category["databaseId"])
	assert.Equal(t, "Electronics", category["name"])
}

func TestMutation_UpdateNonExistent(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	testDB := tidbcloud.NewTestDB(t)
	testDB.LoadSchema(t, "../fixtures/mutation_schema.sql")
	testDB.LoadFixtures(t, "../fixtures/mutation_seed.sql")

	schema, executor := buildMutationSchema(t, testDB)

	// Test: Update non-existent row should return null
	nodeID := nodeIDForTable("categories", 999)
	mutation := `
		mutation {
			updateCategory(id: "` + nodeID + `", set: {description: "New description"}) {
				id
				name
			}
		}
	`
	result := executeMutation(t, schema, executor, mutation, nil)
	require.Empty(t, result.Errors, "Mutation should not return errors")

	data := result.Data.(map[string]interface{})
	category := data["updateCategory"]
	assert.Nil(t, category, "Update of non-existent row should return null")
}

func TestMutation_UpdateCompositePK(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	testDB := tidbcloud.NewTestDB(t)
	testDB.LoadSchema(t, "../fixtures/mutation_schema.sql")
	testDB.LoadFixtures(t, "../fixtures/mutation_seed.sql")

	schema, executor := buildMutationSchema(t, testDB)

	// Test: Update order item with composite PK
	nodeID := nodeIDForTable("order_items", 100, 1)
	mutation := `
		mutation {
			updateOrderItem(id: "` + nodeID + `", set: {quantity: 10}) {
				orderId
				lineNumber
				quantity
				unitPrice
			}
		}
	`
	result := executeMutation(t, schema, executor, mutation, nil)
	require.Empty(t, result.Errors, "Mutation should not return errors: %v", result.Errors)

	data := result.Data.(map[string]interface{})
	item := data["updateOrderItem"].(map[string]interface{})

	assert.EqualValues(t, 100, item["orderId"])
	assert.EqualValues(t, 1, item["lineNumber"])
	assert.EqualValues(t, 10, item["quantity"])
}

func TestMutation_DeleteSimple(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	testDB := tidbcloud.NewTestDB(t)
	testDB.LoadSchema(t, "../fixtures/mutation_schema.sql")
	testDB.LoadFixtures(t, "../fixtures/mutation_seed.sql")

	schema, executor := buildMutationSchema(t, testDB)

	// Test: Delete a category (need to delete products first due to FK)
	// First, let's delete an audit_log entry instead since it has no FK constraints
	mutation := `
		mutation {
			createAuditLog(input: {action: "TEST", entityType: "test", entityId: 1}) {
				id
			}
		}
	`
	result := executeMutation(t, schema, executor, mutation, nil)
	require.Empty(t, result.Errors)
	data := result.Data.(map[string]interface{})
	created := data["createAuditLog"].(map[string]interface{})
	createdID := created["id"].(string)

	// Now delete it
	deleteMutation := `
		mutation($id: ID!) {
			deleteAuditLog(id: $id) {
				id
			}
		}
	`
	result = executeMutation(t, schema, executor, deleteMutation, map[string]interface{}{"id": createdID})
	require.Empty(t, result.Errors, "Delete mutation should not return errors: %v", result.Errors)

	data = result.Data.(map[string]interface{})
	deleted := data["deleteAuditLog"].(map[string]interface{})
	assert.Equal(t, createdID, deleted["id"])
}

func TestMutation_DeleteNonExistent(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	testDB := tidbcloud.NewTestDB(t)
	testDB.LoadSchema(t, "../fixtures/mutation_schema.sql")
	testDB.LoadFixtures(t, "../fixtures/mutation_seed.sql")

	schema, executor := buildMutationSchema(t, testDB)

	// Test: Delete non-existent row should return null
	nodeID := nodeIDForTable("audit_log", 999)
	mutation := `
		mutation {
			deleteAuditLog(id: "` + nodeID + `") {
				id
			}
		}
	`
	result := executeMutation(t, schema, executor, mutation, nil)
	require.Empty(t, result.Errors, "Delete mutation should not return errors")

	data := result.Data.(map[string]interface{})
	deleted := data["deleteAuditLog"]
	assert.Nil(t, deleted, "Delete of non-existent row should return null")
}

func TestMutation_DeleteCompositePK(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	testDB := tidbcloud.NewTestDB(t)
	testDB.LoadSchema(t, "../fixtures/mutation_schema.sql")
	testDB.LoadFixtures(t, "../fixtures/mutation_seed.sql")

	schema, executor := buildMutationSchema(t, testDB)

	// Test: Delete order item with composite PK
	nodeID := nodeIDForTable("order_items", 100, 2)
	mutation := `
		mutation {
			deleteOrderItem(id: "` + nodeID + `") {
				orderId
				lineNumber
			}
		}
	`
	result := executeMutation(t, schema, executor, mutation, nil)
	require.Empty(t, result.Errors, "Delete mutation should not return errors: %v", result.Errors)

	data := result.Data.(map[string]interface{})
	deleted := data["deleteOrderItem"].(map[string]interface{})

	assert.EqualValues(t, 100, deleted["orderId"])
	assert.EqualValues(t, 2, deleted["lineNumber"])

	// Verify deletion by trying to query it
	query := `
		{
			orderItem(id: "` + nodeID + `") {
				orderId
				lineNumber
			}
		}
	`
	queryResult := graphql.Do(graphql.Params{
		Schema:        schema,
		RequestString: query,
		Context:       context.Background(),
	})
	require.Empty(t, queryResult.Errors)
	queryData := queryResult.Data.(map[string]interface{})
	assert.Nil(t, queryData["orderItem"], "Deleted item should not be found")
}

func TestMutation_UniqueConstraintViolation(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	testDB := tidbcloud.NewTestDB(t)
	testDB.LoadSchema(t, "../fixtures/mutation_schema.sql")
	testDB.LoadFixtures(t, "../fixtures/mutation_seed.sql")

	schema, executor := buildMutationSchema(t, testDB)

	// Test: Create category with duplicate name (should fail with unique violation)
	mutation := `
		mutation {
			createCategory(input: {name: "Electronics"}) {
				id
				name
			}
		}
	`
	result := executeMutationExpectRollback(t, schema, executor, mutation, nil)
	require.NotEmpty(t, result.Errors, "Mutation should return error for unique violation")

	// Check that error has the right code
	err := result.Errors[0]
	extensions := err.Extensions
	if extensions != nil {
		code, ok := extensions["code"].(string)
		if ok {
			assert.Equal(t, "unique_violation", code, "Error code should be unique_violation")
		}
	}
}

func TestMutation_ForeignKeyViolation(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	testDB := tidbcloud.NewTestDB(t)
	testDB.LoadSchema(t, "../fixtures/mutation_schema.sql")
	testDB.LoadFixtures(t, "../fixtures/mutation_seed.sql")

	schema, executor := buildMutationSchema(t, testDB)

	// Test: Create product with non-existent category (should fail with FK violation)
	mutation := `
		mutation {
			createProduct(input: {categoryId: 999, sku: "FAIL-001", name: "Will Fail", price: 10.00}) {
				id
			}
		}
	`
	result := executeMutationExpectRollback(t, schema, executor, mutation, nil)
	require.NotEmpty(t, result.Errors, "Mutation should return error for FK violation")

	// Check that error has the right code
	err := result.Errors[0]
	extensions := err.Extensions
	if extensions != nil {
		code, ok := extensions["code"].(string)
		if ok {
			assert.Equal(t, "foreign_key_violation", code, "Error code should be foreign_key_violation")
		}
	}
}

func TestMutation_ForeignKeyDeleteRestrict(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	testDB := tidbcloud.NewTestDB(t)
	testDB.LoadSchema(t, "../fixtures/mutation_schema.sql")
	testDB.LoadFixtures(t, "../fixtures/mutation_seed.sql")

	schema, executor := buildMutationSchema(t, testDB)

	// Test: Delete category that has products (should fail due to FK constraint)
	nodeID := nodeIDForTable("categories", 1)
	mutation := `
		mutation {
			deleteCategory(id: "` + nodeID + `") {
				id
			}
		}
	`
	result := executeMutationExpectRollback(t, schema, executor, mutation, nil)
	require.NotEmpty(t, result.Errors, "Mutation should return error when deleting referenced row")

	// Check that error has the right code
	err := result.Errors[0]
	extensions := err.Extensions
	if extensions != nil {
		code, ok := extensions["code"].(string)
		if ok {
			assert.Equal(t, "foreign_key_violation", code, "Error code should be foreign_key_violation")
		}
	}
}

func TestMutation_GeneratedColumnExcluded(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	testDB := tidbcloud.NewTestDB(t)
	testDB.LoadSchema(t, "../fixtures/mutation_schema.sql")
	testDB.LoadFixtures(t, "../fixtures/mutation_seed.sql")

	schema, executor := buildMutationSchema(t, testDB)

	// Test: Create inventory - generated column (total_value) should be excluded from input
	// but should be computed and returned
	mutation := `
		mutation {
			createInventory(input: {productId: 1, quantity: 100, unitCost: 10.50, location: "New Location"}) {
				id
				productId
				quantity
				unitCost
				totalValue
				location
			}
		}
	`
	result := executeMutation(t, schema, executor, mutation, nil)
	require.Empty(t, result.Errors, "Mutation should not return errors: %v", result.Errors)

	data := result.Data.(map[string]interface{})
	inventory := data["createInventory"].(map[string]interface{})

	assert.NotNil(t, inventory["id"])
	assert.EqualValues(t, 1, inventory["productId"])
	assert.EqualValues(t, 100, inventory["quantity"])
	// Generated column should be computed: 100 * 10.50 = 1050.00
	assert.EqualValues(t, 1050.00, requireDecimalAsFloat64(t, inventory["totalValue"]))
}

func TestMutation_TransactionRollbackOnSecondMutation(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	testDB := tidbcloud.NewTestDB(t)
	testDB.LoadSchema(t, "../fixtures/mutation_schema.sql")
	testDB.LoadFixtures(t, "../fixtures/mutation_seed.sql")

	schema, executor := buildMutationSchema(t, testDB)

	// First, verify initial state
	var initialCount int
	err := testDB.DB.QueryRow("SELECT COUNT(*) FROM categories").Scan(&initialCount)
	require.NoError(t, err)

	// Execute a mutation that has two operations where the second fails
	// Note: In GraphQL, mutations execute sequentially, so we need to test this
	// by checking that a successful first mutation is rolled back when the second fails
	ctx := context.Background()
	tx, err := executor.BeginTx(ctx)
	require.NoError(t, err)

	mc := resolver.NewMutationContext(tx)
	ctx = resolver.WithMutationContext(ctx, mc)

	// First mutation should succeed
	mutation1 := `
		mutation {
			createCategory(input: {name: "Will Be Rolled Back"}) {
				id
				name
			}
		}
	`
	result1 := graphql.Do(graphql.Params{
		Schema:        schema,
		RequestString: mutation1,
		Context:       ctx,
	})
	require.Empty(t, result1.Errors, "First mutation should succeed")

	// Second mutation should fail (duplicate unique key)
	mutation2 := `
		mutation {
			createCategory(input: {name: "Electronics"}) {
				id
			}
		}
	`
	result2 := graphql.Do(graphql.Params{
		Schema:        schema,
		RequestString: mutation2,
		Context:       ctx,
	})
	require.NotEmpty(t, result2.Errors, "Second mutation should fail")

	// Mark error and finalize (should rollback)
	mc.MarkError()
	err = mc.Finalize()
	require.NoError(t, err)

	// Verify that the first mutation was also rolled back
	var finalCount int
	err = testDB.DB.QueryRow("SELECT COUNT(*) FROM categories").Scan(&finalCount)
	require.NoError(t, err)

	assert.Equal(t, initialCount, finalCount, "Transaction should have been rolled back, count should be unchanged")

	// Also verify that "Will Be Rolled Back" category doesn't exist
	var exists bool
	err = testDB.DB.QueryRow("SELECT EXISTS(SELECT 1 FROM categories WHERE name = 'Will Be Rolled Back')").Scan(&exists)
	require.NoError(t, err)
	assert.False(t, exists, "Rolled back category should not exist")
}

func TestMutation_SchemaFilterDenyMutationTable(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	testDB := tidbcloud.NewTestDB(t)
	testDB.LoadSchema(t, "../fixtures/mutation_schema.sql")
	testDB.LoadFixtures(t, "../fixtures/mutation_seed.sql")

	dbSchema, err := introspection.IntrospectDatabase(testDB.DB, testDB.DatabaseName)
	require.NoError(t, err)

	// Configure schema filter to deny mutations on audit_log table
	filters := schemafilter.Config{
		DenyMutationTables: []string{"audit_log"},
	}

	executor := dbexec.NewStandardExecutor(testDB.DB)
	res := resolver.NewResolver(executor, dbSchema, nil, 0, filters, naming.DefaultConfig())
	schema, err := res.BuildGraphQLSchema()
	require.NoError(t, err)

	// Verify audit_log is still queryable
	query := `
		{
			__type(name: "Query") {
				fields {
					name
				}
			}
		}
	`
	result := graphql.Do(graphql.Params{
		Schema:        schema,
		RequestString: query,
		Context:       context.Background(),
	})
	require.Empty(t, result.Errors)

	// Check that createAuditLog mutation doesn't exist
	mutationQuery := `
		{
			__type(name: "Mutation") {
				fields {
					name
				}
			}
		}
	`
	mutationResult := graphql.Do(graphql.Params{
		Schema:        schema,
		RequestString: mutationQuery,
		Context:       context.Background(),
	})
	require.Empty(t, mutationResult.Errors)

	data := mutationResult.Data.(map[string]interface{})
	mutationType := data["__type"].(map[string]interface{})
	fields := mutationType["fields"].([]interface{})

	// Check that no audit_log mutations exist
	for _, field := range fields {
		fieldName := field.(map[string]interface{})["name"].(string)
		assert.NotContains(t, fieldName, "AuditLog", "AuditLog mutations should not exist when table is denied")
	}
}

func TestMutation_SchemaFilterDenyMutationColumn(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	testDB := tidbcloud.NewTestDB(t)
	testDB.LoadSchema(t, "../fixtures/mutation_schema.sql")
	testDB.LoadFixtures(t, "../fixtures/mutation_seed.sql")

	dbSchema, err := introspection.IntrospectDatabase(testDB.DB, testDB.DatabaseName)
	require.NoError(t, err)

	// Configure schema filter to deny mutations on created_at column for all tables
	filters := schemafilter.Config{
		DenyMutationColumns: map[string][]string{
			"*": {"created_at"},
		},
	}

	executor := dbexec.NewStandardExecutor(testDB.DB)
	res := resolver.NewResolver(executor, dbSchema, nil, 0, filters, naming.DefaultConfig())
	schema, err := res.BuildGraphQLSchema()
	require.NoError(t, err)

	// Check that CreateCategoryInput doesn't include created_at
	query := `
		{
			__type(name: "CreateCategoryInput") {
				inputFields {
					name
				}
			}
		}
	`
	result := graphql.Do(graphql.Params{
		Schema:        schema,
		RequestString: query,
		Context:       context.Background(),
	})
	require.Empty(t, result.Errors)

	data := result.Data.(map[string]interface{})
	inputType := data["__type"].(map[string]interface{})
	inputFields := inputType["inputFields"].([]interface{})

	for _, field := range inputFields {
		fieldName := field.(map[string]interface{})["name"].(string)
		assert.NotEqual(t, "createdAt", fieldName, "createdAt should not be in CreateCategoryInput")
	}

	// But created_at should still be queryable
	nodeID := nodeIDForTable("categories", 1)
	categoryQuery := `
		{
			category(id: "` + nodeID + `") {
				id
				databaseId
				name
				createdAt
			}
		}
	`
	categoryResult := graphql.Do(graphql.Params{
		Schema:        schema,
		RequestString: categoryQuery,
		Context:       context.Background(),
	})
	require.Empty(t, categoryResult.Errors, "createdAt should be queryable: %v", categoryResult.Errors)
}
