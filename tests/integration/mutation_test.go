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

	// Business errors are represented in mutation data unions, not result.Errors.
	// This helper is only used by tests that expect rollback semantics.
	mc.MarkError()

	// Finalize the transaction (should rollback due to errors)
	err = mc.Finalize()
	require.NoError(t, err, "Failed to finalize transaction")

	return result
}

func mutationResultField(t *testing.T, result *graphql.Result, fieldName string) map[string]interface{} {
	t.Helper()
	require.Empty(t, result.Errors, "Mutation should not return GraphQL errors: %v", result.Errors)
	require.NotNil(t, result.Data, "Mutation result data should not be nil")
	data := result.Data.(map[string]interface{})
	raw, ok := data[fieldName]
	require.True(t, ok, "Mutation field %q missing from response", fieldName)
	out, ok := raw.(map[string]interface{})
	require.True(t, ok, "Mutation field %q should return an object, got %T", fieldName, raw)
	return out
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
				__typename
				... on CreateCategorySuccess {
					category {
						id
						name
						description
					}
				}
				... on MutationError {
					message
				}
			}
		}
	`
	result := executeMutation(t, schema, executor, mutation, nil)
	wrapper := mutationResultField(t, result, "createCategory")
	assert.Equal(t, "CreateCategorySuccess", wrapper["__typename"])
	category := wrapper["category"].(map[string]interface{})

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
				__typename
				... on CreateCategorySuccess {
					category {
						id
						databaseId
						name
					}
				}
				... on MutationError {
					message
				}
			}
		}
	`
	result := executeMutation(t, schema, executor, mutation, nil)
	wrapper := mutationResultField(t, result, "createCategory")
	assert.Equal(t, "CreateCategorySuccess", wrapper["__typename"])
	category := wrapper["category"].(map[string]interface{})

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
				__typename
				... on CreateProductSuccess {
					product {
						id
						sku
						name
						price
						categoryId
					}
				}
				... on MutationError {
					message
				}
			}
		}
	`
	result := executeMutation(t, schema, executor, mutation, nil)
	wrapper := mutationResultField(t, result, "createProduct")
	assert.Equal(t, "CreateProductSuccess", wrapper["__typename"])
	product := wrapper["product"].(map[string]interface{})

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
				__typename
				... on CreateOrderItemSuccess {
					orderItem {
						orderId
						lineNumber
						productId
						quantity
						unitPrice
					}
				}
				... on MutationError {
					message
				}
			}
		}
	`
	result := executeMutation(t, schema, executor, mutation, nil)
	wrapper := mutationResultField(t, result, "createOrderItem")
	assert.Equal(t, "CreateOrderItemSuccess", wrapper["__typename"])
	item := wrapper["orderItem"].(map[string]interface{})

	assert.EqualValues(t, 200, item["orderId"])
	assert.EqualValues(t, 1, item["lineNumber"])
	assert.EqualValues(t, 1, item["productId"])
	assert.EqualValues(t, 5, item["quantity"])
}

func TestMutation_CreateCategoryWithNestedProducts(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	testDB := tidbcloud.NewTestDB(t)
	testDB.LoadSchema(t, "../fixtures/mutation_schema.sql")
	testDB.LoadFixtures(t, "../fixtures/mutation_seed.sql")

	schema, executor := buildMutationSchema(t, testDB)

	mutation := `
		mutation {
			createCategory(input: {
				name: "Nested Category"
				description: "Created with nested products"
				productsCreate: [
					{
						sku: "NEST-001"
						name: "Nested Product 1"
						price: 12.34
					}
				]
			}) {
				__typename
				... on CreateCategorySuccess {
					category {
						databaseId
						name
						products(first: 10) {
							nodes {
								sku
								categoryId
							}
						}
					}
				}
				... on MutationError {
					__typename
					message
				}
			}
		}
	`

	result := executeMutation(t, schema, executor, mutation, nil)
	wrapper := mutationResultField(t, result, "createCategory")
	assert.Equal(t, "CreateCategorySuccess", wrapper["__typename"])

	category := wrapper["category"].(map[string]interface{})
	assert.Equal(t, "Nested Category", category["name"])
	categoryID := category["databaseId"]

	products := requireCollectionNodes(t, category, "products")
	require.Len(t, products, 1)
	product := products[0].(map[string]interface{})
	assert.Equal(t, "NEST-001", product["sku"])
	assert.Equal(t, categoryID, product["categoryId"])
}

func TestMutation_CreateProduct_ConnectScalarXORValidation(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	testDB := tidbcloud.NewTestDB(t)
	testDB.LoadSchema(t, "../fixtures/mutation_schema.sql")
	testDB.LoadFixtures(t, "../fixtures/mutation_seed.sql")

	schema, executor := buildMutationSchema(t, testDB)

	mutation := `
		mutation {
			createProduct(input: {
				categoryId: 1
				categoryConnect: { byName: { name: "Electronics" } }
				sku: "XOR-001"
				name: "Xor Product"
				price: 9.99
			}) {
				__typename
				... on CreateProductSuccess {
					product { databaseId }
				}
				... on InputValidationError {
					message
				}
				... on MutationError {
					__typename
					message
				}
			}
		}
	`

	result := executeMutationExpectRollback(t, schema, executor, mutation, nil)
	wrapper := mutationResultField(t, result, "createProduct")
	assert.Equal(t, "InputValidationError", wrapper["__typename"])
	assert.Contains(t, wrapper["message"], "either categoryId or categoryConnect")
}

func TestMutation_CreateProduct_ConnectNotFoundValidation(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	testDB := tidbcloud.NewTestDB(t)
	testDB.LoadSchema(t, "../fixtures/mutation_schema.sql")
	testDB.LoadFixtures(t, "../fixtures/mutation_seed.sql")

	schema, executor := buildMutationSchema(t, testDB)

	mutation := `
		mutation {
			createProduct(input: {
				categoryConnect: { byName: { name: "DoesNotExist" } }
				sku: "NF-001"
				name: "Not Found Product"
				price: 2.99
			}) {
				__typename
				... on CreateProductSuccess {
					product { databaseId }
				}
				... on InputValidationError {
					message
				}
				... on MutationError {
					__typename
					message
				}
			}
		}
	`

	result := executeMutationExpectRollback(t, schema, executor, mutation, nil)
	wrapper := mutationResultField(t, result, "createProduct")
	assert.Equal(t, "InputValidationError", wrapper["__typename"])
	assert.Contains(t, wrapper["message"], "not found")
}

func TestMutation_NestedCreateRollbackOnChildConflict(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	testDB := tidbcloud.NewTestDB(t)
	testDB.LoadSchema(t, "../fixtures/mutation_schema.sql")
	testDB.LoadFixtures(t, "../fixtures/mutation_seed.sql")

	schema, executor := buildMutationSchema(t, testDB)

	// Ensure target category does not exist before mutation.
	var beforeExists bool
	err := testDB.DB.QueryRow("SELECT EXISTS(SELECT 1 FROM categories WHERE name = 'Rollback Nested Category')").Scan(&beforeExists)
	require.NoError(t, err)
	require.False(t, beforeExists)

	// Child row uses an existing product SKU (ELEC-001), triggering unique violation.
	mutation := `
		mutation {
			createCategory(input: {
				name: "Rollback Nested Category"
				productsCreate: [
					{
						sku: "ELEC-001"
						name: "Should Fail"
						price: 1.23
					}
				]
			}) {
				__typename
				... on CreateCategorySuccess {
					category { databaseId }
				}
				... on ConflictError {
					message
				}
				... on MutationError {
					__typename
					message
				}
			}
		}
	`
	result := executeMutationExpectRollback(t, schema, executor, mutation, nil)
	wrapper := mutationResultField(t, result, "createCategory")
	assert.Equal(t, "ConflictError", wrapper["__typename"])

	// Parent insert must be rolled back.
	var afterExists bool
	err = testDB.DB.QueryRow("SELECT EXISTS(SELECT 1 FROM categories WHERE name = 'Rollback Nested Category')").Scan(&afterExists)
	require.NoError(t, err)
	assert.False(t, afterExists, "parent row should be rolled back when nested child insert fails")
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
				__typename
				... on UpdateCategorySuccess {
					category {
						id
						databaseId
						name
						description
					}
				}
				... on MutationError {
					message
				}
			}
		}
	`
	result := executeMutation(t, schema, executor, mutation, nil)
	wrapper := mutationResultField(t, result, "updateCategory")
	assert.Equal(t, "UpdateCategorySuccess", wrapper["__typename"])
	category := wrapper["category"].(map[string]interface{})

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
				__typename
				... on UpdateCategorySuccess {
					category {
						id
						databaseId
						name
						description
					}
				}
				... on MutationError {
					message
				}
			}
		}
	`
	result := executeMutation(t, schema, executor, mutation, nil)
	wrapper := mutationResultField(t, result, "updateCategory")
	assert.Equal(t, "UpdateCategorySuccess", wrapper["__typename"])
	category := wrapper["category"].(map[string]interface{})

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

	// Test: Update non-existent row should return success wrapper with null entity
	nodeID := nodeIDForTable("categories", 999)
	mutation := `
		mutation {
			updateCategory(id: "` + nodeID + `", set: {description: "New description"}) {
				__typename
				... on UpdateCategorySuccess {
					category {
						id
						name
					}
				}
				... on MutationError {
					message
				}
			}
		}
	`
	result := executeMutation(t, schema, executor, mutation, nil)
	wrapper := mutationResultField(t, result, "updateCategory")
	assert.Equal(t, "UpdateCategorySuccess", wrapper["__typename"])
	category := wrapper["category"]
	assert.Nil(t, category, "Update of non-existent row should return success with null category")
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
				__typename
				... on UpdateOrderItemSuccess {
					orderItem {
						orderId
						lineNumber
						quantity
						unitPrice
					}
				}
				... on MutationError {
					message
				}
			}
		}
	`
	result := executeMutation(t, schema, executor, mutation, nil)
	wrapper := mutationResultField(t, result, "updateOrderItem")
	assert.Equal(t, "UpdateOrderItemSuccess", wrapper["__typename"])
	item := wrapper["orderItem"].(map[string]interface{})

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
				__typename
				... on CreateAuditLogSuccess {
					auditLog {
						id
					}
				}
				... on MutationError {
					message
				}
			}
		}
	`
	result := executeMutation(t, schema, executor, mutation, nil)
	createdWrapper := mutationResultField(t, result, "createAuditLog")
	assert.Equal(t, "CreateAuditLogSuccess", createdWrapper["__typename"])
	created := createdWrapper["auditLog"].(map[string]interface{})
	createdID := created["id"].(string)

	// Now delete it
	deleteMutation := `
		mutation($id: ID!) {
			deleteAuditLog(id: $id) {
				__typename
				... on DeleteAuditLogSuccess {
					id
				}
				... on MutationError {
					message
				}
			}
		}
	`
	result = executeMutation(t, schema, executor, deleteMutation, map[string]interface{}{"id": createdID})
	deleted := mutationResultField(t, result, "deleteAuditLog")
	assert.Equal(t, "DeleteAuditLogSuccess", deleted["__typename"])
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

	// Test: Delete non-existent row should return NotFoundError
	nodeID := nodeIDForTable("audit_log", 999)
	mutation := `
		mutation {
			deleteAuditLog(id: "` + nodeID + `") {
				__typename
				... on DeleteAuditLogSuccess {
					id
				}
				... on NotFoundError {
					message
				}
				... on MutationError {
					message
				}
			}
		}
	`
	result := executeMutation(t, schema, executor, mutation, nil)
	deleted := mutationResultField(t, result, "deleteAuditLog")
	assert.Equal(t, "NotFoundError", deleted["__typename"])
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
				__typename
				... on DeleteOrderItemSuccess {
					orderId
					lineNumber
				}
				... on MutationError {
					message
				}
			}
		}
	`
	result := executeMutation(t, schema, executor, mutation, nil)
	deleted := mutationResultField(t, result, "deleteOrderItem")
	assert.Equal(t, "DeleteOrderItemSuccess", deleted["__typename"])

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

	// Test: Create category with duplicate name (should return ConflictError in data)
	mutation := `
		mutation {
			createCategory(input: {name: "Electronics"}) {
				__typename
				... on CreateCategorySuccess {
					category {
						id
						name
					}
				}
				... on ConflictError {
					message
				}
				... on MutationError {
					message
				}
			}
		}
	`
	result := executeMutationExpectRollback(t, schema, executor, mutation, nil)
	wrapper := mutationResultField(t, result, "createCategory")
	assert.Equal(t, "ConflictError", wrapper["__typename"])
}

func TestMutation_ForeignKeyViolation(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	testDB := tidbcloud.NewTestDB(t)
	testDB.LoadSchema(t, "../fixtures/mutation_schema.sql")
	testDB.LoadFixtures(t, "../fixtures/mutation_seed.sql")

	schema, executor := buildMutationSchema(t, testDB)

	// Test: Create product with non-existent category (should return ConstraintError in data)
	mutation := `
		mutation {
			createProduct(input: {categoryId: 999, sku: "FAIL-001", name: "Will Fail", price: 10.00}) {
				__typename
				... on CreateProductSuccess {
					product {
						id
					}
				}
				... on ConstraintError {
					message
				}
				... on MutationError {
					message
				}
			}
		}
	`
	result := executeMutationExpectRollback(t, schema, executor, mutation, nil)
	wrapper := mutationResultField(t, result, "createProduct")
	assert.Equal(t, "ConstraintError", wrapper["__typename"])
}

func TestMutation_ForeignKeyDeleteRestrict(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	testDB := tidbcloud.NewTestDB(t)
	testDB.LoadSchema(t, "../fixtures/mutation_schema.sql")
	testDB.LoadFixtures(t, "../fixtures/mutation_seed.sql")

	schema, executor := buildMutationSchema(t, testDB)

	// Test: Delete category that has products (should return ConstraintError in data)
	nodeID := nodeIDForTable("categories", 1)
	mutation := `
		mutation {
			deleteCategory(id: "` + nodeID + `") {
				__typename
				... on DeleteCategorySuccess {
					id
				}
				... on ConstraintError {
					message
				}
				... on MutationError {
					message
				}
			}
		}
	`
	result := executeMutationExpectRollback(t, schema, executor, mutation, nil)
	wrapper := mutationResultField(t, result, "deleteCategory")
	assert.Equal(t, "ConstraintError", wrapper["__typename"])
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
				__typename
				... on CreateInventorySuccess {
					inventory {
						id
						productId
						quantity
						unitCost
						totalValue
						location
					}
				}
				... on MutationError {
					message
				}
			}
		}
	`
	result := executeMutation(t, schema, executor, mutation, nil)
	wrapper := mutationResultField(t, result, "createInventory")
	assert.Equal(t, "CreateInventorySuccess", wrapper["__typename"])
	inventory := wrapper["inventory"].(map[string]interface{})

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
				__typename
				... on CreateCategorySuccess {
					category {
						id
						name
					}
				}
				... on MutationError {
					message
				}
			}
		}
	`
	result1 := graphql.Do(graphql.Params{
		Schema:        schema,
		RequestString: mutation1,
		Context:       ctx,
	})
	require.Empty(t, result1.Errors, "First mutation should succeed")
	wrapper1 := mutationResultField(t, result1, "createCategory")
	assert.Equal(t, "CreateCategorySuccess", wrapper1["__typename"])

	// Second mutation should fail (duplicate unique key)
	mutation2 := `
		mutation {
			createCategory(input: {name: "Electronics"}) {
				__typename
				... on CreateCategorySuccess {
					category {
						id
					}
				}
				... on ConflictError {
					message
				}
				... on MutationError {
					message
				}
			}
		}
	`
	result2 := graphql.Do(graphql.Params{
		Schema:        schema,
		RequestString: mutation2,
		Context:       ctx,
	})
	require.Empty(t, result2.Errors, "Second mutation should not produce GraphQL errors")
	wrapper2 := mutationResultField(t, result2, "createCategory")
	assert.Equal(t, "ConflictError", wrapper2["__typename"])

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
