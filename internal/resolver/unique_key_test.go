package resolver

import (
	"testing"

	_ "github.com/go-sql-driver/mysql"
	"tidb-graphql/internal/introspection"
	"tidb-graphql/internal/naming"
	"tidb-graphql/internal/planner"
	"tidb-graphql/internal/schemafilter"
)

// TestUniqueKeyLookups tests unique key query generation and resolution
func TestUniqueKeyLookups(t *testing.T) {
	// Skip if no database available
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	// This test requires the filtering_schema.sql fixture
	// We'll test the schema generation and query planning logic

	// Create a mock schema with unique indexes
	schema := &introspection.Schema{
		Tables: []introspection.Table{
			{
				Name: "products",
				Columns: []introspection.Column{
					{Name: "id", DataType: "int", IsNullable: false, IsPrimaryKey: true},
					{Name: "sku", DataType: "varchar(50)", IsNullable: false},
					{Name: "manufacturer_id", DataType: "int", IsNullable: false},
					{Name: "name", DataType: "varchar(100)", IsNullable: false},
				},
				Indexes: []introspection.Index{
					{Name: "PRIMARY", Unique: true, Columns: []string{"id"}},
					{Name: "sku", Unique: true, Columns: []string{"sku"}},
					{Name: "uk_manufacturer_sku", Unique: true, Columns: []string{"manufacturer_id", "sku"}},
					{Name: "idx_name", Unique: false, Columns: []string{"name"}},
				},
			},
		},
	}
	renamePrimaryKeyID(&schema.Tables[0])

	// Test 1: Verify single-column unique key query is generated
	t.Run("SingleColumnUniqueKeyQuery", func(t *testing.T) {
		// Create resolver with mock DB (not actually connecting)
		resolver := NewResolver(nil, schema, nil, 0, schemafilter.Config{}, naming.DefaultConfig())

		// Build GraphQL schema
		graphqlSchema, err := resolver.BuildGraphQLSchema()
		if err != nil {
			t.Fatalf("Failed to build GraphQL schema: %v", err)
		}

		// Check that product_by_sku query exists
		queryType := graphqlSchema.QueryType()
		if queryType == nil {
			t.Fatal("Query type is nil")
		}

		fields := queryType.Fields()
		if _, ok := fields["product_by_sku"]; !ok {
			t.Error("product_by_sku query not generated")
		}
	})

	// Test 2: Verify composite unique key query is generated
	t.Run("CompositeUniqueKeyQuery", func(t *testing.T) {
		resolver := NewResolver(nil, schema, nil, 0, schemafilter.Config{}, naming.DefaultConfig())
		graphqlSchema, err := resolver.BuildGraphQLSchema()
		if err != nil {
			t.Fatalf("Failed to build GraphQL schema: %v", err)
		}

		queryType := graphqlSchema.QueryType()
		fields := queryType.Fields()

		if _, ok := fields["product_by_manufacturerId_sku"]; !ok {
			t.Error("product_by_manufacturerId_sku query not generated for composite unique key")
		}
	})

	// Test 3: Verify non-unique indexes don't generate queries
	t.Run("NonUniqueIndexIgnored", func(t *testing.T) {
		resolver := NewResolver(nil, schema, nil, 0, schemafilter.Config{}, naming.DefaultConfig())
		graphqlSchema, err := resolver.BuildGraphQLSchema()
		if err != nil {
			t.Fatalf("Failed to build GraphQL schema: %v", err)
		}

		queryType := graphqlSchema.QueryType()
		fields := queryType.Fields()

		if _, ok := fields["product_by_name"]; ok {
			t.Error("product_by_name query generated for non-unique index (should not exist)")
		}
	})

	// Test 4: Verify PRIMARY key is not duplicated
	t.Run("PrimaryKeyNotDuplicated", func(t *testing.T) {
		resolver := NewResolver(nil, schema, nil, 0, schemafilter.Config{}, naming.DefaultConfig())
		graphqlSchema, err := resolver.BuildGraphQLSchema()
		if err != nil {
			t.Fatalf("Failed to build GraphQL schema: %v", err)
		}

		queryType := graphqlSchema.QueryType()
		fields := queryType.Fields()

		// Should have product (PK lookup) and product_by_databaseId for raw PK lookup
		if _, ok := fields["product"]; !ok {
			t.Error("product PK query not found")
		}
		if _, ok := fields["product_by_databaseId"]; !ok {
			t.Error("product_by_databaseId query not generated for primary key lookup")
		}
	})
}

// TestUniqueKeySQL tests SQL generation for unique key lookups
func TestUniqueKeySQL(t *testing.T) {
	table := introspection.Table{
		Name: "products",
		Columns: []introspection.Column{
			{Name: "id", DataType: "int"},
			{Name: "sku", DataType: "varchar(50)"},
			{Name: "manufacturer_id", DataType: "int"},
			{Name: "name", DataType: "varchar(100)"},
		},
	}

	// Test single-column unique key
	t.Run("SingleColumnUniqueKey", func(t *testing.T) {
		idx := introspection.Index{
			Name:    "sku",
			Unique:  true,
			Columns: []string{"sku"},
		}

		values := map[string]interface{}{
			"sku": "WIDGET-001",
		}

		query, err := planner.PlanUniqueKeyLookup(table, nil, idx, values)
		if err != nil {
			t.Fatalf("Failed to plan unique key lookup: %v", err)
		}

		expectedSQL := "SELECT `id`, `sku`, `manufacturer_id`, `name` FROM `products` WHERE `sku` = ?"
		if query.SQL != expectedSQL {
			t.Errorf("Expected SQL:\n%s\nGot:\n%s", expectedSQL, query.SQL)
		}

		if len(query.Args) != 1 || query.Args[0] != "WIDGET-001" {
			t.Errorf("Expected args [WIDGET-001], got %v", query.Args)
		}
	})

	// Test composite unique key
	t.Run("CompositeUniqueKey", func(t *testing.T) {
		idx := introspection.Index{
			Name:    "uk_manufacturer_sku",
			Unique:  true,
			Columns: []string{"manufacturer_id", "sku"},
		}

		values := map[string]interface{}{
			"manufacturer_id": 1,
			"sku":             "WIDGET-001",
		}

		query, err := planner.PlanUniqueKeyLookup(table, nil, idx, values)
		if err != nil {
			t.Fatalf("Failed to plan unique key lookup: %v", err)
		}

		// SQL can have columns in any order due to map iteration
		if !contains(query.SQL, "`manufacturer_id` = ?") {
			t.Error("SQL missing manufacturer_id condition")
		}
		if !contains(query.SQL, "`sku` = ?") {
			t.Error("SQL missing sku condition")
		}
		if len(query.Args) != 2 {
			t.Errorf("Expected 2 args, got %d", len(query.Args))
		}
	})

	// Test missing value error
	t.Run("MissingValueError", func(t *testing.T) {
		idx := introspection.Index{
			Name:    "uk_manufacturer_sku",
			Unique:  true,
			Columns: []string{"manufacturer_id", "sku"},
		}

		values := map[string]interface{}{
			"sku": "WIDGET-001",
			// Missing manufacturer_id
		}

		_, err := planner.PlanUniqueKeyLookup(table, nil, idx, values)
		if err == nil {
			t.Error("Expected error for missing value, got nil")
		}
	})
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > len(substr) && (s[:len(substr)] == substr || s[len(s)-len(substr):] == substr || containsMiddle(s, substr)))
}

func containsMiddle(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
