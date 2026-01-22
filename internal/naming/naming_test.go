package naming

import (
	"bytes"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestToGraphQLTypeName(t *testing.T) {
	namer := Default()

	tests := []struct {
		input    string
		expected string
	}{
		{"users", "Users"},
		{"user_profiles", "UserProfiles"},
		{"order_items", "OrderItems"},
		{"api_v2_endpoints", "ApiV2Endpoints"},
		{"a", "A"},
		{"", ""},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result := namer.ToGraphQLTypeName(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestToGraphQLFieldName(t *testing.T) {
	namer := Default()

	tests := []struct {
		input    string
		expected string
	}{
		{"user_name", "userName"},
		{"created_at", "createdAt"},
		{"id", "id"},
		{"user_profile_id", "userProfileId"},
		{"api_v2_key", "apiV2Key"},
		{"", ""},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result := namer.ToGraphQLFieldName(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestPluralize(t *testing.T) {
	namer := Default()

	tests := []struct {
		input    string
		expected string
	}{
		{"user", "users"},
		{"category", "categories"},
		{"person", "people"},
		{"child", "children"},
		{"status", "statuses"},
		{"analysis", "analyses"},
		{"orderItem", "orderItems"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result := namer.Pluralize(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestSingularize(t *testing.T) {
	namer := Default()

	tests := []struct {
		input    string
		expected string
	}{
		{"users", "user"},
		{"categories", "category"},
		{"people", "person"},
		{"children", "child"},
		{"statuses", "status"},
		{"analyses", "analysis"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result := namer.Singularize(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestPluralizeWithOverrides(t *testing.T) {
	cfg := Config{
		PluralOverrides: map[string]string{
			"staff": "staff", // Same singular/plural
		},
		SingularOverrides: make(map[string]string),
	}
	namer := New(cfg, nil)

	assert.Equal(t, "staff", namer.Pluralize("staff"))
	assert.Equal(t, "users", namer.Pluralize("user")) // Falls back to library
}

func TestSingularizeWithOverrides(t *testing.T) {
	cfg := Config{
		PluralOverrides: make(map[string]string),
		SingularOverrides: map[string]string{
			"data": "datum",
		},
	}
	namer := New(cfg, nil)

	assert.Equal(t, "datum", namer.Singularize("data"))
	assert.Equal(t, "user", namer.Singularize("users")) // Falls back to library
}

func TestManyToOneFieldName(t *testing.T) {
	namer := Default()

	tests := []struct {
		fkColumn string
		expected string
	}{
		{"author_id", "author"},
		{"editor_id", "editor"},
		{"user_id", "user"},
		{"created_by_user_id", "createdByUser"},
		{"parent_category_id", "parentCategory"},
		{"owner_fk", "owner"},
		{"simple", "simple"}, // No suffix to strip
	}

	for _, tt := range tests {
		t.Run(tt.fkColumn, func(t *testing.T) {
			result := namer.ManyToOneFieldName(tt.fkColumn)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestOneToManyFieldName(t *testing.T) {
	namer := Default()

	tests := []struct {
		sourceTable string
		fkColumn    string
		isOnlyFK    bool
		expected    string
	}{
		{"comments", "user_id", true, "comments"},       // Single FK: use table name
		{"posts", "author_id", false, "authorPosts"},    // Multiple FKs: prefix
		{"posts", "editor_id", false, "editorPosts"},    // Multiple FKs: prefix
		{"order_items", "order_id", true, "orderItems"}, // Single FK with underscore
	}

	for _, tt := range tests {
		t.Run(tt.sourceTable+"_"+tt.fkColumn, func(t *testing.T) {
			result := namer.OneToManyFieldName(tt.sourceTable, tt.fkColumn, tt.isOnlyFK)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestReservedWordSuffixing(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))
	namer := New(DefaultConfig(), logger)

	tests := []struct {
		input    string
		expected string
	}{
		{"query", "Query_"},                          // PascalCase + reserved suffix
		{"Query", "Query_"},                          // Already PascalCase + reserved suffix
		{"type", "Type_"},                            // PascalCase + reserved suffix
		{"mutation", "Mutation_"},                    // PascalCase + reserved suffix
		{"users", "Users"},                           // Not reserved
		{"products_aggregate", "ProductsAggregate_"}, // Reserved pattern
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			namer.Reset() // Reset collision state
			result := namer.ToGraphQLTypeName(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestQueryReservedPatternSuffixing(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))
	namer := New(DefaultConfig(), logger)

	result := namer.RegisterQueryField("sales_aggregate")
	assert.Equal(t, "salesAggregate_", result)
	assert.Contains(t, buf.String(), "reserved pattern")
}

func TestCollision_TableToTable(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))
	namer := New(DefaultConfig(), logger)

	// First registration
	result1 := namer.RegisterType("user_profile")
	assert.Equal(t, "UserProfile", result1)

	// Second registration with collision - different source but same GraphQL name
	// Simulating userprofile table which would also become UserProfile
	result2 := namer.resolver.RegisterType("UserProfile", "userprofile")
	assert.Equal(t, "UserProfile2", result2)

	// Verify warning was logged
	assert.Contains(t, buf.String(), "naming collision detected")
}

func TestCollision_ColumnToColumn(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))
	namer := New(DefaultConfig(), logger)

	// First column
	result1 := namer.RegisterColumnField("User", "user_id")
	assert.Equal(t, "userId", result1)

	// Second column that produces same field name
	result2 := namer.resolver.RegisterField("User", "userId", "column:userId")
	assert.Equal(t, "userId2", result2)

	assert.Contains(t, buf.String(), "naming collision detected")
}

func TestCollision_RelationshipToColumn(t *testing.T) {
	namer := Default()

	// Register column first (columns have precedence)
	namer.RegisterColumnField("Order", "author")

	// Register relationship - should get suffix
	result := namer.RegisterRelationshipField("Order", "author", "users", true)
	assert.Equal(t, "authorRef", result)
}

func TestCollision_QueryField(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))
	namer := New(DefaultConfig(), logger)

	// First query field
	result1 := namer.RegisterQueryField("products")
	assert.Equal(t, "products", result1)

	// Second query field with collision
	result2 := namer.RegisterQueryField("product") // Would also produce "products" after pluralization logic
	// Note: RegisterQueryField uses the table name directly, not pluralized
	assert.Equal(t, "product", result2) // Different name, no collision
}

func TestReset(t *testing.T) {
	namer := Default()

	// Register a type
	namer.RegisterType("users")

	// Reset
	namer.Reset()

	// Should be able to register same type again without collision
	result := namer.RegisterType("users")
	assert.Equal(t, "Users", result)
}

func TestEdgeTypeName(t *testing.T) {
	namer := Default()

	tests := []struct {
		name      string
		leftTable string
		rightTable string
		expected   string
	}{
		{
			name:       "alphabetical order preserved",
			leftTable:  "departments",
			rightTable: "employees",
			expected:   "DepartmentEmployee",
		},
		{
			name:       "alphabetical order reversed input",
			leftTable:  "employees",
			rightTable: "departments",
			expected:   "DepartmentEmployee", // Same result - sorted alphabetically
		},
		{
			name:       "singular table names",
			leftTable:  "role",
			rightTable: "user",
			expected:   "RoleUser",
		},
		{
			name:       "plural table names singularized",
			leftTable:  "users",
			rightTable: "roles",
			expected:   "RoleUser",
		},
		{
			name:       "snake_case tables",
			leftTable:  "user_profiles",
			rightTable: "access_levels",
			expected:   "AccessLevelUserProfile",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := namer.EdgeTypeName(tt.leftTable, tt.rightTable)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestEdgeFieldName(t *testing.T) {
	namer := Default()

	tests := []struct {
		name       string
		leftTable  string
		rightTable string
		expected   string
	}{
		{
			name:       "basic edge field",
			leftTable:  "departments",
			rightTable: "employees",
			expected:   "departmentEmployees",
		},
		{
			name:       "reversed input same result",
			leftTable:  "employees",
			rightTable: "departments",
			expected:   "departmentEmployees",
		},
		{
			name:       "snake_case tables",
			leftTable:  "user_profiles",
			rightTable: "access_levels",
			expected:   "accessLevelUserProfiles",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := namer.EdgeFieldName(tt.leftTable, tt.rightTable)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestManyToManyFieldName(t *testing.T) {
	namer := Default()

	tests := []struct {
		targetTable string
		expected    string
	}{
		{"employees", "employees"},
		{"employee", "employees"},
		{"department", "departments"},
		{"user_role", "userRoles"},
		{"role", "roles"},
	}

	for _, tt := range tests {
		t.Run(tt.targetTable, func(t *testing.T) {
			result := namer.ManyToManyFieldName(tt.targetTable)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestJunctionFieldName(t *testing.T) {
	namer := Default()

	tests := []struct {
		name          string
		junctionTable string
		leftTable     string
		rightTable    string
		targetTable   string
		isAttribute   bool
		expected      string
	}{
		{
			name:          "attribute junction uses junction table name",
			junctionTable: "dept_emp",
			leftTable:     "departments",
			rightTable:    "employees",
			targetTable:   "",
			isAttribute:   true,
			expected:      "deptEmps",
		},
		{
			name:          "pure simple join uses target table name",
			junctionTable: "user_roles",
			leftTable:     "users",
			rightTable:    "roles",
			targetTable:   "roles",
			isAttribute:   false,
			expected:      "roles",
		},
		{
			name:          "pure semantic join uses junction table name",
			junctionTable: "dept_manager",
			leftTable:     "departments",
			rightTable:    "employees",
			targetTable:   "employees",
			isAttribute:   false,
			expected:      "deptManagers",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := namer.JunctionFieldName(tt.junctionTable, tt.leftTable, tt.rightTable, tt.targetTable, tt.isAttribute)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestJunctionEdgeRefFieldName(t *testing.T) {
	namer := Default()

	tests := []struct {
		table    string
		expected string
	}{
		{"employees", "employee"},
		{"departments", "department"},
		{"user_profiles", "userProfile"},
		{"status", "status"},
	}

	for _, tt := range tests {
		t.Run(tt.table, func(t *testing.T) {
			result := namer.JunctionEdgeRefFieldName(tt.table)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestRegisterEdgeField(t *testing.T) {
	namer := Default()

	// Register edge field
	result := namer.RegisterEdgeField("Employee", "departments", "employees")
	assert.Equal(t, "departmentEmployees", result)

	// Register column with same name to test collision
	namer.Reset()
	namer.RegisterColumnField("Employee", "department_employees")
	result2 := namer.RegisterEdgeField("Employee", "departments", "employees")
	assert.Equal(t, "departmentEmployeesEdge", result2)
}

func TestRegisterManyToManyField(t *testing.T) {
	namer := Default()

	// Register M2M field
	result := namer.RegisterManyToManyField("User", "roles", "user_roles")
	assert.Equal(t, "roles", result)

	// Test collision handling
	namer.Reset()
	namer.RegisterColumnField("User", "roles") // Pre-existing column
	result2 := namer.RegisterManyToManyField("User", "roles", "user_roles")
	assert.Equal(t, "rolesViaUserRoles", result2)
}
