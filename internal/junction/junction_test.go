package junction

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidb-graphql/internal/introspection"
)

func TestClassifyJunctions(t *testing.T) {
	tests := []struct {
		name           string
		schema         *introspection.Schema
		expectedTypes  map[string]Type
		expectedCounts map[string]int // table -> attribute column count
	}{
		{
			name: "pure junction - only FK columns",
			schema: &introspection.Schema{
				Tables: []introspection.Table{
					{
						Name:    "users",
						Columns: []introspection.Column{{Name: "id", IsPrimaryKey: true}},
					},
					{
						Name:    "roles",
						Columns: []introspection.Column{{Name: "id", IsPrimaryKey: true}},
					},
					{
						Name: "user_roles",
						Columns: []introspection.Column{
							{Name: "user_id", IsPrimaryKey: true, IsNullable: false},
							{Name: "role_id", IsPrimaryKey: true, IsNullable: false},
						},
						ForeignKeys: []introspection.ForeignKey{
							{ColumnName: "user_id", ReferencedTable: "users", ReferencedColumn: "id"},
							{ColumnName: "role_id", ReferencedTable: "roles", ReferencedColumn: "id"},
						},
					},
				},
			},
			expectedTypes:  map[string]Type{"user_roles": PureJunction},
			expectedCounts: map[string]int{"user_roles": 0},
		},
		{
			name: "attribute junction - has extra columns",
			schema: &introspection.Schema{
				Tables: []introspection.Table{
					{
						Name:    "employees",
						Columns: []introspection.Column{{Name: "emp_no", IsPrimaryKey: true}},
					},
					{
						Name:    "departments",
						Columns: []introspection.Column{{Name: "dept_no", IsPrimaryKey: true}},
					},
					{
						Name: "dept_emp",
						Columns: []introspection.Column{
							{Name: "emp_no", IsPrimaryKey: true, IsNullable: false},
							{Name: "dept_no", IsPrimaryKey: true, IsNullable: false},
							{Name: "from_date", DataType: "date", IsNullable: false},
							{Name: "to_date", DataType: "date", IsNullable: false},
						},
						ForeignKeys: []introspection.ForeignKey{
							{ColumnName: "emp_no", ReferencedTable: "employees", ReferencedColumn: "emp_no"},
							{ColumnName: "dept_no", ReferencedTable: "departments", ReferencedColumn: "dept_no"},
						},
					},
				},
			},
			expectedTypes:  map[string]Type{"dept_emp": AttributeJunction},
			expectedCounts: map[string]int{"dept_emp": 2}, // from_date, to_date
		},
		{
			name: "not a junction - single FK",
			schema: &introspection.Schema{
				Tables: []introspection.Table{
					{
						Name:    "users",
						Columns: []introspection.Column{{Name: "id", IsPrimaryKey: true}},
					},
					{
						Name: "posts",
						Columns: []introspection.Column{
							{Name: "id", IsPrimaryKey: true},
							{Name: "user_id", IsNullable: false},
						},
						ForeignKeys: []introspection.ForeignKey{
							{ColumnName: "user_id", ReferencedTable: "users", ReferencedColumn: "id"},
						},
					},
				},
			},
			expectedTypes: map[string]Type{},
		},
		{
			name: "not a junction - nullable FK",
			schema: &introspection.Schema{
				Tables: []introspection.Table{
					{
						Name:    "a",
						Columns: []introspection.Column{{Name: "id", IsPrimaryKey: true}},
					},
					{
						Name:    "b",
						Columns: []introspection.Column{{Name: "id", IsPrimaryKey: true}},
					},
					{
						Name: "a_b",
						Columns: []introspection.Column{
							{Name: "a_id", IsPrimaryKey: true, IsNullable: false},
							{Name: "b_id", IsPrimaryKey: true, IsNullable: true}, // Nullable!
						},
						ForeignKeys: []introspection.ForeignKey{
							{ColumnName: "a_id", ReferencedTable: "a", ReferencedColumn: "id"},
							{ColumnName: "b_id", ReferencedTable: "b", ReferencedColumn: "id"},
						},
					},
				},
			},
			expectedTypes: map[string]Type{},
		},
		{
			name: "not a junction - self-referential",
			schema: &introspection.Schema{
				Tables: []introspection.Table{
					{
						Name:    "users",
						Columns: []introspection.Column{{Name: "id", IsPrimaryKey: true}},
					},
					{
						Name: "user_friends",
						Columns: []introspection.Column{
							{Name: "user_id", IsPrimaryKey: true, IsNullable: false},
							{Name: "friend_id", IsPrimaryKey: true, IsNullable: false},
						},
						ForeignKeys: []introspection.ForeignKey{
							{ColumnName: "user_id", ReferencedTable: "users", ReferencedColumn: "id"},
							{ColumnName: "friend_id", ReferencedTable: "users", ReferencedColumn: "id"},
						},
					},
				},
			},
			expectedTypes: map[string]Type{},
		},
		{
			name: "not a junction - three FKs",
			schema: &introspection.Schema{
				Tables: []introspection.Table{
					{
						Name:    "a",
						Columns: []introspection.Column{{Name: "id", IsPrimaryKey: true}},
					},
					{
						Name:    "b",
						Columns: []introspection.Column{{Name: "id", IsPrimaryKey: true}},
					},
					{
						Name:    "c",
						Columns: []introspection.Column{{Name: "id", IsPrimaryKey: true}},
					},
					{
						Name: "a_b_c",
						Columns: []introspection.Column{
							{Name: "a_id", IsPrimaryKey: true, IsNullable: false},
							{Name: "b_id", IsPrimaryKey: true, IsNullable: false},
							{Name: "c_id", IsPrimaryKey: true, IsNullable: false},
						},
						ForeignKeys: []introspection.ForeignKey{
							{ColumnName: "a_id", ReferencedTable: "a", ReferencedColumn: "id"},
							{ColumnName: "b_id", ReferencedTable: "b", ReferencedColumn: "id"},
							{ColumnName: "c_id", ReferencedTable: "c", ReferencedColumn: "id"},
						},
					},
				},
			},
			expectedTypes: map[string]Type{},
		},
		{
			name: "not a junction - no covering constraint",
			schema: &introspection.Schema{
				Tables: []introspection.Table{
					{
						Name:    "a",
						Columns: []introspection.Column{{Name: "id", IsPrimaryKey: true}},
					},
					{
						Name:    "b",
						Columns: []introspection.Column{{Name: "id", IsPrimaryKey: true}},
					},
					{
						Name: "a_b",
						Columns: []introspection.Column{
							{Name: "id", IsPrimaryKey: true, IsNullable: false}, // PK is separate column
							{Name: "a_id", IsNullable: false},
							{Name: "b_id", IsNullable: false},
						},
						ForeignKeys: []introspection.ForeignKey{
							{ColumnName: "a_id", ReferencedTable: "a", ReferencedColumn: "id"},
							{ColumnName: "b_id", ReferencedTable: "b", ReferencedColumn: "id"},
						},
					},
				},
			},
			expectedTypes: map[string]Type{},
		},
		{
			name: "junction with unique index instead of PK",
			schema: &introspection.Schema{
				Tables: []introspection.Table{
					{
						Name:    "users",
						Columns: []introspection.Column{{Name: "id", IsPrimaryKey: true}},
					},
					{
						Name:    "roles",
						Columns: []introspection.Column{{Name: "id", IsPrimaryKey: true}},
					},
					{
						Name: "user_roles",
						Columns: []introspection.Column{
							{Name: "id", IsPrimaryKey: true, IsNullable: false}, // Separate PK
							{Name: "user_id", IsNullable: false},
							{Name: "role_id", IsNullable: false},
						},
						ForeignKeys: []introspection.ForeignKey{
							{ColumnName: "user_id", ReferencedTable: "users", ReferencedColumn: "id"},
							{ColumnName: "role_id", ReferencedTable: "roles", ReferencedColumn: "id"},
						},
						Indexes: []introspection.Index{
							{Name: "uq_user_role", Unique: true, Columns: []string{"user_id", "role_id"}},
						},
					},
				},
			},
			expectedTypes:  map[string]Type{"user_roles": AttributeJunction}, // id is an attribute
			expectedCounts: map[string]int{"user_roles": 1},                  // id column
		},
		{
			name: "referenced table missing from schema",
			schema: &introspection.Schema{
				Tables: []introspection.Table{
					{
						Name:    "users",
						Columns: []introspection.Column{{Name: "id", IsPrimaryKey: true}},
					},
					// "roles" table is missing
					{
						Name: "user_roles",
						Columns: []introspection.Column{
							{Name: "user_id", IsPrimaryKey: true, IsNullable: false},
							{Name: "role_id", IsPrimaryKey: true, IsNullable: false},
						},
						ForeignKeys: []introspection.ForeignKey{
							{ColumnName: "user_id", ReferencedTable: "users", ReferencedColumn: "id"},
							{ColumnName: "role_id", ReferencedTable: "roles", ReferencedColumn: "id"},
						},
					},
				},
			},
			expectedTypes: map[string]Type{},
		},
		{
			name: "view is skipped",
			schema: &introspection.Schema{
				Tables: []introspection.Table{
					{
						Name:    "users",
						Columns: []introspection.Column{{Name: "id", IsPrimaryKey: true}},
					},
					{
						Name:    "roles",
						Columns: []introspection.Column{{Name: "id", IsPrimaryKey: true}},
					},
					{
						Name:   "user_roles_view",
						IsView: true,
						Columns: []introspection.Column{
							{Name: "user_id", IsPrimaryKey: true, IsNullable: false},
							{Name: "role_id", IsPrimaryKey: true, IsNullable: false},
						},
						ForeignKeys: []introspection.ForeignKey{
							{ColumnName: "user_id", ReferencedTable: "users", ReferencedColumn: "id"},
							{ColumnName: "role_id", ReferencedTable: "roles", ReferencedColumn: "id"},
						},
					},
				},
			},
			expectedTypes: map[string]Type{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ClassifyJunctions(tt.schema)

			// Verify expected junctions are found
			for tableName, expectedType := range tt.expectedTypes {
				info, exists := result[tableName]
				require.True(t, exists, "Expected junction %s not found", tableName)
				assert.Equal(t, expectedType, info.Type, "Wrong type for %s", tableName)

				if expectedCount, ok := tt.expectedCounts[tableName]; ok {
					assert.Len(t, info.AttributeColumns, expectedCount,
						"Wrong attribute count for %s", tableName)
				}
			}

			// Verify no unexpected junctions
			for tableName := range result {
				_, expected := tt.expectedTypes[tableName]
				assert.True(t, expected, "Unexpected junction %s found", tableName)
			}
		})
	}
}

func TestOrderFKs(t *testing.T) {
	fk1 := introspection.ForeignKey{
		ColumnName:       "emp_no",
		ReferencedTable:  "employees",
		ReferencedColumn: "emp_no",
	}
	fk2 := introspection.ForeignKey{
		ColumnName:       "dept_no",
		ReferencedTable:  "departments",
		ReferencedColumn: "dept_no",
	}

	// Test both orderings result in same alphabetical order
	left, right := orderFKs(fk1, fk2)
	assert.Equal(t, "departments", left.ReferencedTable)
	assert.Equal(t, "employees", right.ReferencedTable)

	left2, right2 := orderFKs(fk2, fk1)
	assert.Equal(t, "departments", left2.ReferencedTable)
	assert.Equal(t, "employees", right2.ReferencedTable)
}

func TestTypeString(t *testing.T) {
	assert.Equal(t, "NotJunction", NotJunction.String())
	assert.Equal(t, "PureJunction", PureJunction.String())
	assert.Equal(t, "AttributeJunction", AttributeJunction.String())
	assert.Equal(t, "Unknown", Type(99).String())
}

func TestFindAttributeColumns(t *testing.T) {
	table := introspection.Table{
		Columns: []introspection.Column{
			{Name: "user_id"},
			{Name: "role_id"},
			{Name: "created_at"},
			{Name: "expires_at"},
		},
	}
	fkCols := map[string]bool{"user_id": true, "role_id": true}

	attrs := findAttributeColumns(table, fkCols)
	assert.ElementsMatch(t, []string{"created_at", "expires_at"}, attrs)
}

func TestCoversAll(t *testing.T) {
	tests := []struct {
		name     string
		covering map[string]bool
		required map[string]bool
		expected bool
	}{
		{
			name:     "exact match",
			covering: map[string]bool{"a": true, "b": true},
			required: map[string]bool{"a": true, "b": true},
			expected: true,
		},
		{
			name:     "covering has extra",
			covering: map[string]bool{"a": true, "b": true, "c": true},
			required: map[string]bool{"a": true, "b": true},
			expected: true,
		},
		{
			name:     "missing column",
			covering: map[string]bool{"a": true},
			required: map[string]bool{"a": true, "b": true},
			expected: false,
		},
		{
			name:     "empty required",
			covering: map[string]bool{"a": true},
			required: map[string]bool{},
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := coversAll(tt.covering, tt.required)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestToIntrospectionMap(t *testing.T) {
	jMap := Map{
		"user_roles": Info{
			Table: "user_roles",
			Type:  PureJunction,
			LeftFK: FKInfo{
				ColumnName:       "role_id",
				ReferencedTable:  "roles",
				ReferencedColumn: "id",
			},
			RightFK: FKInfo{
				ColumnName:       "user_id",
				ReferencedTable:  "users",
				ReferencedColumn: "id",
			},
			AttributeColumns: nil,
		},
		"project_members": Info{
			Table: "project_members",
			Type:  AttributeJunction,
			LeftFK: FKInfo{
				ColumnName:       "project_id",
				ReferencedTable:  "projects",
				ReferencedColumn: "id",
			},
			RightFK: FKInfo{
				ColumnName:       "user_id",
				ReferencedTable:  "users",
				ReferencedColumn: "id",
			},
			AttributeColumns: []string{"assigned_at", "role_level"},
		},
	}

	iMap := jMap.ToIntrospectionMap()

	// Verify pure junction conversion
	userRoles, ok := iMap["user_roles"]
	require.True(t, ok)
	assert.Equal(t, introspection.JunctionTypePure, userRoles.Type)
	assert.Equal(t, "roles", userRoles.LeftFK.ReferencedTable)
	assert.Equal(t, "users", userRoles.RightFK.ReferencedTable)

	// Verify attribute junction conversion
	projectMembers, ok := iMap["project_members"]
	require.True(t, ok)
	assert.Equal(t, introspection.JunctionTypeAttribute, projectMembers.Type)
	assert.Equal(t, "projects", projectMembers.LeftFK.ReferencedTable)
	assert.Equal(t, "users", projectMembers.RightFK.ReferencedTable)
}

// TestJunctionSchemaFixture tests classification against the junction_schema.sql structure
func TestJunctionSchemaFixture(t *testing.T) {
	// Build schema matching junction_schema.sql
	schema := &introspection.Schema{
		Tables: []introspection.Table{
			{
				Name: "users",
				Columns: []introspection.Column{
					{Name: "id", IsPrimaryKey: true},
					{Name: "name"},
					{Name: "email"},
				},
			},
			{
				Name: "roles",
				Columns: []introspection.Column{
					{Name: "id", IsPrimaryKey: true},
					{Name: "name"},
					{Name: "description", IsNullable: true},
				},
			},
			{
				Name: "projects",
				Columns: []introspection.Column{
					{Name: "id", IsPrimaryKey: true},
					{Name: "name"},
					{Name: "description", IsNullable: true},
					{Name: "created_at"},
				},
			},
			{
				Name: "user_roles",
				Columns: []introspection.Column{
					{Name: "user_id", IsPrimaryKey: true, IsNullable: false},
					{Name: "role_id", IsPrimaryKey: true, IsNullable: false},
				},
				ForeignKeys: []introspection.ForeignKey{
					{ColumnName: "user_id", ReferencedTable: "users", ReferencedColumn: "id"},
					{ColumnName: "role_id", ReferencedTable: "roles", ReferencedColumn: "id"},
				},
			},
			{
				Name: "project_members",
				Columns: []introspection.Column{
					{Name: "user_id", IsPrimaryKey: true, IsNullable: false},
					{Name: "project_id", IsPrimaryKey: true, IsNullable: false},
					{Name: "assigned_at", IsNullable: false},
					{Name: "role_level", IsNullable: false},
				},
				ForeignKeys: []introspection.ForeignKey{
					{ColumnName: "user_id", ReferencedTable: "users", ReferencedColumn: "id"},
					{ColumnName: "project_id", ReferencedTable: "projects", ReferencedColumn: "id"},
				},
			},
		},
	}

	junctions := ClassifyJunctions(schema)

	// Verify user_roles is a pure junction
	userRoles, ok := junctions["user_roles"]
	require.True(t, ok, "user_roles should be classified as junction")
	assert.Equal(t, PureJunction, userRoles.Type)
	assert.Empty(t, userRoles.AttributeColumns)
	// Verify FK ordering (alphabetical by referenced table: roles < users)
	assert.Equal(t, "roles", userRoles.LeftFK.ReferencedTable)
	assert.Equal(t, "users", userRoles.RightFK.ReferencedTable)

	// Verify project_members is an attribute junction
	projectMembers, ok := junctions["project_members"]
	require.True(t, ok, "project_members should be classified as junction")
	assert.Equal(t, AttributeJunction, projectMembers.Type)
	assert.Len(t, projectMembers.AttributeColumns, 2)
	assert.Contains(t, projectMembers.AttributeColumns, "assigned_at")
	assert.Contains(t, projectMembers.AttributeColumns, "role_level")
	// Verify FK ordering (alphabetical by referenced table: projects < users)
	assert.Equal(t, "projects", projectMembers.LeftFK.ReferencedTable)
	assert.Equal(t, "users", projectMembers.RightFK.ReferencedTable)

	// Verify base tables are not classified as junctions
	_, ok = junctions["users"]
	assert.False(t, ok, "users should not be a junction")
	_, ok = junctions["roles"]
	assert.False(t, ok, "roles should not be a junction")
	_, ok = junctions["projects"]
	assert.False(t, ok, "projects should not be a junction")
}
