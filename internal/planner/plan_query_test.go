package planner

import (
	"testing"

	"github.com/graphql-go/graphql/language/ast"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"tidb-graphql/internal/introspection"
)

func TestPlanQuery_ListField(t *testing.T) {
	dbSchema := &introspection.Schema{
		Tables: []introspection.Table{
			{
				Name: "users",
				Columns: []introspection.Column{
					{Name: "id", IsPrimaryKey: true},
					{Name: "username"},
				},
			},
		},
	}

	field := &ast.Field{Name: &ast.Name{Value: "users"}}
	plan, err := PlanQuery(dbSchema, field, map[string]interface{}{
		"limit":  5,
		"offset": 2,
	})
	require.Error(t, err)
	assert.Nil(t, plan)
}

func TestPlanQuery_ListFieldProjection(t *testing.T) {
	dbSchema := &introspection.Schema{
		Tables: []introspection.Table{
			{
				Name: "users",
				Columns: []introspection.Column{
					{Name: "id", IsPrimaryKey: true},
					{Name: "username"},
					{Name: "email"},
				},
			},
		},
	}

	field := &ast.Field{
		Name: &ast.Name{Value: "users"},
		SelectionSet: &ast.SelectionSet{Selections: []ast.Selection{
			&ast.Field{Name: &ast.Name{Value: "id"}},
			&ast.Field{Name: &ast.Name{Value: "username"}},
		}},
	}
	plan, err := PlanQuery(dbSchema, field, nil)
	require.Error(t, err)
	assert.Nil(t, plan)
}

func TestPlanQuery_ListFieldProjectionIncludesRelationshipKey(t *testing.T) {
	dbSchema := &introspection.Schema{
		Tables: []introspection.Table{
			{
				Name: "users",
				Columns: []introspection.Column{
					{Name: "id", IsPrimaryKey: true},
					{Name: "account_id"},
					{Name: "name"},
				},
				Relationships: []introspection.Relationship{
					{
						IsManyToOne:      true,
						LocalColumn:      "account_id",
						RemoteTable:      "accounts",
						RemoteColumn:     "id",
						GraphQLFieldName: "account",
					},
				},
			},
		},
	}

	field := &ast.Field{
		Name: &ast.Name{Value: "users"},
		SelectionSet: &ast.SelectionSet{Selections: []ast.Selection{
			&ast.Field{
				Name: &ast.Name{Value: "account"},
				SelectionSet: &ast.SelectionSet{Selections: []ast.Selection{
					&ast.Field{Name: &ast.Name{Value: "id"}},
				}},
			},
		}},
	}
	plan, err := PlanQuery(dbSchema, field, nil)
	require.Error(t, err)
	assert.Nil(t, plan)
}

func TestPlanQuery_ListFieldDefaults(t *testing.T) {
	dbSchema := &introspection.Schema{
		Tables: []introspection.Table{
			{
				Name: "users",
				Columns: []introspection.Column{
					{Name: "id", IsPrimaryKey: true},
					{Name: "username"},
				},
			},
		},
	}

	field := &ast.Field{Name: &ast.Name{Value: "users"}}
	plan, err := PlanQuery(dbSchema, field, nil)
	require.Error(t, err)
	assert.Nil(t, plan)
}

func TestPlanQuery_ListFieldRejectsNegativeLimit(t *testing.T) {
	dbSchema := &introspection.Schema{
		Tables: []introspection.Table{
			{
				Name: "users",
				Columns: []introspection.Column{
					{Name: "id", IsPrimaryKey: true},
					{Name: "username"},
				},
			},
		},
	}

	field := &ast.Field{Name: &ast.Name{Value: "users"}}
	plan, err := PlanQuery(dbSchema, field, map[string]interface{}{
		"limit": -1,
	})
	require.Error(t, err)
	assert.Nil(t, plan)
}

func TestPlanQuery_ListFieldRejectsNegativeOffset(t *testing.T) {
	dbSchema := &introspection.Schema{
		Tables: []introspection.Table{
			{
				Name: "users",
				Columns: []introspection.Column{
					{Name: "id", IsPrimaryKey: true},
					{Name: "username"},
				},
			},
		},
	}

	field := &ast.Field{Name: &ast.Name{Value: "users"}}
	plan, err := PlanQuery(dbSchema, field, map[string]interface{}{
		"offset": -5,
	})
	require.Error(t, err)
	assert.Nil(t, plan)
}

func TestPlanQuery_ListFieldOrderBy(t *testing.T) {
	dbSchema := &introspection.Schema{
		Tables: []introspection.Table{
			{
				Name: "users",
				Columns: []introspection.Column{
					{Name: "id", IsPrimaryKey: true},
					{Name: "last_name"},
					{Name: "first_name"},
				},
				Indexes: []introspection.Index{
					{
						Name:    "idx_last_first",
						Unique:  false,
						Columns: []string{"last_name", "first_name"},
					},
				},
			},
		},
	}

	field := &ast.Field{Name: &ast.Name{Value: "users"}}
	plan, err := PlanQuery(dbSchema, field, map[string]interface{}{
		"orderBy": map[string]interface{}{
			"lastName_firstName": "ASC",
		},
	})
	require.Error(t, err)
	assert.Nil(t, plan)
}

func TestPlanQuery_ListFieldOrderByInvalid(t *testing.T) {
	dbSchema := &introspection.Schema{
		Tables: []introspection.Table{
			{
				Name: "users",
				Columns: []introspection.Column{
					{Name: "id", IsPrimaryKey: true},
					{Name: "last_name"},
				},
				Indexes: []introspection.Index{
					{
						Name:    "idx_last",
						Unique:  false,
						Columns: []string{"last_name"},
					},
				},
			},
		},
	}

	field := &ast.Field{Name: &ast.Name{Value: "users"}}
	plan, err := PlanQuery(dbSchema, field, map[string]interface{}{
		"orderBy": map[string]interface{}{
			"firstName": "ASC",
		},
	})
	require.Error(t, err)
	assert.Nil(t, plan)
}

func TestPlanQuery_ListFieldOrderByMultipleFields(t *testing.T) {
	dbSchema := &introspection.Schema{
		Tables: []introspection.Table{
			{
				Name: "users",
				Columns: []introspection.Column{
					{Name: "id", IsPrimaryKey: true},
					{Name: "last_name"},
					{Name: "created_at"},
				},
				Indexes: []introspection.Index{
					{
						Name:    "idx_last",
						Unique:  false,
						Columns: []string{"last_name"},
					},
					{
						Name:    "idx_created",
						Unique:  false,
						Columns: []string{"created_at"},
					},
				},
			},
		},
	}

	field := &ast.Field{Name: &ast.Name{Value: "users"}}
	plan, err := PlanQuery(dbSchema, field, map[string]interface{}{
		"orderBy": map[string]interface{}{
			"lastName":  "ASC",
			"createdAt": "DESC",
		},
	})
	require.Error(t, err)
	assert.Nil(t, plan)
}
func TestPlanQuery_PKField(t *testing.T) {
	dbSchema := &introspection.Schema{
		Tables: []introspection.Table{
			{
				Name: "users",
				Columns: []introspection.Column{
					{Name: "id", IsPrimaryKey: true},
					{Name: "username"},
				},
			},
		},
	}

	field := &ast.Field{Name: &ast.Name{Value: "user"}}
	plan, err := PlanQuery(dbSchema, field, map[string]interface{}{
		"id": 9,
	})
	require.NoError(t, err)
	require.NotNil(t, plan)
	assert.Equal(t, "users", plan.Table.Name)
	assertSQLMatches(t, plan.Root.SQL, "SELECT `id`, `username` FROM `users` WHERE `id` = ?")
	assertArgsEqual(t, plan.Root.Args, []interface{}{9})
}

func TestPlanQuery_UnsupportedField(t *testing.T) {
	dbSchema := &introspection.Schema{
		Tables: []introspection.Table{
			{Name: "users"},
		},
	}

	field := &ast.Field{Name: &ast.Name{Value: "unknown"}}
	plan, err := PlanQuery(dbSchema, field, nil)
	require.Error(t, err)
	assert.Nil(t, plan)
}

func TestPlanQuery_MissingPKArg(t *testing.T) {
	dbSchema := &introspection.Schema{
		Tables: []introspection.Table{
			{
				Name: "users",
				Columns: []introspection.Column{
					{Name: "id", IsPrimaryKey: true},
				},
			},
		},
	}

	field := &ast.Field{Name: &ast.Name{Value: "user"}}
	plan, err := PlanQuery(dbSchema, field, map[string]interface{}{})
	require.Error(t, err)
	assert.Nil(t, plan)
}

func TestPlanQuery_NoPrimaryKey(t *testing.T) {
	dbSchema := &introspection.Schema{
		Tables: []introspection.Table{
			{
				Name: "users",
				Columns: []introspection.Column{
					{Name: "username"},
				},
			},
		},
	}

	field := &ast.Field{Name: &ast.Name{Value: "user"}}
	plan, err := PlanQuery(dbSchema, field, map[string]interface{}{
		"id": 1,
	})
	require.Error(t, err)
	assert.Nil(t, plan)
}

func TestPlanQuery_RelationshipManyToOne(t *testing.T) {
	dbSchema := &introspection.Schema{
		Tables: []introspection.Table{
			{
				Name: "accounts",
				Columns: []introspection.Column{
					{Name: "id", IsPrimaryKey: true},
					{Name: "email"},
				},
			},
		},
	}

	field := &ast.Field{Name: &ast.Name{Value: "account"}}
	plan, err := PlanQuery(dbSchema, field, nil, WithRelationship(RelationshipContext{
		RelatedTable: dbSchema.Tables[0],
		RemoteColumn: "id",
		Value:        42,
		IsManyToOne:  true,
	}))
	require.NoError(t, err)
	require.NotNil(t, plan)
	assert.Equal(t, "accounts", plan.Table.Name)
	assertSQLMatches(t, plan.Root.SQL, "SELECT `id`, `email` FROM `accounts` WHERE `id` = ?")
	assertArgsEqual(t, plan.Root.Args, []interface{}{42})
}

func TestPlanQuery_RelationshipOneToMany(t *testing.T) {
	dbSchema := &introspection.Schema{
		Tables: []introspection.Table{
			{
				Name: "posts",
				Columns: []introspection.Column{
					{Name: "id", IsPrimaryKey: true},
					{Name: "user_id"},
					{Name: "title"},
				},
			},
		},
	}

	field := &ast.Field{Name: &ast.Name{Value: "posts"}}
	plan, err := PlanQuery(dbSchema, field, map[string]interface{}{
		"limit":  12,
		"offset": 4,
	}, WithRelationship(RelationshipContext{
		RelatedTable: dbSchema.Tables[0],
		RemoteColumn: "user_id",
		Value:        7,
		IsOneToMany:  true,
	}))
	require.Error(t, err)
	assert.Nil(t, plan)
}

func TestSelectedColumnsForConnection_RootFragmentSpread(t *testing.T) {
	table := introspection.Table{
		Name: "users",
		Columns: []introspection.Column{
			{Name: "id", IsPrimaryKey: true},
			{Name: "email"},
			{Name: "created_at"},
		},
	}

	field := &ast.Field{
		Name: &ast.Name{Value: "usersConnection"},
		SelectionSet: &ast.SelectionSet{Selections: []ast.Selection{
			&ast.FragmentSpread{Name: &ast.Name{Value: "ConnFields"}},
		}},
	}
	fragments := map[string]ast.Definition{
		"ConnFields": &ast.FragmentDefinition{
			Name: &ast.Name{Value: "ConnFields"},
			SelectionSet: &ast.SelectionSet{Selections: []ast.Selection{
				&ast.Field{
					Name: &ast.Name{Value: "nodes"},
					SelectionSet: &ast.SelectionSet{Selections: []ast.Selection{
						&ast.Field{Name: &ast.Name{Value: "email"}},
					}},
				},
			}},
		},
	}

	orderBy := &OrderBy{Columns: []string{"created_at", "id"}, Directions: []string{"ASC", "ASC"}}
	cols := SelectedColumnsForConnection(table, field, fragments, orderBy)
	names := columnNamesOnly(cols)

	assert.Equal(t, []string{"id", "email", "created_at"}, names)
}

func TestSelectedColumnsForConnection_EdgesNodeViaFragment(t *testing.T) {
	table := introspection.Table{
		Name: "users",
		Columns: []introspection.Column{
			{Name: "id", IsPrimaryKey: true},
			{Name: "email"},
			{Name: "created_at"},
		},
	}

	field := &ast.Field{
		Name: &ast.Name{Value: "usersConnection"},
		SelectionSet: &ast.SelectionSet{Selections: []ast.Selection{
			&ast.Field{
				Name: &ast.Name{Value: "edges"},
				SelectionSet: &ast.SelectionSet{Selections: []ast.Selection{
					&ast.FragmentSpread{Name: &ast.Name{Value: "EdgeBits"}},
				}},
			},
		}},
	}
	fragments := map[string]ast.Definition{
		"EdgeBits": &ast.FragmentDefinition{
			Name: &ast.Name{Value: "EdgeBits"},
			SelectionSet: &ast.SelectionSet{Selections: []ast.Selection{
				&ast.Field{
					Name: &ast.Name{Value: "node"},
					SelectionSet: &ast.SelectionSet{Selections: []ast.Selection{
						&ast.Field{Name: &ast.Name{Value: "createdAt"}},
					}},
				},
			}},
		},
	}

	orderBy := &OrderBy{Columns: []string{"id"}, Directions: []string{"ASC"}}
	cols := SelectedColumnsForConnection(table, field, fragments, orderBy)
	names := columnNamesOnly(cols)

	assert.Equal(t, []string{"id", "created_at"}, names)
}

func TestSelectedColumnsForConnection_MetadataOnlyUsesCursorColumns(t *testing.T) {
	table := introspection.Table{
		Name: "users",
		Columns: []introspection.Column{
			{Name: "id", IsPrimaryKey: true},
			{Name: "email"},
			{Name: "created_at"},
		},
	}

	field := &ast.Field{
		Name: &ast.Name{Value: "usersConnection"},
		SelectionSet: &ast.SelectionSet{Selections: []ast.Selection{
			&ast.Field{Name: &ast.Name{Value: "totalCount"}},
			&ast.Field{
				Name: &ast.Name{Value: "aggregate"},
				SelectionSet: &ast.SelectionSet{Selections: []ast.Selection{
					&ast.Field{Name: &ast.Name{Value: "count"}},
				}},
			},
		}},
	}

	orderBy := &OrderBy{Columns: []string{"created_at", "id"}, Directions: []string{"ASC", "ASC"}}
	cols := SelectedColumnsForConnection(table, field, nil, orderBy)
	names := columnNamesOnly(cols)

	assert.Equal(t, []string{"id", "created_at"}, names)
}

func TestSelectedColumnsForConnection_IncludesRelationshipLocalKeys(t *testing.T) {
	table := introspection.Table{
		Name: "posts",
		Columns: []introspection.Column{
			{Name: "id", IsPrimaryKey: true},
			{Name: "user_id"},
			{Name: "title"},
		},
		Relationships: []introspection.Relationship{
			{
				IsManyToOne:      true,
				LocalColumns:     []string{"user_id"},
				RemoteTable:      "users",
				RemoteColumns:    []string{"id"},
				GraphQLFieldName: "user",
			},
		},
	}

	field := &ast.Field{
		Name: &ast.Name{Value: "postsConnection"},
		SelectionSet: &ast.SelectionSet{Selections: []ast.Selection{
			&ast.Field{
				Name: &ast.Name{Value: "nodes"},
				SelectionSet: &ast.SelectionSet{Selections: []ast.Selection{
					&ast.Field{Name: &ast.Name{Value: "databaseId"}},
					&ast.Field{
						Name: &ast.Name{Value: "user"},
						SelectionSet: &ast.SelectionSet{Selections: []ast.Selection{
							&ast.Field{Name: &ast.Name{Value: "username"}},
						}},
					},
				}},
			},
		}},
	}

	orderBy := &OrderBy{Columns: []string{"id"}, Directions: []string{"ASC"}}
	cols := SelectedColumnsForConnection(table, field, nil, orderBy)
	names := columnNamesOnly(cols)

	assert.Equal(t, []string{"id", "user_id"}, names)
}

func columnNamesOnly(cols []introspection.Column) []string {
	names := make([]string, 0, len(cols))
	for _, c := range cols {
		names = append(names, c.Name)
	}
	return names
}
