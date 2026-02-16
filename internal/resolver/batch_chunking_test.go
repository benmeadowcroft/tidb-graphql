package resolver

import (
	"context"
	"testing"

	"github.com/graphql-go/graphql"
	"github.com/graphql-go/graphql/language/ast"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"tidb-graphql/internal/introspection"
	"tidb-graphql/internal/naming"
	"tidb-graphql/internal/schemafilter"
)

func TestTryBatchManyToManyConnection_Chunking(t *testing.T) {
	parentCount := batchMaxInClause + 1

	executor := &fakeExecutor{responses: [][][]any{
		{{11, 1}},
		{{11001, parentCount}},
	}}

	users := introspection.Table{
		Name: "users",
		Columns: []introspection.Column{
			{Name: "id", IsPrimaryKey: true},
		},
	}
	tags := introspection.Table{
		Name: "tags",
		Columns: []introspection.Column{
			{Name: "id", IsPrimaryKey: true},
		},
	}

	schema := &introspection.Schema{Tables: []introspection.Table{users, tags}}
	r := NewResolver(executor, schema, nil, 0, schemafilter.Config{}, naming.DefaultConfig())

	ctx := NewBatchingContext(context.Background())
	state, ok := GetBatchState(ctx)
	require.True(t, ok)

	parentKey := "users|list|"
	parentRows := make([]map[string]interface{}, parentCount)
	for i := 0; i < parentCount; i++ {
		parentRows[i] = map[string]interface{}{"id": i + 1, batchParentKeyField: parentKey}
	}
	state.setParentRows(parentKey, parentRows)

	rel := introspection.Relationship{
		IsManyToMany:     true,
		LocalColumn:      "id",
		RemoteTable:      "tags",
		RemoteColumn:     "id",
		JunctionTable:    "user_tags",
		JunctionLocalFK:  "user_id",
		JunctionRemoteFK: "tag_id",
		GraphQLFieldName: "tags",
	}

	field := &ast.Field{
		Name: &ast.Name{Value: "tags"},
		SelectionSet: &ast.SelectionSet{Selections: []ast.Selection{
			&ast.Field{
				Name: &ast.Name{Value: "nodes"},
				SelectionSet: &ast.SelectionSet{Selections: []ast.Selection{
					&ast.Field{Name: &ast.Name{Value: "id"}},
				}},
			},
		}},
	}

	results, ok, err := r.tryBatchManyToManyConnection(graphql.ResolveParams{
		Source:  parentRows[parentCount-1],
		Args:    map[string]interface{}{"first": 10},
		Context: ctx,
		Info: graphql.ResolveInfo{
			FieldASTs: []*ast.Field{field},
		},
	}, users, rel, parentCount)
	require.NoError(t, err)
	require.True(t, ok)
	nodes, ok := results["nodes"].([]map[string]interface{})
	require.True(t, ok)
	require.Len(t, nodes, 1)
	assert.EqualValues(t, 11001, nodes[0]["id"])

	assert.Equal(t, 2, executor.calls)
	require.Len(t, executor.args, 2)
	assert.Len(t, executor.args[0], batchMaxInClause+2)
	assert.Len(t, executor.args[1], 3)
}

func TestTryBatchEdgeListConnection_Chunking(t *testing.T) {
	parentCount := batchMaxInClause + 1

	executor := &fakeExecutor{responses: [][][]any{
		{{1, 101, 1}},
		{{parentCount, 999, parentCount}},
	}}

	users := introspection.Table{
		Name: "users",
		Columns: []introspection.Column{
			{Name: "id", IsPrimaryKey: true},
		},
	}
	junction := introspection.Table{
		Name: "user_tags",
		Columns: []introspection.Column{
			{Name: "user_id", IsPrimaryKey: true},
			{Name: "tag_id", IsPrimaryKey: true},
		},
	}

	schema := &introspection.Schema{Tables: []introspection.Table{users, junction}}
	r := NewResolver(executor, schema, nil, 0, schemafilter.Config{}, naming.DefaultConfig())

	ctx := NewBatchingContext(context.Background())
	state, ok := GetBatchState(ctx)
	require.True(t, ok)

	parentKey := "users|list|"
	parentRows := make([]map[string]interface{}, parentCount)
	for i := 0; i < parentCount; i++ {
		parentRows[i] = map[string]interface{}{"id": i + 1, batchParentKeyField: parentKey}
	}
	state.setParentRows(parentKey, parentRows)

	rel := introspection.Relationship{
		IsEdgeList:       true,
		LocalColumn:      "id",
		JunctionTable:    "user_tags",
		JunctionLocalFK:  "user_id",
		GraphQLFieldName: "userTags",
	}

	field := &ast.Field{
		Name: &ast.Name{Value: "userTags"},
		SelectionSet: &ast.SelectionSet{Selections: []ast.Selection{
			&ast.Field{
				Name: &ast.Name{Value: "nodes"},
				SelectionSet: &ast.SelectionSet{Selections: []ast.Selection{
					&ast.Field{Name: &ast.Name{Value: "tagId"}},
				}},
			},
		}},
	}

	results, ok, err := r.tryBatchEdgeListConnection(graphql.ResolveParams{
		Source:  parentRows[parentCount-1],
		Args:    map[string]interface{}{"first": 10},
		Context: ctx,
		Info: graphql.ResolveInfo{
			FieldASTs: []*ast.Field{field},
		},
	}, users, rel, parentCount)
	require.NoError(t, err)
	require.True(t, ok)
	nodes, ok := results["nodes"].([]map[string]interface{})
	require.True(t, ok)
	require.Len(t, nodes, 1)
	assert.EqualValues(t, 999, nodes[0]["tagId"])
	_, hasUserID := nodes[0]["userId"]
	assert.True(t, hasUserID)

	assert.Equal(t, 2, executor.calls)
	require.Len(t, executor.args, 2)
	assert.Len(t, executor.args[0], batchMaxInClause+2)
	assert.Len(t, executor.args[1], 3)
}
