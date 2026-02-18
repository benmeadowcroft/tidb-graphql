package resolver

import (
	"context"
	"testing"

	"tidb-graphql/internal/dbexec"
	"tidb-graphql/internal/introspection"
	"tidb-graphql/internal/naming"
	"tidb-graphql/internal/planner"
	"tidb-graphql/internal/schemafilter"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/graphql-go/graphql"
	"github.com/graphql-go/graphql/language/ast"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func vectorDocsTable() introspection.Table {
	table := introspection.Table{
		Name: "docs",
		Columns: []introspection.Column{
			{Name: "id", DataType: "bigint", IsPrimaryKey: true},
			{Name: "embedding", DataType: "vector", ColumnType: "vector(3)", VectorDimension: 3},
			{Name: "title", DataType: "varchar"},
		},
		Indexes: []introspection.Index{
			{Name: "PRIMARY", Unique: true, Type: "BTREE", Columns: []string{"id"}},
			{Name: "idx_embedding", Type: "HNSW", Columns: []string{"embedding"}},
		},
	}
	renamePrimaryKeyID(&table)
	return table
}

func vectorAutoEmbeddingDocsTable() introspection.Table {
	table := introspection.Table{
		Name: "docs",
		Columns: []introspection.Column{
			{Name: "id", DataType: "bigint", IsPrimaryKey: true},
			{
				Name:                 "embedding",
				DataType:             "vector",
				ColumnType:           "vector(3)",
				VectorDimension:      3,
				GenerationExpression: `EMBED_TEXT("tidbcloud_free/amazon/titan-embed-text-v2", title, '{"dimensions":3}')`,
			},
			{Name: "title", DataType: "varchar"},
		},
		Indexes: []introspection.Index{
			{Name: "PRIMARY", Unique: true, Type: "BTREE", Columns: []string{"id"}},
			{Name: "idx_embedding", Type: "HNSW", Columns: []string{"embedding"}},
		},
	}
	renamePrimaryKeyID(&table)
	return table
}

func vectorConnectionFieldAST() *ast.Field {
	return &ast.Field{
		Name: &ast.Name{Value: "searchDocsByEmbeddingVector"},
		SelectionSet: &ast.SelectionSet{Selections: []ast.Selection{
			&ast.Field{
				Name: &ast.Name{Value: "edges"},
				SelectionSet: &ast.SelectionSet{Selections: []ast.Selection{
					&ast.Field{Name: &ast.Name{Value: "cursor"}},
					&ast.Field{Name: &ast.Name{Value: "distance"}},
					&ast.Field{Name: &ast.Name{Value: "rank"}},
					&ast.Field{
						Name: &ast.Name{Value: "node"},
						SelectionSet: &ast.SelectionSet{Selections: []ast.Selection{
							&ast.Field{Name: &ast.Name{Value: "databaseId"}},
							&ast.Field{Name: &ast.Name{Value: "title"}},
						}},
					},
				}},
			},
			&ast.Field{
				Name: &ast.Name{Value: "nodes"},
				SelectionSet: &ast.SelectionSet{Selections: []ast.Selection{
					&ast.Field{Name: &ast.Name{Value: "databaseId"}},
					&ast.Field{Name: &ast.Name{Value: "title"}},
				}},
			},
			&ast.Field{
				Name: &ast.Name{Value: "pageInfo"},
				SelectionSet: &ast.SelectionSet{Selections: []ast.Selection{
					&ast.Field{Name: &ast.Name{Value: "hasNextPage"}},
					&ast.Field{Name: &ast.Name{Value: "endCursor"}},
				}},
			},
		}},
	}
}

func TestBuildSchema_VectorSearchFieldGenerated(t *testing.T) {
	docs := vectorDocsTable()
	r := NewResolver(nil, &introspection.Schema{Tables: []introspection.Table{docs}}, nil, 0, schemafilter.Config{}, naming.DefaultConfig())
	r.SetVectorSearchConfig(VectorSearchConfig{RequireIndex: true, MaxTopK: 100})

	schema, err := r.BuildGraphQLSchema()
	require.NoError(t, err)

	root := schema.QueryType()
	require.NotNil(t, root)
	field := root.Fields()["searchDocsByEmbeddingVector"]
	require.NotNil(t, field)
	assert.True(t, hasArg(field, "vector"))
	assert.False(t, hasArg(field, "queryText"))
	assert.True(t, hasArg(field, "metric"))
	assert.True(t, hasArg(field, "first"))
	assert.True(t, hasArg(field, "after"))
	assert.True(t, hasArg(field, "where"))
	vectorArg := getArg(field, "vector")
	require.NotNil(t, vectorArg)
	_, isNonNull := vectorArg.Type.(*graphql.NonNull)
	assert.True(t, isNonNull)

	connObj := unwrapObjectType(t, field.Type)
	nodesField, ok := connObj.Fields()["nodes"]
	require.True(t, ok)
	nodesOuterNonNull, ok := nodesField.Type.(*graphql.NonNull)
	require.True(t, ok)
	nodesList, ok := nodesOuterNonNull.OfType.(*graphql.List)
	require.True(t, ok)
	nodesInnerNonNull, ok := nodesList.OfType.(*graphql.NonNull)
	require.True(t, ok)
	nodesObj, ok := nodesInnerNonNull.OfType.(*graphql.Object)
	require.True(t, ok)
	assert.Equal(t, introspection.GraphQLTypeName(docs), nodesObj.Name())
}

func TestBuildSchema_VectorSearchFieldGenerated_AutoEmbedding(t *testing.T) {
	docs := vectorAutoEmbeddingDocsTable()
	r := NewResolver(nil, &introspection.Schema{Tables: []introspection.Table{docs}}, nil, 0, schemafilter.Config{}, naming.DefaultConfig())
	r.SetVectorSearchConfig(VectorSearchConfig{RequireIndex: true, MaxTopK: 100})

	schema, err := r.BuildGraphQLSchema()
	require.NoError(t, err)

	root := schema.QueryType()
	require.NotNil(t, root)
	field := root.Fields()["searchDocsByEmbeddingVector"]
	require.NotNil(t, field)
	assert.True(t, hasArg(field, "vector"))
	assert.True(t, hasArg(field, "queryText"))

	vectorArg := getArg(field, "vector")
	require.NotNil(t, vectorArg)
	_, isNonNull := vectorArg.Type.(*graphql.NonNull)
	assert.False(t, isNonNull)
}

func TestVectorConnectionResolver_PaginatesWithAfterCursor(t *testing.T) {
	db, mock := newMockDB(t)
	defer db.Close()

	docs := vectorDocsTable()
	schema := &introspection.Schema{Tables: []introspection.Table{docs}}
	r := NewResolver(dbexec.NewStandardExecutor(db), schema, nil, 0, schemafilter.Config{}, naming.DefaultConfig())
	r.SetVectorSearchConfig(VectorSearchConfig{RequireIndex: true, MaxTopK: 100, DefaultFirst: 2})

	field := vectorConnectionFieldAST()
	args := map[string]interface{}{
		"vector": []interface{}{0.1, 0.2, 0.3},
		"first":  2,
	}

	plan, err := planner.PlanVectorSearchConnection(schema, docs, docs.Columns[1], field, args, 100, 2, planner.WithSchema(schema))
	require.NoError(t, err)

	rows := sqlmock.NewRows([]string{"id", "title", "__vector_distance"}).
		AddRow(1, "first", 0.1).
		AddRow(2, "second", 0.2).
		AddRow(3, "third", 0.3)
	expectQuery(t, mock, plan.Root.SQL, plan.Root.Args, rows)

	resolverFn := r.makeVectorConnectionResolver(docs, docs.Columns[1])
	result, err := resolverFn(graphql.ResolveParams{
		Args:    args,
		Context: context.Background(),
		Info: graphql.ResolveInfo{
			FieldASTs: []*ast.Field{field},
		},
	})
	require.NoError(t, err)

	conn, ok := result.(map[string]interface{})
	require.True(t, ok)
	edges, ok := conn["edges"].([]map[string]interface{})
	require.True(t, ok)
	require.Len(t, edges, 2)
	nodes, ok := conn["nodes"].([]map[string]interface{})
	require.True(t, ok)
	require.Len(t, nodes, 2)
	assert.EqualValues(t, 1, nodes[0]["databaseId"])
	assert.Equal(t, "first", nodes[0]["title"])
	assert.EqualValues(t, 1, edges[0]["rank"])
	assert.EqualValues(t, 2, edges[1]["rank"])
	pageInfo := conn["pageInfo"].(map[string]interface{})
	assert.Equal(t, true, pageInfo["hasNextPage"])
	after, _ := pageInfo["endCursor"].(string)
	require.NotEmpty(t, after)

	args2 := map[string]interface{}{
		"vector": []interface{}{0.1, 0.2, 0.3},
		"first":  2,
		"after":  after,
	}
	plan2, err := planner.PlanVectorSearchConnection(schema, docs, docs.Columns[1], field, args2, 100, 2, planner.WithSchema(schema))
	require.NoError(t, err)
	rows2 := sqlmock.NewRows([]string{"id", "title", "__vector_distance"}).
		AddRow(3, "third", 0.3)
	expectQuery(t, mock, plan2.Root.SQL, plan2.Root.Args, rows2)

	result2, err := resolverFn(graphql.ResolveParams{
		Args:    args2,
		Context: context.Background(),
		Info: graphql.ResolveInfo{
			FieldASTs: []*ast.Field{field},
		},
	})
	require.NoError(t, err)
	conn2, ok := result2.(map[string]interface{})
	require.True(t, ok)
	edges2, ok := conn2["edges"].([]map[string]interface{})
	require.True(t, ok)
	require.Len(t, edges2, 1)
	assert.EqualValues(t, 3, edges2[0]["node"].(map[string]interface{})["databaseId"])

	require.NoError(t, mock.ExpectationsWereMet())
}

func TestVectorConnectionResolver_TextMode(t *testing.T) {
	db, mock := newMockDB(t)
	defer db.Close()

	docs := vectorAutoEmbeddingDocsTable()
	schema := &introspection.Schema{Tables: []introspection.Table{docs}}
	r := NewResolver(dbexec.NewStandardExecutor(db), schema, nil, 0, schemafilter.Config{}, naming.DefaultConfig())
	r.SetVectorSearchConfig(VectorSearchConfig{RequireIndex: true, MaxTopK: 100, DefaultFirst: 2})

	field := vectorConnectionFieldAST()
	args := map[string]interface{}{
		"queryText": "great battery life",
		"metric":    "COSINE",
		"first":     2,
	}

	plan, err := planner.PlanVectorSearchConnection(schema, docs, docs.Columns[1], field, args, 100, 2, planner.WithSchema(schema))
	require.NoError(t, err)
	require.Contains(t, plan.Root.SQL, "VEC_EMBED_COSINE_DISTANCE")

	rows := sqlmock.NewRows([]string{"id", "title", "__vector_distance"}).
		AddRow(1, "first", 0.1).
		AddRow(2, "second", 0.2)
	expectQuery(t, mock, plan.Root.SQL, plan.Root.Args, rows)

	resolverFn := r.makeVectorConnectionResolver(docs, docs.Columns[1])
	result, err := resolverFn(graphql.ResolveParams{
		Args:    args,
		Context: context.Background(),
		Info: graphql.ResolveInfo{
			FieldASTs: []*ast.Field{field},
		},
	})
	require.NoError(t, err)

	conn, ok := result.(map[string]interface{})
	require.True(t, ok)
	edges, ok := conn["edges"].([]map[string]interface{})
	require.True(t, ok)
	require.Len(t, edges, 2)
	assert.EqualValues(t, 1, edges[0]["rank"])
	assert.EqualValues(t, 1, edges[0]["node"].(map[string]interface{})["databaseId"])

	require.NoError(t, mock.ExpectationsWereMet())
}
