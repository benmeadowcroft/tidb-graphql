package resolver

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"regexp"
	"testing"
	"time"

	"tidb-graphql/internal/cursor"
	"tidb-graphql/internal/dbexec"
	"tidb-graphql/internal/introspection"
	"tidb-graphql/internal/naming"
	"tidb-graphql/internal/nodeid"
	"tidb-graphql/internal/planner"
	"tidb-graphql/internal/scalars"
	"tidb-graphql/internal/schemafilter"
	"tidb-graphql/internal/sqltype"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/go-sql-driver/mysql"
	"github.com/graphql-go/graphql"
	"github.com/graphql-go/graphql/language/ast"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func assertNonNullListOfNonNullObject(t *testing.T, typ graphql.Type) {
	t.Helper()

	outerNonNull, ok := typ.(*graphql.NonNull)
	require.True(t, ok, "expected outer NonNull, got %T", typ)

	list, ok := outerNonNull.OfType.(*graphql.List)
	require.True(t, ok, "expected List, got %T", outerNonNull.OfType)

	innerNonNull, ok := list.OfType.(*graphql.NonNull)
	require.True(t, ok, "expected inner NonNull, got %T", list.OfType)

	_, ok = innerNonNull.OfType.(*graphql.Object)
	require.True(t, ok, "expected Object, got %T", innerNonNull.OfType)
}

func hasArg(field *graphql.FieldDefinition, name string) bool {
	for _, arg := range field.Args {
		if arg != nil && arg.Name() == name {
			return true
		}
	}
	return false
}

func getArg(field *graphql.FieldDefinition, name string) *graphql.Argument {
	for _, arg := range field.Args {
		if arg != nil && arg.Name() == name {
			return arg
		}
	}
	return nil
}

func unwrapObjectType(t *testing.T, typ graphql.Type) *graphql.Object {
	t.Helper()
	if nonNull, ok := typ.(*graphql.NonNull); ok {
		typ = nonNull.OfType
	}
	obj, ok := typ.(*graphql.Object)
	require.True(t, ok, "expected object type, got %T", typ)
	return obj
}

func TestConnectionResolver(t *testing.T) {
	db, mock := newMockDB(t)
	defer db.Close()

	users := introspection.Table{
		Name: "users",
		Columns: []introspection.Column{
			{Name: "id", DataType: "int", IsPrimaryKey: true},
			{Name: "username"},
		},
	}
	renamePrimaryKeyID(&users)
	dbSchema := &introspection.Schema{Tables: []introspection.Table{users}}
	r := NewResolver(dbexec.NewStandardExecutor(db), dbSchema, nil, 0, schemafilter.Config{}, naming.DefaultConfig())

	field := &ast.Field{
		Name: &ast.Name{Value: "users"},
		SelectionSet: &ast.SelectionSet{Selections: []ast.Selection{
			&ast.Field{
				Name: &ast.Name{Value: "nodes"},
				SelectionSet: &ast.SelectionSet{Selections: []ast.Selection{
					&ast.Field{Name: &ast.Name{Value: "id"}},
					&ast.Field{Name: &ast.Name{Value: "username"}},
				}},
			},
		}},
	}
	args := map[string]interface{}{"first": 2}
	plan, err := planner.PlanConnection(dbSchema, users, field, args)
	require.NoError(t, err)

	rows := sqlmock.NewRows([]string{"id", "username"}).
		AddRow(1, []byte("alice")).
		AddRow(2, "bob").
		AddRow(3, "charlie")
	expectQuery(t, mock, plan.Root.SQL, plan.Root.Args, rows)

	resolverFn := r.makeConnectionResolver(users)
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
	records, ok := conn["nodes"].([]map[string]interface{})
	require.True(t, ok)
	require.Len(t, records, 2)
	assert.EqualValues(t, 1, records[0]["databaseId"])
	assert.Equal(t, "alice", records[0]["username"])
	assert.EqualValues(t, 2, records[1]["databaseId"])
	assert.Equal(t, "bob", records[1]["username"])

	require.NoError(t, mock.ExpectationsWereMet())
}

func TestConnectionResolver_Empty(t *testing.T) {
	db, mock := newMockDB(t)
	defer db.Close()

	users := introspection.Table{
		Name: "users",
		Columns: []introspection.Column{
			{Name: "id", DataType: "int", IsPrimaryKey: true},
			{Name: "username"},
		},
	}
	renamePrimaryKeyID(&users)
	dbSchema := &introspection.Schema{Tables: []introspection.Table{users}}
	r := NewResolver(dbexec.NewStandardExecutor(db), dbSchema, nil, 0, schemafilter.Config{}, naming.DefaultConfig())

	field := &ast.Field{
		Name: &ast.Name{Value: "users"},
		SelectionSet: &ast.SelectionSet{Selections: []ast.Selection{
			&ast.Field{
				Name: &ast.Name{Value: "nodes"},
				SelectionSet: &ast.SelectionSet{Selections: []ast.Selection{
					&ast.Field{Name: &ast.Name{Value: "id"}},
					&ast.Field{Name: &ast.Name{Value: "username"}},
				}},
			},
		}},
	}
	args := map[string]interface{}{"first": 2}
	plan, err := planner.PlanConnection(dbSchema, users, field, args)
	require.NoError(t, err)

	rows := sqlmock.NewRows([]string{"id", "username"})
	expectQuery(t, mock, plan.Root.SQL, plan.Root.Args, rows)

	resolverFn := r.makeConnectionResolver(users)
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
	records, ok := conn["nodes"].([]map[string]interface{})
	require.True(t, ok)
	assert.NotNil(t, records)
	assert.Len(t, records, 0)

	require.NoError(t, mock.ExpectationsWereMet())
}

func TestConnectionResolver_BackwardLastWithoutBefore(t *testing.T) {
	db, mock := newMockDB(t)
	defer db.Close()

	users := introspection.Table{
		Name: "users",
		Columns: []introspection.Column{
			{Name: "id", DataType: "int", IsPrimaryKey: true},
			{Name: "username"},
		},
	}
	renamePrimaryKeyID(&users)
	dbSchema := &introspection.Schema{Tables: []introspection.Table{users}}
	r := NewResolver(dbexec.NewStandardExecutor(db), dbSchema, nil, 0, schemafilter.Config{}, naming.DefaultConfig())

	field := &ast.Field{
		Name: &ast.Name{Value: "users"},
		SelectionSet: &ast.SelectionSet{Selections: []ast.Selection{
			&ast.Field{
				Name: &ast.Name{Value: "nodes"},
				SelectionSet: &ast.SelectionSet{Selections: []ast.Selection{
					&ast.Field{Name: &ast.Name{Value: "id"}},
				}},
			},
		}},
	}
	args := map[string]interface{}{"last": 2}
	plan, err := planner.PlanConnection(dbSchema, users, field, args)
	require.NoError(t, err)

	// Backward plans query in reverse order and are re-ordered before response.
	rows := sqlmock.NewRows([]string{"id"}).
		AddRow(4).
		AddRow(3).
		AddRow(2)
	expectQuery(t, mock, plan.Root.SQL, plan.Root.Args, rows)

	resolverFn := r.makeConnectionResolver(users)
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
	records, ok := conn["nodes"].([]map[string]interface{})
	require.True(t, ok)
	require.Len(t, records, 2)
	assert.EqualValues(t, 3, records[0]["databaseId"])
	assert.EqualValues(t, 4, records[1]["databaseId"])

	pageInfo, ok := conn["pageInfo"].(map[string]interface{})
	require.True(t, ok)
	assert.Equal(t, false, pageInfo["hasNextPage"])
	assert.Equal(t, true, pageInfo["hasPreviousPage"])

	require.NoError(t, mock.ExpectationsWereMet())
}

func TestConnectionResolver_BackwardBeforeSetsHasNextLightweight(t *testing.T) {
	db, mock := newMockDB(t)
	defer db.Close()

	users := introspection.Table{
		Name: "users",
		Columns: []introspection.Column{
			{Name: "id", DataType: "int", IsPrimaryKey: true},
			{Name: "username"},
		},
	}
	renamePrimaryKeyID(&users)
	dbSchema := &introspection.Schema{Tables: []introspection.Table{users}}
	r := NewResolver(dbexec.NewStandardExecutor(db), dbSchema, nil, 0, schemafilter.Config{}, naming.DefaultConfig())

	field := &ast.Field{
		Name: &ast.Name{Value: "users"},
		SelectionSet: &ast.SelectionSet{Selections: []ast.Selection{
			&ast.Field{
				Name: &ast.Name{Value: "nodes"},
				SelectionSet: &ast.SelectionSet{Selections: []ast.Selection{
					&ast.Field{Name: &ast.Name{Value: "id"}},
				}},
			},
		}},
	}
	before := cursor.EncodeCursor("Users", "databaseId", []string{"ASC"}, 4)
	args := map[string]interface{}{
		"last":   1,
		"before": before,
	}
	plan, err := planner.PlanConnection(dbSchema, users, field, args)
	require.NoError(t, err)

	rows := sqlmock.NewRows([]string{"id"}).
		AddRow(3).
		AddRow(2)
	expectQuery(t, mock, plan.Root.SQL, plan.Root.Args, rows)

	resolverFn := r.makeConnectionResolver(users)
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
	records, ok := conn["nodes"].([]map[string]interface{})
	require.True(t, ok)
	require.Len(t, records, 1)
	assert.EqualValues(t, 3, records[0]["databaseId"])

	pageInfo, ok := conn["pageInfo"].(map[string]interface{})
	require.True(t, ok)
	assert.Equal(t, true, pageInfo["hasNextPage"])
	assert.Equal(t, true, pageInfo["hasPreviousPage"])

	require.NoError(t, mock.ExpectationsWereMet())
}

func TestPKResolver(t *testing.T) {
	db, mock := newMockDB(t)
	defer db.Close()

	users := introspection.Table{
		Name: "users",
		Columns: []introspection.Column{
			{Name: "id", DataType: "int", IsPrimaryKey: true},
			{Name: "username"},
		},
	}
	renamePrimaryKeyID(&users)
	dbSchema := &introspection.Schema{Tables: []introspection.Table{users}}
	r := NewResolver(dbexec.NewStandardExecutor(db), dbSchema, nil, 0, schemafilter.Config{}, naming.DefaultConfig())

	field := &ast.Field{Name: &ast.Name{Value: "user"}}
	pkCols := introspection.PrimaryKeyColumns(users)
	nodeID := nodeid.Encode(introspection.GraphQLTypeName(users), int64(1))
	args := map[string]interface{}{"id": nodeID}
	query, err := planner.PlanTableByPK(users, users.Columns, &pkCols[0], int64(1))
	require.NoError(t, err)

	rows := sqlmock.NewRows([]string{"id", "username"}).
		AddRow(1, []byte("alice"))
	expectQuery(t, mock, query.SQL, query.Args, rows)

	resolverFn := r.makePrimaryKeyResolver(users, pkCols)
	result, err := resolverFn(graphql.ResolveParams{
		Args:    args,
		Context: context.Background(),
		Info: graphql.ResolveInfo{
			FieldASTs: []*ast.Field{field},
		},
	})
	require.NoError(t, err)

	record, ok := result.(map[string]interface{})
	require.True(t, ok)
	assert.EqualValues(t, 1, record["databaseId"])
	assert.Equal(t, "alice", record["username"])

	require.NoError(t, mock.ExpectationsWereMet())
}

func TestNodeIDField(t *testing.T) {
	table := introspection.Table{
		Name: "users",
		Columns: []introspection.Column{
			{Name: "id", IsPrimaryKey: true},
			{Name: "username"},
		},
	}
	renamePrimaryKeyID(&table)
	dbSchema := &introspection.Schema{Tables: []introspection.Table{table}}
	r := NewResolver(nil, dbSchema, nil, 0, schemafilter.Config{}, naming.DefaultConfig())

	pkCols := introspection.PrimaryKeyColumns(table)
	resolverFn := r.makeNodeIDResolver(table, pkCols)
	result, err := resolverFn(graphql.ResolveParams{
		Source: map[string]interface{}{
			"databaseId": int64(5),
		},
	})
	require.NoError(t, err)

	expected := nodeid.Encode(introspection.GraphQLTypeName(table), int64(5))
	assert.Equal(t, expected, result)
}

func TestNodeResolver(t *testing.T) {
	db, mock := newMockDB(t)
	defer db.Close()

	users := introspection.Table{
		Name: "users",
		Columns: []introspection.Column{
			{Name: "id", DataType: "int", IsPrimaryKey: true},
			{Name: "username"},
		},
	}
	renamePrimaryKeyID(&users)
	dbSchema := &introspection.Schema{Tables: []introspection.Table{users}}
	r := NewResolver(dbexec.NewStandardExecutor(db), dbSchema, nil, 0, schemafilter.Config{}, naming.DefaultConfig())

	nodeID := nodeid.Encode(introspection.GraphQLTypeName(users), int64(1))
	query, err := planner.PlanTableByPK(users, users.Columns, &users.Columns[0], int64(1))
	require.NoError(t, err)

	rows := sqlmock.NewRows([]string{"id", "username"}).
		AddRow(1, []byte("alice"))
	expectQuery(t, mock, query.SQL, query.Args, rows)

	field := &ast.Field{Name: &ast.Name{Value: "node"}}
	resolverFn := r.makeNodeResolver()
	result, err := resolverFn(graphql.ResolveParams{
		Args: map[string]interface{}{
			"id": nodeID,
		},
		Context: context.Background(),
		Info: graphql.ResolveInfo{
			FieldASTs: []*ast.Field{field},
		},
	})
	require.NoError(t, err)

	record, ok := result.(map[string]interface{})
	require.True(t, ok)
	assert.EqualValues(t, 1, record["databaseId"])
	assert.Equal(t, "alice", record["username"])
	assert.Equal(t, introspection.GraphQLTypeName(users), record["__typename"])

	require.NoError(t, mock.ExpectationsWereMet())
}

func TestPKValuesFromDecodedNodeID(t *testing.T) {
	users := introspection.Table{
		Name: "users",
		Columns: []introspection.Column{
			{Name: "id", DataType: "int", IsPrimaryKey: true},
		},
	}
	renamePrimaryKeyID(&users)
	r := NewResolver(nil, &introspection.Schema{Tables: []introspection.Table{users}}, nil, 0, schemafilter.Config{}, naming.DefaultConfig())
	pkCols := introspection.PrimaryKeyColumns(users)

	t.Run("success", func(t *testing.T) {
		values := []interface{}{float64(1)}
		got, err := r.pkValuesFromDecodedNodeID(users, pkCols, introspection.GraphQLTypeName(users), values)
		require.NoError(t, err)
		require.EqualValues(t, int64(1), got["id"])
	})

	t.Run("type mismatch", func(t *testing.T) {
		_, err := r.pkValuesFromDecodedNodeID(users, pkCols, "Orders", []interface{}{float64(1)})
		require.EqualError(t, err, "invalid id for Users")
	})

	t.Run("wrong arity", func(t *testing.T) {
		_, err := r.pkValuesFromDecodedNodeID(users, pkCols, introspection.GraphQLTypeName(users), []interface{}{})
		require.EqualError(t, err, "invalid id for Users")
	})

	t.Run("parse failure", func(t *testing.T) {
		_, err := r.pkValuesFromDecodedNodeID(users, pkCols, introspection.GraphQLTypeName(users), []interface{}{"not-an-int"})
		require.Error(t, err)
	})
}

func TestRelationshipConnectionFields_Wiring(t *testing.T) {
	users := introspection.Table{
		Name: "users",
		Columns: []introspection.Column{
			{Name: "id", DataType: "int", IsPrimaryKey: true},
			{Name: "name"},
		},
	}
	posts := introspection.Table{
		Name: "posts",
		Columns: []introspection.Column{
			{Name: "id", DataType: "int", IsPrimaryKey: true},
			{Name: "user_id"},
		},
	}
	tags := introspection.Table{
		Name: "tags",
		Columns: []introspection.Column{
			{Name: "id", DataType: "int", IsPrimaryKey: true},
		},
	}
	userTags := introspection.Table{
		Name: "user_tags",
		Columns: []introspection.Column{
			{Name: "user_id", DataType: "int", IsPrimaryKey: true},
			{Name: "tag_id", DataType: "int", IsPrimaryKey: true},
		},
	}

	renamePrimaryKeyID(&users)
	renamePrimaryKeyID(&posts)
	renamePrimaryKeyID(&tags)
	renamePrimaryKeyID(&userTags)

	users.Relationships = []introspection.Relationship{
		{
			IsOneToMany:      true,
			LocalColumns:     []string{"id"},
			RemoteTable:      "posts",
			RemoteColumns:    []string{"user_id"},
			GraphQLFieldName: "posts",
		},
		{
			IsManyToMany:            true,
			LocalColumns:            []string{"id"},
			RemoteTable:             "tags",
			RemoteColumns:           []string{"id"},
			JunctionTable:           "user_tags",
			JunctionLocalFKColumns:  []string{"user_id"},
			JunctionRemoteFKColumns: []string{"tag_id"},
			GraphQLFieldName:        "tags",
		},
		{
			IsEdgeList:             true,
			LocalColumns:           []string{"id"},
			JunctionTable:          "user_tags",
			JunctionLocalFKColumns: []string{"user_id"},
			GraphQLFieldName:       "userTags",
		},
	}

	dbSchema := &introspection.Schema{Tables: []introspection.Table{users, posts, tags, userTags}}
	r := NewResolver(nil, dbSchema, nil, 0, schemafilter.Config{}, naming.DefaultConfig())

	userType := r.buildGraphQLType(users)
	fields := userType.Fields()

	tagsConn, ok := fields["tags"]
	require.True(t, ok, "expected tags field")
	require.True(t, hasArg(tagsConn, "first"), "expected tags first arg")
	require.True(t, hasArg(tagsConn, "after"), "expected tags after arg")
	require.True(t, hasArg(tagsConn, "last"), "expected tags last arg")
	require.True(t, hasArg(tagsConn, "before"), "expected tags before arg")
	require.True(t, hasArg(tagsConn, "where"), "expected tags where arg")

	_, hasLegacyTagsConnection := fields["tagsConnection"]
	assert.False(t, hasLegacyTagsConnection, "did not expect tagsConnection legacy field")

	userTagsConn, ok := fields["userTags"]
	require.True(t, ok, "expected userTags field")
	require.True(t, hasArg(userTagsConn, "first"), "expected userTags first arg")
	require.True(t, hasArg(userTagsConn, "after"), "expected userTags after arg")
	require.True(t, hasArg(userTagsConn, "last"), "expected userTags last arg")
	require.True(t, hasArg(userTagsConn, "before"), "expected userTags before arg")
	require.True(t, hasArg(userTagsConn, "where"), "expected userTags where arg")

	_, hasLegacyUserTagsConnection := fields["userTagsConnection"]
	assert.False(t, hasLegacyUserTagsConnection, "did not expect userTagsConnection legacy field")

	postsConn, ok := fields["posts"]
	require.True(t, ok, "expected posts field")
	require.True(t, hasArg(postsConn, "first"), "expected posts first arg")
	require.True(t, hasArg(postsConn, "after"), "expected posts after arg")
	require.True(t, hasArg(postsConn, "last"), "expected posts last arg")
	require.True(t, hasArg(postsConn, "before"), "expected posts before arg")
	require.True(t, hasArg(postsConn, "where"), "expected posts where arg")

	_, hasLegacyPostsConnection := fields["postsConnection"]
	assert.False(t, hasLegacyPostsConnection, "did not expect postsConnection legacy field")
}

func TestConnectionAggregateSchemaWiring(t *testing.T) {
	users := introspection.Table{
		Name: "users",
		Columns: []introspection.Column{
			{Name: "id", DataType: "int", IsPrimaryKey: true},
			{Name: "name"},
		},
	}
	posts := introspection.Table{
		Name: "posts",
		Columns: []introspection.Column{
			{Name: "id", DataType: "int", IsPrimaryKey: true},
			{Name: "user_id"},
			{Name: "score", DataType: "int"},
		},
	}
	renamePrimaryKeyID(&users)
	renamePrimaryKeyID(&posts)
	users.Relationships = []introspection.Relationship{
		{
			IsOneToMany:      true,
			LocalColumns:     []string{"id"},
			RemoteTable:      "posts",
			RemoteColumns:    []string{"user_id"},
			GraphQLFieldName: "posts",
		},
	}

	dbSchema := &introspection.Schema{Tables: []introspection.Table{users, posts}}
	r := NewResolver(nil, dbSchema, nil, 0, schemafilter.Config{}, naming.DefaultConfig())

	schema, err := r.BuildGraphQLSchema()
	require.NoError(t, err)

	queryFields := schema.QueryType().Fields()
	_, hasStandaloneAggregate := queryFields["users_aggregate"]
	assert.False(t, hasStandaloneAggregate, "standalone aggregate root field should not be generated")

	usersConnection := queryFields["users"]
	require.NotNil(t, usersConnection, "expected users root connection field")
	usersConnObj := unwrapObjectType(t, usersConnection.Type)
	_, hasConnectionAggregate := usersConnObj.Fields()["aggregate"]
	assert.True(t, hasConnectionAggregate, "expected aggregate field on connection type")

	userType := r.buildGraphQLType(users)
	userFields := userType.Fields()
	_, hasRelationshipAggregate := userFields["posts_aggregate"]
	assert.False(t, hasRelationshipAggregate, "standalone relationship aggregate field should not be generated")

	postsField, ok := userFields["posts"]
	require.True(t, ok, "expected posts relationship connection field")
	postsConnObj := unwrapObjectType(t, postsField.Type)
	_, hasRelationshipConnectionAggregate := postsConnObj.Fields()["aggregate"]
	assert.True(t, hasRelationshipConnectionAggregate, "expected aggregate field on relationship connection type")
}

func TestConnectionOrderByPolicyArgWiring(t *testing.T) {
	users := introspection.Table{
		Name: "users",
		Columns: []introspection.Column{
			{Name: "id", IsPrimaryKey: true},
			{Name: "email"},
		},
		Indexes: []introspection.Index{
			{Name: "PRIMARY", Unique: true, Columns: []string{"id"}},
			{Name: "idx_users_email", Columns: []string{"email"}},
		},
		Relationships: []introspection.Relationship{
			{
				IsOneToMany:      true,
				LocalColumns:     []string{"id"},
				RemoteTable:      "posts",
				RemoteColumns:    []string{"user_id"},
				GraphQLFieldName: "posts",
			},
		},
	}
	posts := introspection.Table{
		Name: "posts",
		Columns: []introspection.Column{
			{Name: "id", IsPrimaryKey: true},
			{Name: "user_id"},
			{Name: "created_at"},
		},
		Indexes: []introspection.Index{
			{Name: "PRIMARY", Unique: true, Columns: []string{"id"}},
			{Name: "idx_posts_user_created", Columns: []string{"user_id", "created_at"}},
		},
	}

	renamePrimaryKeyID(&users)
	renamePrimaryKeyID(&posts)

	dbSchema := &introspection.Schema{Tables: []introspection.Table{users, posts}}
	r := NewResolver(nil, dbSchema, nil, 0, schemafilter.Config{}, naming.DefaultConfig())

	schema, err := r.BuildGraphQLSchema()
	require.NoError(t, err)

	rootCollectionField := schema.QueryType().Fields()["users"]
	require.NotNil(t, rootCollectionField)
	require.True(t, hasArg(rootCollectionField, "orderBy"), "expected users orderBy arg")
	require.True(t, hasArg(rootCollectionField, "orderByPolicy"), "expected users orderByPolicy arg")

	orderByPolicyArg := getArg(rootCollectionField, "orderByPolicy")
	require.NotNil(t, orderByPolicyArg)
	policyEnum, ok := orderByPolicyArg.Type.(*graphql.Enum)
	require.True(t, ok, "expected orderByPolicy enum type, got %T", orderByPolicyArg.Type)
	var hasIndexPrefixOnly bool
	var hasAllowNonPrefix bool
	for _, value := range policyEnum.Values() {
		if value.Name == "INDEX_PREFIX_ONLY" {
			hasIndexPrefixOnly = true
		}
		if value.Name == "ALLOW_NON_PREFIX" {
			hasAllowNonPrefix = true
		}
	}
	require.True(t, hasIndexPrefixOnly, "expected INDEX_PREFIX_ONLY enum value")
	require.True(t, hasAllowNonPrefix, "expected ALLOW_NON_PREFIX enum value")

	userType := r.buildGraphQLType(users)
	fields := userType.Fields()
	postsField := fields["posts"]
	require.NotNil(t, postsField)
	require.True(t, hasArg(postsField, "orderBy"), "expected posts relationship orderBy arg")
	require.True(t, hasArg(postsField, "orderByPolicy"), "expected posts relationship orderByPolicy arg")
}

func TestRootCollectionFieldNotGeneratedWithoutPrimaryKey(t *testing.T) {
	logs := introspection.Table{
		Name: "logs",
		Columns: []introspection.Column{
			{Name: "message"},
		},
	}

	dbSchema := &introspection.Schema{Tables: []introspection.Table{logs}}
	r := NewResolver(nil, dbSchema, nil, 0, schemafilter.Config{}, naming.DefaultConfig())

	schema, err := r.BuildGraphQLSchema()
	require.NoError(t, err)

	queryFields := schema.QueryType().Fields()
	_, hasLogsCollection := queryFields["logs"]
	assert.False(t, hasLogsCollection, "did not expect root collection field for table without primary key")
}

func TestRelationshipCollectionFieldNotGeneratedWithoutRelatedPrimaryKey(t *testing.T) {
	users := introspection.Table{
		Name: "users",
		Columns: []introspection.Column{
			{Name: "id", IsPrimaryKey: true},
			{Name: "name"},
		},
		Relationships: []introspection.Relationship{
			{
				IsOneToMany:      true,
				LocalColumns:     []string{"id"},
				RemoteTable:      "posts",
				RemoteColumns:    []string{"user_id"},
				GraphQLFieldName: "posts",
			},
		},
	}
	posts := introspection.Table{
		Name: "posts",
		Columns: []introspection.Column{
			{Name: "user_id"},
			{Name: "title"},
		},
	}

	renamePrimaryKeyID(&users)
	dbSchema := &introspection.Schema{Tables: []introspection.Table{users, posts}}
	r := NewResolver(nil, dbSchema, nil, 0, schemafilter.Config{}, naming.DefaultConfig())

	userType := r.buildGraphQLType(users)
	fields := userType.Fields()
	_, hasPostsCollection := fields["posts"]
	assert.False(t, hasPostsCollection, "did not expect relationship collection field when related table has no primary key")
}

func TestConnectionResultAggregate_CountOnlyUsesCountQuery(t *testing.T) {
	db, mock := newMockDB(t)
	defer db.Close()

	table := introspection.Table{
		Name: "orders",
		Columns: []introspection.Column{
			{Name: "id", DataType: "int", IsPrimaryKey: true},
		},
	}
	countQuery, err := planner.BuildOneToManyCountSQL(table, "id", int64(1), nil)
	require.NoError(t, err)
	aggregateBase, err := planner.BuildOneToManyAggregateBaseSQL(table, "id", int64(1), nil)
	require.NoError(t, err)

	rows := sqlmock.NewRows([]string{"count"}).AddRow(3)
	expectQuery(t, mock, countQuery.SQL, countQuery.Args, rows)

	cr := &connectionResult{
		plan:          &planner.ConnectionPlan{Count: countQuery, AggregateBase: aggregateBase, Table: table},
		executor:      dbexec.NewStandardExecutor(db),
		countCtx:      context.Background(),
		aggregateVals: make(map[string]map[string]interface{}),
	}

	result, err := cr.aggregate(planner.AggregateSelection{Count: true})
	require.NoError(t, err)
	assert.EqualValues(t, 3, result["count"])
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestConnectionResultAggregate_NonCountSeedsTotalCount(t *testing.T) {
	db, mock := newMockDB(t)
	defer db.Close()

	table := introspection.Table{
		Name: "orders",
		Columns: []introspection.Column{
			{Name: "id", DataType: "int", IsPrimaryKey: true},
			{Name: "amount", DataType: "decimal"},
		},
	}
	countQuery, err := planner.BuildOneToManyCountSQL(table, "id", int64(1), nil)
	require.NoError(t, err)
	aggregateBase, err := planner.BuildOneToManyAggregateBaseSQL(table, "id", int64(1), nil)
	require.NoError(t, err)

	selection := planner.AggregateSelection{SumColumns: []string{"amount"}}
	aggregateQuery := planner.BuildConnectionAggregateSQL(aggregateBase, selection)
	aggRows := sqlmock.NewRows([]string{"count", "sum_amount"}).AddRow(3, 42.5)
	expectQuery(t, mock, aggregateQuery.SQL, aggregateQuery.Args, aggRows)

	cr := &connectionResult{
		plan:          &planner.ConnectionPlan{Count: countQuery, AggregateBase: aggregateBase, Table: table},
		executor:      dbexec.NewStandardExecutor(db),
		countCtx:      context.Background(),
		aggregateVals: make(map[string]map[string]interface{}),
	}

	result, err := cr.aggregate(selection)
	require.NoError(t, err)
	assert.EqualValues(t, 3, result["count"])
	sum, ok := result["sum"].(map[string]interface{})
	require.True(t, ok)
	assert.EqualValues(t, 42.5, sum["amount"])

	totalCount, err := cr.totalCount()
	require.NoError(t, err)
	assert.Equal(t, 3, totalCount)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestSchemaDescriptionsFromComments(t *testing.T) {
	table := introspection.Table{
		Name:    "users",
		Comment: "Registered users of the store.",
		Columns: []introspection.Column{
			{Name: "id", IsPrimaryKey: true, IsAutoIncrement: true, Comment: "Primary key for users."},
			{Name: "email", Comment: "User email address."},
		},
	}
	renamePrimaryKeyID(&table)
	dbSchema := &introspection.Schema{Tables: []introspection.Table{table}}
	r := NewResolver(nil, dbSchema, nil, 0, schemafilter.Config{}, naming.DefaultConfig())

	objType := r.buildGraphQLType(table)
	assert.Equal(t, "Registered users of the store.", objType.Description())
	objFields := objType.Fields()
	assert.Equal(t, "Primary key for users.", objFields["databaseId"].Description)
	assert.Equal(t, "User email address.", objFields["email"].Description)

	createInput := r.createInputType(table, table.Columns)
	createFields := createInput.Fields()
	assert.Equal(t, "User email address.", createFields["email"].Description())

	whereInput := r.whereInput(table)
	whereFields := whereInput.Fields()
	assert.Equal(t, "User email address.", whereFields["email"].Description())
}

func TestWhereInputIncludesRelationshipFiltersSingleHop(t *testing.T) {
	users := introspection.Table{
		Name: "users",
		Columns: []introspection.Column{
			{Name: "id", IsPrimaryKey: true},
			{Name: "username"},
		},
	}
	posts := introspection.Table{
		Name: "posts",
		Columns: []introspection.Column{
			{Name: "id", IsPrimaryKey: true},
			{Name: "user_id"},
			{Name: "published", DataType: "boolean"},
		},
	}
	renamePrimaryKeyID(&users)
	renamePrimaryKeyID(&posts)

	users.Relationships = []introspection.Relationship{
		{
			IsOneToMany:      true,
			LocalColumns:     []string{"id"},
			RemoteTable:      "posts",
			RemoteColumns:    []string{"user_id"},
			GraphQLFieldName: "posts",
		},
	}
	posts.Relationships = []introspection.Relationship{
		{
			IsManyToOne:      true,
			LocalColumns:     []string{"user_id"},
			RemoteTable:      "users",
			RemoteColumns:    []string{"id"},
			GraphQLFieldName: "user",
		},
	}

	dbSchema := &introspection.Schema{Tables: []introspection.Table{users, posts}}
	r := NewResolver(nil, dbSchema, nil, 0, schemafilter.Config{}, naming.DefaultConfig())

	usersWhere := r.whereInput(users)
	require.NotNil(t, usersWhere)
	usersWhereFields := usersWhere.Fields()
	postsField, ok := usersWhereFields["posts"]
	require.True(t, ok, "expected users.where.posts relationship filter field")
	postsFilterInput, ok := postsField.Type.(*graphql.InputObject)
	require.True(t, ok, "expected users.where.posts to be an input object")
	postsFilterFields := postsFilterInput.Fields()
	require.Contains(t, postsFilterFields, "some")
	require.Contains(t, postsFilterFields, "none")

	someType, ok := postsFilterFields["some"].Type.(*graphql.InputObject)
	require.True(t, ok, "expected posts.some nested where type")
	someWhereFields := someType.Fields()
	require.Contains(t, someWhereFields, "published")
	require.NotContains(t, someWhereFields, "user", "single-hop nested where should be scalar-only")

	postsWhere := r.whereInput(posts)
	require.NotNil(t, postsWhere)
	postsWhereFields := postsWhere.Fields()
	userField, ok := postsWhereFields["user"]
	require.True(t, ok, "expected posts.where.user relationship filter field")
	userFilterInput, ok := userField.Type.(*graphql.InputObject)
	require.True(t, ok, "expected posts.where.user to be an input object")
	userFilterFields := userFilterInput.Fields()
	require.Contains(t, userFilterFields, "is")
	require.Contains(t, userFilterFields, "isNull")

	isType, ok := userFilterFields["is"].Type.(*graphql.InputObject)
	require.True(t, ok, "expected user.is nested where type")
	isWhereFields := isType.Fields()
	require.Contains(t, isWhereFields, "username")
	require.NotContains(t, isWhereFields, "posts", "single-hop nested where should be scalar-only")
}

func TestDeletePayloadIncludesID(t *testing.T) {
	table := introspection.Table{
		Name: "users",
		Columns: []introspection.Column{
			{Name: "id", IsPrimaryKey: true},
		},
	}
	renamePrimaryKeyID(&table)
	dbSchema := &introspection.Schema{Tables: []introspection.Table{table}}
	r := NewResolver(nil, dbSchema, nil, 0, schemafilter.Config{}, naming.DefaultConfig())

	pkCols := introspection.PrimaryKeyColumns(table)
	payload := r.deletePayloadType(table, pkCols)
	fields := payload.Fields()
	_, ok := fields["id"]
	require.True(t, ok)
	_, ok = fields["databaseId"]
	require.True(t, ok)
}

func TestMutationEnumRoundTrip_Create(t *testing.T) {
	db, mock := newMockDB(t)
	defer db.Close()

	table := introspection.Table{
		Name: "users",
		Columns: []introspection.Column{
			{Name: "id", DataType: "int", IsPrimaryKey: true, IsAutoIncrement: true},
			{Name: "status", DataType: "enum", EnumValues: []string{"ready", "pending"}, IsNullable: false},
		},
	}
	renamePrimaryKeyID(&table)
	dbSchema := &introspection.Schema{Tables: []introspection.Table{table}}
	r := NewResolver(dbexec.NewStandardExecutor(db), dbSchema, nil, 0, schemafilter.Config{}, naming.DefaultConfig())

	inputArg := map[string]interface{}{"status": "ready"}
	insertable := columnNameSet(r.mutationInsertableColumns(table))
	columns, values, err := mapInputColumns(table, inputArg, insertable)
	require.NoError(t, err)

	insertPlan, err := planner.PlanInsert(table, columns, values)
	require.NoError(t, err)

	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta(insertPlan.SQL)).
		WithArgs(toDriverValues(insertPlan.Args)...).
		WillReturnResult(sqlmock.NewResult(1, 1))

	field := &ast.Field{
		Name: &ast.Name{Value: "createUser"},
		SelectionSet: &ast.SelectionSet{Selections: []ast.Selection{
			&ast.Field{Name: &ast.Name{Value: "id"}},
			&ast.Field{Name: &ast.Name{Value: "status"}},
		}},
	}
	selected := planner.SelectedColumns(table, field, nil)
	selectPlan, err := planner.PlanTableByPK(table, selected, &table.Columns[0], int64(1))
	require.NoError(t, err)

	rows := sqlmock.NewRows([]string{"id", "status"}).AddRow(1, "ready")
	expectQuery(t, mock, selectPlan.SQL, selectPlan.Args, rows)

	tx, err := dbexec.NewStandardExecutor(db).BeginTx(context.Background())
	require.NoError(t, err)
	ctx := WithMutationContext(context.Background(), NewMutationContext(tx))

	successType := r.createSuccessType(table, r.buildGraphQLType(table))
	resolverFn := r.makeCreateResolver(table, insertable, successType)
	result, err := resolverFn(graphql.ResolveParams{
		Args: map[string]interface{}{
			"input": inputArg,
		},
		Context: ctx,
		Info: graphql.ResolveInfo{
			FieldASTs: []*ast.Field{field},
		},
	})
	require.NoError(t, err)

	wrapper, ok := result.(map[string]interface{})
	require.True(t, ok)
	record, ok := wrapper["user"].(map[string]interface{})
	require.True(t, ok)
	assert.EqualValues(t, 1, record["databaseId"])
	assert.Equal(t, "ready", record["status"])

	require.NoError(t, mock.ExpectationsWereMet())
}

func TestCreateResolver_UniqueViolation_ConflictError(t *testing.T) {
	db, mock := newMockDB(t)
	defer db.Close()

	table := introspection.Table{
		Name: "users",
		Columns: []introspection.Column{
			{Name: "id", DataType: "int", IsPrimaryKey: true, IsAutoIncrement: true},
			{Name: "username", DataType: "varchar"},
		},
	}
	renamePrimaryKeyID(&table)
	dbSchema := &introspection.Schema{Tables: []introspection.Table{table}}
	r := NewResolver(dbexec.NewStandardExecutor(db), dbSchema, nil, 0, schemafilter.Config{}, naming.DefaultConfig())

	inputArg := map[string]interface{}{"username": "alice"}
	insertable := columnNameSet(r.mutationInsertableColumns(table))
	columns, values, err := mapInputColumns(table, inputArg, insertable)
	require.NoError(t, err)
	insertPlan, err := planner.PlanInsert(table, columns, values)
	require.NoError(t, err)

	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta(insertPlan.SQL)).
		WithArgs(toDriverValues(insertPlan.Args)...).
		WillReturnError(&mysql.MySQLError{Number: 1062, Message: "Duplicate entry"})
	mock.ExpectRollback()

	tx, err := dbexec.NewStandardExecutor(db).BeginTx(context.Background())
	require.NoError(t, err)
	mc := NewMutationContext(tx)
	ctx := WithMutationContext(context.Background(), mc)

	resolverFn := r.makeCreateResolver(table, insertable, r.createSuccessType(table, r.buildGraphQLType(table)))
	result, err := resolverFn(graphql.ResolveParams{
		Args: map[string]interface{}{
			"input": inputArg,
		},
		Context: ctx,
	})
	require.NoError(t, err)

	payload, ok := result.(map[string]interface{})
	require.True(t, ok)
	assert.Equal(t, "ConflictError", payload["__typename"])
	assert.Equal(t, "Duplicate entry", payload["message"])

	require.NoError(t, mc.Finalize())
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestCreateResolver_InvalidInput_ValidationError(t *testing.T) {
	db, mock := newMockDB(t)
	defer db.Close()

	table := introspection.Table{
		Name: "users",
		Columns: []introspection.Column{
			{Name: "id", DataType: "int", IsPrimaryKey: true, IsAutoIncrement: true},
			{Name: "username", DataType: "varchar"},
		},
	}
	renamePrimaryKeyID(&table)
	dbSchema := &introspection.Schema{Tables: []introspection.Table{table}}
	r := NewResolver(dbexec.NewStandardExecutor(db), dbSchema, nil, 0, schemafilter.Config{}, naming.DefaultConfig())

	mock.ExpectBegin()
	mock.ExpectRollback()
	tx, err := dbexec.NewStandardExecutor(db).BeginTx(context.Background())
	require.NoError(t, err)
	mc := NewMutationContext(tx)
	ctx := WithMutationContext(context.Background(), mc)

	insertable := columnNameSet(r.mutationInsertableColumns(table))
	resolverFn := r.makeCreateResolver(table, insertable, r.createSuccessType(table, r.buildGraphQLType(table)))
	result, err := resolverFn(graphql.ResolveParams{
		Args: map[string]interface{}{
			"input": map[string]interface{}{"unknownField": "value"},
		},
		Context: ctx,
	})
	require.NoError(t, err)

	payload, ok := result.(map[string]interface{})
	require.True(t, ok)
	assert.Equal(t, "InputValidationError", payload["__typename"])
	assert.Contains(t, payload["message"], "unknown or disallowed column")

	require.NoError(t, mc.Finalize())
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestNestedCreateInputType_IsolatedPerParentRelation(t *testing.T) {
	users := introspection.Table{
		Name: "users",
		Columns: []introspection.Column{
			{Name: "id", DataType: "int", IsPrimaryKey: true, IsAutoIncrement: true},
			{Name: "username", DataType: "varchar"},
		},
	}
	posts := introspection.Table{
		Name: "posts",
		Columns: []introspection.Column{
			{Name: "id", DataType: "int", IsPrimaryKey: true, IsAutoIncrement: true},
			{Name: "title", DataType: "varchar"},
		},
	}
	comments := introspection.Table{
		Name: "comments",
		Columns: []introspection.Column{
			{Name: "id", DataType: "int", IsPrimaryKey: true, IsAutoIncrement: true},
			{Name: "user_id", DataType: "int", IsNullable: false},
			{Name: "post_id", DataType: "int", IsNullable: false},
			{Name: "body", DataType: "varchar"},
		},
	}
	renamePrimaryKeyID(&users)
	renamePrimaryKeyID(&posts)
	renamePrimaryKeyID(&comments)

	users.Relationships = []introspection.Relationship{
		{
			IsOneToMany:      true,
			LocalColumns:     []string{"id"},
			RemoteTable:      "comments",
			RemoteColumns:    []string{"user_id"},
			GraphQLFieldName: "comments",
		},
	}
	posts.Relationships = []introspection.Relationship{
		{
			IsOneToMany:      true,
			LocalColumns:     []string{"id"},
			RemoteTable:      "comments",
			RemoteColumns:    []string{"post_id"},
			GraphQLFieldName: "comments",
		},
	}
	comments.Relationships = []introspection.Relationship{
		{
			IsManyToOne:      true,
			LocalColumns:     []string{"user_id"},
			RemoteTable:      "users",
			RemoteColumns:    []string{"id"},
			GraphQLFieldName: "user",
		},
		{
			IsManyToOne:      true,
			LocalColumns:     []string{"post_id"},
			RemoteTable:      "posts",
			RemoteColumns:    []string{"id"},
			GraphQLFieldName: "post",
		},
	}

	dbSchema := &introspection.Schema{Tables: []introspection.Table{users, posts, comments}}
	r := NewResolver(nil, dbSchema, nil, 0, schemafilter.Config{}, naming.DefaultConfig())

	userNested := r.nestedCreateInputForRel(users, users.Relationships[0])
	postNested := r.nestedCreateInputForRel(posts, posts.Relationships[0])
	require.NotNil(t, userNested)
	require.NotNil(t, postNested)
	assert.NotEqual(t, userNested.Name(), postNested.Name())

	userFields := userNested.Fields()
	_, hasUserID := userFields["userId"]
	_, hasPostID := userFields["postId"]
	assert.False(t, hasUserID, "user_id should be omitted for users->comments nested input")
	assert.True(t, hasPostID, "post_id should remain for users->comments nested input")

	postFields := postNested.Fields()
	_, hasUserID = postFields["userId"]
	_, hasPostID = postFields["postId"]
	assert.True(t, hasUserID, "user_id should remain for posts->comments nested input")
	assert.False(t, hasPostID, "post_id should be omitted for posts->comments nested input")
}

func TestCreateResolver_ConnectMultipleStrategies_ValidationError(t *testing.T) {
	db, mock := newMockDB(t)
	defer db.Close()

	categories := introspection.Table{
		Name: "categories",
		Columns: []introspection.Column{
			{Name: "id", DataType: "int", IsPrimaryKey: true, IsAutoIncrement: true},
			{Name: "name", DataType: "varchar", IsNullable: false},
		},
		Indexes: []introspection.Index{
			{Name: "PRIMARY", Unique: true, Columns: []string{"id"}},
			{Name: "uq_categories_name", Unique: true, Columns: []string{"name"}},
		},
	}
	posts := introspection.Table{
		Name: "posts",
		Columns: []introspection.Column{
			{Name: "id", DataType: "int", IsPrimaryKey: true, IsAutoIncrement: true},
			{Name: "category_id", DataType: "int", IsNullable: false},
			{Name: "title", DataType: "varchar", IsNullable: false},
		},
		Relationships: []introspection.Relationship{
			{
				IsManyToOne:      true,
				LocalColumns:     []string{"category_id"},
				RemoteTable:      "categories",
				RemoteColumns:    []string{"id"},
				GraphQLFieldName: "category",
			},
		},
	}
	renamePrimaryKeyID(&categories)
	renamePrimaryKeyID(&posts)

	dbSchema := &introspection.Schema{Tables: []introspection.Table{posts, categories}}
	r := NewResolver(dbexec.NewStandardExecutor(db), dbSchema, nil, 0, schemafilter.Config{}, naming.DefaultConfig())

	mock.ExpectBegin()
	mock.ExpectRollback()
	tx, err := dbexec.NewStandardExecutor(db).BeginTx(context.Background())
	require.NoError(t, err)
	mc := NewMutationContext(tx)
	ctx := WithMutationContext(context.Background(), mc)

	insertable := columnNameSet(r.mutationInsertableColumns(posts))
	resolverFn := r.makeCreateResolver(posts, insertable, r.createSuccessType(posts, r.buildGraphQLType(posts)))
	result, err := resolverFn(graphql.ResolveParams{
		Args: map[string]interface{}{
			"input": map[string]interface{}{
				"title": "Hello",
				"categoryConnect": map[string]interface{}{
					"id":     nodeid.Encode(introspection.GraphQLTypeName(categories), int64(1)),
					"byName": map[string]interface{}{"name": "Electronics"},
				},
			},
		},
		Context: ctx,
	})
	require.NoError(t, err)

	payload, ok := result.(map[string]interface{})
	require.True(t, ok)
	assert.Equal(t, "InputValidationError", payload["__typename"])
	assert.Contains(t, payload["message"], "exactly one strategy")

	require.NoError(t, mc.Finalize())
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestCreateResolver_ConnectIDCoercesNonString_ValidationError(t *testing.T) {
	db, mock := newMockDB(t)
	defer db.Close()

	categories := introspection.Table{
		Name: "categories",
		Columns: []introspection.Column{
			{Name: "id", DataType: "int", IsPrimaryKey: true, IsAutoIncrement: true},
			{Name: "name", DataType: "varchar", IsNullable: false},
		},
	}
	posts := introspection.Table{
		Name: "posts",
		Columns: []introspection.Column{
			{Name: "id", DataType: "int", IsPrimaryKey: true, IsAutoIncrement: true},
			{Name: "category_id", DataType: "int", IsNullable: false},
			{Name: "title", DataType: "varchar", IsNullable: false},
		},
		Relationships: []introspection.Relationship{
			{
				IsManyToOne:      true,
				LocalColumns:     []string{"category_id"},
				RemoteTable:      "categories",
				RemoteColumns:    []string{"id"},
				GraphQLFieldName: "category",
			},
		},
	}
	renamePrimaryKeyID(&categories)
	renamePrimaryKeyID(&posts)

	dbSchema := &introspection.Schema{Tables: []introspection.Table{posts, categories}}
	r := NewResolver(dbexec.NewStandardExecutor(db), dbSchema, nil, 0, schemafilter.Config{}, naming.DefaultConfig())

	mock.ExpectBegin()
	mock.ExpectRollback()
	tx, err := dbexec.NewStandardExecutor(db).BeginTx(context.Background())
	require.NoError(t, err)
	mc := NewMutationContext(tx)
	ctx := WithMutationContext(context.Background(), mc)

	insertable := columnNameSet(r.mutationInsertableColumns(posts))
	resolverFn := r.makeCreateResolver(posts, insertable, r.createSuccessType(posts, r.buildGraphQLType(posts)))
	result, err := resolverFn(graphql.ResolveParams{
		Args: map[string]interface{}{
			"input": map[string]interface{}{
				"title": "Hello",
				"categoryConnect": map[string]interface{}{
					"id": int64(123),
				},
			},
		},
		Context: ctx,
	})
	require.NoError(t, err)

	payload, ok := result.(map[string]interface{})
	require.True(t, ok)
	assert.Equal(t, "InputValidationError", payload["__typename"])

	require.NoError(t, mc.Finalize())
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestCreateMutation_FilteredFKConnectDoesNotExposeCreate(t *testing.T) {
	categories := introspection.Table{
		Name: "categories",
		Columns: []introspection.Column{
			{Name: "id", DataType: "int", IsPrimaryKey: true, IsAutoIncrement: true},
			{Name: "name", DataType: "varchar", IsNullable: false},
		},
		Indexes: []introspection.Index{
			{Name: "PRIMARY", Unique: true, Columns: []string{"id"}},
			{Name: "uq_categories_name", Unique: true, Columns: []string{"name"}},
		},
	}
	posts := introspection.Table{
		Name: "posts",
		Columns: []introspection.Column{
			{Name: "id", DataType: "int", IsPrimaryKey: true, IsAutoIncrement: true},
			{Name: "category_id", DataType: "int", IsNullable: false},
			{Name: "title", DataType: "varchar", IsNullable: false},
		},
		Relationships: []introspection.Relationship{
			{
				IsManyToOne:      true,
				LocalColumns:     []string{"category_id"},
				RemoteTable:      "categories",
				RemoteColumns:    []string{"id"},
				GraphQLFieldName: "category",
			},
		},
	}
	renamePrimaryKeyID(&categories)
	renamePrimaryKeyID(&posts)

	dbSchema := &introspection.Schema{Tables: []introspection.Table{posts, categories}}
	filters := schemafilter.Config{
		DenyMutationColumns: map[string][]string{
			"posts": {"category_id"},
		},
	}
	r := NewResolver(nil, dbSchema, nil, 0, filters, naming.DefaultConfig())

	schema, err := r.BuildGraphQLSchema()
	require.NoError(t, err)
	require.NotNil(t, schema.MutationType())

	mutationFields := schema.MutationType().Fields()
	_, hasCreatePost := mutationFields["createPost"]
	_, hasCreateCategory := mutationFields["createCategory"]
	assert.False(t, hasCreatePost, "createPost should be absent when required FK is deny-mutation")
	assert.True(t, hasCreateCategory, "control mutation should remain present")
}

func TestCreateResolver_NestedUsesReferencedNonPKColumnValue(t *testing.T) {
	db, mock := newMockDB(t)
	defer db.Close()

	accounts := introspection.Table{
		Name: "accounts",
		Columns: []introspection.Column{
			{Name: "id", DataType: "int", IsPrimaryKey: true, IsAutoIncrement: true},
			{Name: "external_key", DataType: "varchar", IsNullable: false},
			{Name: "name", DataType: "varchar", IsNullable: false},
		},
		Indexes: []introspection.Index{
			{Name: "PRIMARY", Unique: true, Columns: []string{"id"}},
			{Name: "uq_accounts_external_key", Unique: true, Columns: []string{"external_key"}},
		},
	}
	sessions := introspection.Table{
		Name: "sessions",
		Columns: []introspection.Column{
			{Name: "id", DataType: "int", IsPrimaryKey: true, IsAutoIncrement: true},
			{Name: "account_external_key", DataType: "varchar", IsNullable: false},
			{Name: "token", DataType: "varchar", IsNullable: false},
		},
		Relationships: []introspection.Relationship{
			{
				IsManyToOne:      true,
				LocalColumns:     []string{"account_external_key"},
				RemoteTable:      "accounts",
				RemoteColumns:    []string{"external_key"},
				GraphQLFieldName: "account",
			},
		},
	}
	accounts.Relationships = []introspection.Relationship{
		{
			IsOneToMany:      true,
			LocalColumns:     []string{"external_key"},
			RemoteTable:      "sessions",
			RemoteColumns:    []string{"account_external_key"},
			GraphQLFieldName: "sessions",
		},
	}
	renamePrimaryKeyID(&accounts)
	renamePrimaryKeyID(&sessions)

	dbSchema := &introspection.Schema{Tables: []introspection.Table{accounts, sessions}}
	r := NewResolver(dbexec.NewStandardExecutor(db), dbSchema, nil, 0, schemafilter.Config{}, naming.DefaultConfig())
	insertable := columnNameSet(r.mutationInsertableColumns(accounts))

	parentInput := map[string]interface{}{
		"externalKey": "acct_1",
		"name":        "Acme",
	}
	parentCols, parentVals, err := mapInputColumns(accounts, parentInput, insertable)
	require.NoError(t, err)
	parentInsert, err := planner.PlanInsert(accounts, parentCols, parentVals)
	require.NoError(t, err)

	field := &ast.Field{
		Name: &ast.Name{Value: "createAccount"},
		SelectionSet: &ast.SelectionSet{Selections: []ast.Selection{
			&ast.Field{Name: &ast.Name{Value: "id"}},
			&ast.Field{Name: &ast.Name{Value: "name"}},
		}},
	}
	selected := planner.SelectedColumns(accounts, field, nil)
	selected = planner.EnsureColumns(accounts, selected, []string{"external_key"})
	selectPlan, err := planner.PlanTableByPK(accounts, selected, &accounts.Columns[0], int64(1))
	require.NoError(t, err)

	rowCols := make([]string, len(selected))
	rowVals := make([]driver.Value, len(selected))
	for i, col := range selected {
		rowCols[i] = col.Name
		switch col.Name {
		case "id":
			rowVals[i] = int64(1)
		case "external_key":
			rowVals[i] = "acct_1"
		case "name":
			rowVals[i] = "Acme"
		default:
			rowVals[i] = nil
		}
	}
	selectRows := sqlmock.NewRows(rowCols).AddRow(rowVals...)

	childInsert, err := planner.PlanInsert(sessions, []string{"token", "account_external_key"}, []interface{}{"tok_1", "acct_1"})
	require.NoError(t, err)

	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta(parentInsert.SQL)).
		WithArgs(toDriverValues(parentInsert.Args)...).
		WillReturnResult(sqlmock.NewResult(1, 1))
	expectQuery(t, mock, selectPlan.SQL, selectPlan.Args, selectRows)
	mock.ExpectExec(regexp.QuoteMeta(childInsert.SQL)).
		WithArgs(toDriverValues(childInsert.Args)...).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	tx, err := dbexec.NewStandardExecutor(db).BeginTx(context.Background())
	require.NoError(t, err)
	mc := NewMutationContext(tx)
	ctx := WithMutationContext(context.Background(), mc)

	resolverFn := r.makeCreateResolver(accounts, insertable, r.createSuccessType(accounts, r.buildGraphQLType(accounts)))
	result, err := resolverFn(graphql.ResolveParams{
		Args: map[string]interface{}{
			"input": map[string]interface{}{
				"externalKey": "acct_1",
				"name":        "Acme",
				"sessionsCreate": []interface{}{
					map[string]interface{}{"token": "tok_1"},
				},
			},
		},
		Context: ctx,
		Info: graphql.ResolveInfo{
			FieldASTs: []*ast.Field{field},
		},
	})
	require.NoError(t, err)

	payload, ok := result.(map[string]interface{})
	require.True(t, ok)
	account, ok := payload["account"].(map[string]interface{})
	require.True(t, ok)
	assert.EqualValues(t, 1, account["databaseId"])
	assert.Equal(t, "Acme", account["name"])

	require.NoError(t, mc.Finalize())
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestCreateResolver_PureM2MConnect_InsertsJunctionRows(t *testing.T) {
	db, mock := newMockDB(t)
	defer db.Close()

	users := introspection.Table{
		Name: "users",
		Columns: []introspection.Column{
			{Name: "id", DataType: "int", IsPrimaryKey: true, IsAutoIncrement: true},
			{Name: "username", DataType: "varchar", IsNullable: false},
		},
	}
	groups := introspection.Table{
		Name: "groups",
		Columns: []introspection.Column{
			{Name: "id", DataType: "int", IsPrimaryKey: true, IsAutoIncrement: true},
			{Name: "name", DataType: "varchar", IsNullable: false},
		},
		Indexes: []introspection.Index{
			{Name: "PRIMARY", Unique: true, Columns: []string{"id"}},
			{Name: "uq_groups_name", Unique: true, Columns: []string{"name"}},
		},
	}
	userGroups := introspection.Table{
		Name: "user_groups",
		Columns: []introspection.Column{
			{Name: "user_id", DataType: "int", IsPrimaryKey: true},
			{Name: "group_id", DataType: "int", IsPrimaryKey: true},
		},
	}
	users.Relationships = []introspection.Relationship{
		{
			IsManyToMany:            true,
			LocalColumns:            []string{"id"},
			RemoteTable:             "groups",
			RemoteColumns:           []string{"id"},
			JunctionTable:           "user_groups",
			JunctionLocalFKColumns:  []string{"user_id"},
			JunctionRemoteFKColumns: []string{"group_id"},
			GraphQLFieldName:        "groups",
		},
	}
	renamePrimaryKeyID(&users)
	renamePrimaryKeyID(&groups)

	dbSchema := &introspection.Schema{Tables: []introspection.Table{users, groups, userGroups}}
	r := NewResolver(dbexec.NewStandardExecutor(db), dbSchema, nil, 0, schemafilter.Config{}, naming.DefaultConfig())
	insertable := columnNameSet(r.mutationInsertableColumns(users))

	parentInput := map[string]interface{}{"username": "alice"}
	parentCols, parentVals, err := mapInputColumns(users, parentInput, insertable)
	require.NoError(t, err)
	parentInsert, err := planner.PlanInsert(users, parentCols, parentVals)
	require.NoError(t, err)

	field := &ast.Field{
		Name: &ast.Name{Value: "createUser"},
		SelectionSet: &ast.SelectionSet{Selections: []ast.Selection{
			&ast.Field{Name: &ast.Name{Value: "id"}},
			&ast.Field{Name: &ast.Name{Value: "username"}},
		}},
	}
	selected := planner.SelectedColumns(users, field, nil)
	selected = planner.EnsureColumns(users, selected, []string{"id"})
	selectPlan, err := planner.PlanTableByPK(users, selected, &users.Columns[0], int64(1))
	require.NoError(t, err)

	selectRows := sqlmock.NewRows([]string{"id", "username"}).AddRow(1, "alice")
	groupLookup, err := planner.PlanUniqueKeyLookup(groups, groups.Columns, groups.Indexes[1], map[string]interface{}{"name": "admins"})
	require.NoError(t, err)
	groupRows := sqlmock.NewRows([]string{"id", "name"}).AddRow(10, "admins")
	junctionInsert, err := planner.PlanInsert(userGroups, []string{"user_id", "group_id"}, []interface{}{int64(1), int64(10)})
	require.NoError(t, err)

	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta(parentInsert.SQL)).
		WithArgs(toDriverValues(parentInsert.Args)...).
		WillReturnResult(sqlmock.NewResult(1, 1))
	expectQuery(t, mock, selectPlan.SQL, selectPlan.Args, selectRows)
	expectQuery(t, mock, groupLookup.SQL, groupLookup.Args, groupRows)
	mock.ExpectExec(regexp.QuoteMeta(junctionInsert.SQL)).
		WithArgs(toDriverValues(junctionInsert.Args)...).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	tx, err := dbexec.NewStandardExecutor(db).BeginTx(context.Background())
	require.NoError(t, err)
	mc := NewMutationContext(tx)
	ctx := WithMutationContext(context.Background(), mc)

	resolverFn := r.makeCreateResolver(users, insertable, r.createSuccessType(users, r.buildGraphQLType(users)))
	result, err := resolverFn(graphql.ResolveParams{
		Args: map[string]interface{}{
			"input": map[string]interface{}{
				"username": "alice",
				"groupsConnect": []interface{}{
					map[string]interface{}{
						"byName": map[string]interface{}{"name": "admins"},
					},
				},
			},
		},
		Context: ctx,
		Info: graphql.ResolveInfo{
			FieldASTs: []*ast.Field{field},
		},
	})
	require.NoError(t, err)

	payload, ok := result.(map[string]interface{})
	require.True(t, ok)
	user, ok := payload["user"].(map[string]interface{})
	require.True(t, ok)
	assert.EqualValues(t, 1, user["databaseId"])
	assert.Equal(t, "alice", user["username"])

	require.NoError(t, mc.Finalize())
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestUpdateResolver_NotFound_SuccessWithNullEntity(t *testing.T) {
	db, mock := newMockDB(t)
	defer db.Close()

	table := introspection.Table{
		Name: "users",
		Columns: []introspection.Column{
			{Name: "id", DataType: "int", IsPrimaryKey: true},
			{Name: "username", DataType: "varchar"},
		},
	}
	renamePrimaryKeyID(&table)
	dbSchema := &introspection.Schema{Tables: []introspection.Table{table}}
	r := NewResolver(dbexec.NewStandardExecutor(db), dbSchema, nil, 0, schemafilter.Config{}, naming.DefaultConfig())

	updatable := columnNameSet(r.mutationUpdatableColumns(table))
	pkCols := introspection.PrimaryKeyColumns(table)
	pkValues := map[string]interface{}{"id": int64(999)}
	setValues := map[string]interface{}{"username": "new-name"}
	plan, err := planner.PlanUpdate(table, setValues, pkValues)
	require.NoError(t, err)

	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta(plan.SQL)).
		WithArgs(toDriverValues(plan.Args)...).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectCommit()

	tx, err := dbexec.NewStandardExecutor(db).BeginTx(context.Background())
	require.NoError(t, err)
	mc := NewMutationContext(tx)
	ctx := WithMutationContext(context.Background(), mc)

	id := nodeid.Encode(introspection.GraphQLTypeName(table), 999)
	resolverFn := r.makeUpdateResolver(table, updatable, pkCols, r.updateSuccessType(table, r.buildGraphQLType(table)))
	result, err := resolverFn(graphql.ResolveParams{
		Args: map[string]interface{}{
			"id":  id,
			"set": map[string]interface{}{"username": "new-name"},
		},
		Context: ctx,
	})
	require.NoError(t, err)

	payload, ok := result.(map[string]interface{})
	require.True(t, ok)
	entityField := r.mutationEntityFieldName(table)
	_, exists := payload[entityField]
	require.True(t, exists)
	assert.Nil(t, payload[entityField])

	require.NoError(t, mc.Finalize())
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestDeleteResolver_SuccessShape(t *testing.T) {
	db, mock := newMockDB(t)
	defer db.Close()

	table := introspection.Table{
		Name: "users",
		Columns: []introspection.Column{
			{Name: "id", DataType: "int", IsPrimaryKey: true},
		},
	}
	renamePrimaryKeyID(&table)
	dbSchema := &introspection.Schema{Tables: []introspection.Table{table}}
	r := NewResolver(dbexec.NewStandardExecutor(db), dbSchema, nil, 0, schemafilter.Config{}, naming.DefaultConfig())

	pkCols := introspection.PrimaryKeyColumns(table)
	pkValues := map[string]interface{}{"id": int64(7)}
	plan, err := planner.PlanDelete(table, pkValues)
	require.NoError(t, err)

	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta(plan.SQL)).
		WithArgs(toDriverValues(plan.Args)...).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	tx, err := dbexec.NewStandardExecutor(db).BeginTx(context.Background())
	require.NoError(t, err)
	mc := NewMutationContext(tx)
	ctx := WithMutationContext(context.Background(), mc)

	id := nodeid.Encode(introspection.GraphQLTypeName(table), 7)
	resolverFn := r.makeDeleteResolver(table, pkCols, r.deleteSuccessType(table, pkCols))
	result, err := resolverFn(graphql.ResolveParams{
		Args:    map[string]interface{}{"id": id},
		Context: ctx,
	})
	require.NoError(t, err)

	payload, ok := result.(map[string]interface{})
	require.True(t, ok)
	assert.Equal(t, id, payload["id"])
	assert.EqualValues(t, 7, payload["databaseId"])

	require.NoError(t, mc.Finalize())
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestDeleteResolver_NotFound_NotFoundError(t *testing.T) {
	db, mock := newMockDB(t)
	defer db.Close()

	table := introspection.Table{
		Name: "users",
		Columns: []introspection.Column{
			{Name: "id", DataType: "int", IsPrimaryKey: true},
		},
	}
	renamePrimaryKeyID(&table)
	dbSchema := &introspection.Schema{Tables: []introspection.Table{table}}
	r := NewResolver(dbexec.NewStandardExecutor(db), dbSchema, nil, 0, schemafilter.Config{}, naming.DefaultConfig())

	pkCols := introspection.PrimaryKeyColumns(table)
	pkValues := map[string]interface{}{"id": int64(42)}
	plan, err := planner.PlanDelete(table, pkValues)
	require.NoError(t, err)

	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta(plan.SQL)).
		WithArgs(toDriverValues(plan.Args)...).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectCommit()

	tx, err := dbexec.NewStandardExecutor(db).BeginTx(context.Background())
	require.NoError(t, err)
	mc := NewMutationContext(tx)
	ctx := WithMutationContext(context.Background(), mc)

	id := nodeid.Encode(introspection.GraphQLTypeName(table), 42)
	resolverFn := r.makeDeleteResolver(table, pkCols, r.deleteSuccessType(table, pkCols))
	result, err := resolverFn(graphql.ResolveParams{
		Args:    map[string]interface{}{"id": id},
		Context: ctx,
	})
	require.NoError(t, err)

	payload, ok := result.(map[string]interface{})
	require.True(t, ok)
	assert.Equal(t, "NotFoundError", payload["__typename"])

	require.NoError(t, mc.Finalize())
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestDeleteResolver_BadNodeID_ValidationError(t *testing.T) {
	db, mock := newMockDB(t)
	defer db.Close()

	table := introspection.Table{
		Name: "users",
		Columns: []introspection.Column{
			{Name: "id", DataType: "int", IsPrimaryKey: true},
		},
	}
	renamePrimaryKeyID(&table)
	dbSchema := &introspection.Schema{Tables: []introspection.Table{table}}
	r := NewResolver(dbexec.NewStandardExecutor(db), dbSchema, nil, 0, schemafilter.Config{}, naming.DefaultConfig())

	mock.ExpectBegin()
	mock.ExpectRollback()
	tx, err := dbexec.NewStandardExecutor(db).BeginTx(context.Background())
	require.NoError(t, err)
	mc := NewMutationContext(tx)
	ctx := WithMutationContext(context.Background(), mc)

	pkCols := introspection.PrimaryKeyColumns(table)
	resolverFn := r.makeDeleteResolver(table, pkCols, r.deleteSuccessType(table, pkCols))
	result, err := resolverFn(graphql.ResolveParams{
		Args:    map[string]interface{}{"id": "not-a-valid-node-id"},
		Context: ctx,
	})
	require.NoError(t, err)

	payload, ok := result.(map[string]interface{})
	require.True(t, ok)
	assert.Equal(t, "InputValidationError", payload["__typename"])

	require.NoError(t, mc.Finalize())
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestDeleteResolver_FKRestrict_ConstraintError(t *testing.T) {
	db, mock := newMockDB(t)
	defer db.Close()

	table := introspection.Table{
		Name: "users",
		Columns: []introspection.Column{
			{Name: "id", DataType: "int", IsPrimaryKey: true},
		},
	}
	renamePrimaryKeyID(&table)
	dbSchema := &introspection.Schema{Tables: []introspection.Table{table}}
	r := NewResolver(dbexec.NewStandardExecutor(db), dbSchema, nil, 0, schemafilter.Config{}, naming.DefaultConfig())

	pkCols := introspection.PrimaryKeyColumns(table)
	pkValues := map[string]interface{}{"id": int64(12)}
	plan, err := planner.PlanDelete(table, pkValues)
	require.NoError(t, err)

	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta(plan.SQL)).
		WithArgs(toDriverValues(plan.Args)...).
		WillReturnError(&mysql.MySQLError{Number: 1451, Message: "Cannot delete or update a parent row"})
	mock.ExpectRollback()

	tx, err := dbexec.NewStandardExecutor(db).BeginTx(context.Background())
	require.NoError(t, err)
	mc := NewMutationContext(tx)
	ctx := WithMutationContext(context.Background(), mc)

	id := nodeid.Encode(introspection.GraphQLTypeName(table), 12)
	resolverFn := r.makeDeleteResolver(table, pkCols, r.deleteSuccessType(table, pkCols))
	result, err := resolverFn(graphql.ResolveParams{
		Args:    map[string]interface{}{"id": id},
		Context: ctx,
	})
	require.NoError(t, err)

	payload, ok := result.(map[string]interface{})
	require.True(t, ok)
	assert.Equal(t, "ConstraintError", payload["__typename"])

	require.NoError(t, mc.Finalize())
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestDeleteResolver_PermissionDenied_PermissionError(t *testing.T) {
	db, mock := newMockDB(t)
	defer db.Close()

	table := introspection.Table{
		Name: "users",
		Columns: []introspection.Column{
			{Name: "id", DataType: "int", IsPrimaryKey: true},
		},
	}
	renamePrimaryKeyID(&table)
	dbSchema := &introspection.Schema{Tables: []introspection.Table{table}}
	r := NewResolver(dbexec.NewStandardExecutor(db), dbSchema, nil, 0, schemafilter.Config{}, naming.DefaultConfig())

	pkCols := introspection.PrimaryKeyColumns(table)
	pkValues := map[string]interface{}{"id": int64(22)}
	plan, err := planner.PlanDelete(table, pkValues)
	require.NoError(t, err)

	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta(plan.SQL)).
		WithArgs(toDriverValues(plan.Args)...).
		WillReturnError(&mysql.MySQLError{Number: 1142, Message: "DELETE command denied"})
	mock.ExpectRollback()

	tx, err := dbexec.NewStandardExecutor(db).BeginTx(context.Background())
	require.NoError(t, err)
	mc := NewMutationContext(tx)
	ctx := WithMutationContext(context.Background(), mc)

	id := nodeid.Encode(introspection.GraphQLTypeName(table), 22)
	resolverFn := r.makeDeleteResolver(table, pkCols, r.deleteSuccessType(table, pkCols))
	result, err := resolverFn(graphql.ResolveParams{
		Args:    map[string]interface{}{"id": id},
		Context: ctx,
	})
	require.NoError(t, err)

	payload, ok := result.(map[string]interface{})
	require.True(t, ok)
	assert.Equal(t, "PermissionError", payload["__typename"])

	require.NoError(t, mc.Finalize())
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestDeleteSuccessTypeIncludesID(t *testing.T) {
	table := introspection.Table{
		Name: "users",
		Columns: []introspection.Column{
			{Name: "id", IsPrimaryKey: true},
		},
	}
	renamePrimaryKeyID(&table)
	dbSchema := &introspection.Schema{Tables: []introspection.Table{table}}
	r := NewResolver(nil, dbSchema, nil, 0, schemafilter.Config{}, naming.DefaultConfig())

	pkCols := introspection.PrimaryKeyColumns(table)
	payload := r.deleteSuccessType(table, pkCols)
	fields := payload.Fields()
	_, ok := fields["id"]
	require.True(t, ok)
	_, ok = fields["databaseId"]
	require.True(t, ok)
}

func TestMutationErrorInterface_AllTypesImplementIt(t *testing.T) {
	r := NewResolver(nil, &introspection.Schema{}, nil, 0, schemafilter.Config{}, naming.DefaultConfig())
	mutationErrorInterface := r.sharedMutationErrorInterface()
	types := []*graphql.Object{
		r.sharedValidationErrorType(),
		r.sharedConflictErrorType(),
		r.sharedConstraintErrorType(),
		r.sharedPermissionErrorType(),
		r.sharedNotFoundErrorType(),
		r.sharedInternalErrorType(),
	}
	for _, typ := range types {
		interfaces := typ.Interfaces()
		require.Len(t, interfaces, 1)
		assert.Same(t, mutationErrorInterface, interfaces[0])
	}
}

func TestErrorTypesSingleton(t *testing.T) {
	r := NewResolver(nil, &introspection.Schema{}, nil, 0, schemafilter.Config{}, naming.DefaultConfig())
	assert.Same(t, r.sharedValidationErrorType(), r.sharedValidationErrorType())
	assert.Same(t, r.sharedConflictErrorType(), r.sharedConflictErrorType())
	assert.Same(t, r.sharedConstraintErrorType(), r.sharedConstraintErrorType())
	assert.Same(t, r.sharedPermissionErrorType(), r.sharedPermissionErrorType())
	assert.Same(t, r.sharedNotFoundErrorType(), r.sharedNotFoundErrorType())
	assert.Same(t, r.sharedInternalErrorType(), r.sharedInternalErrorType())
}

func TestCreateResultUnion_TypeNameAndMembers(t *testing.T) {
	table := introspection.Table{
		Name: "users",
		Columns: []introspection.Column{
			{Name: "id", IsPrimaryKey: true},
			{Name: "username"},
		},
	}
	renamePrimaryKeyID(&table)
	r := NewResolver(nil, &introspection.Schema{Tables: []introspection.Table{table}}, nil, 0, schemafilter.Config{}, naming.DefaultConfig())
	success := r.createSuccessType(table, r.buildGraphQLType(table))
	union := r.createResultUnion(table, success)

	assert.Equal(t, "CreateUserResult", union.Name())
	types := union.Types()
	require.Len(t, types, 6)
	names := make([]string, 0, len(types))
	for _, typ := range types {
		names = append(names, typ.Name())
	}
	assert.Contains(t, names, "CreateUserSuccess")
	assert.Contains(t, names, "InputValidationError")
	assert.Contains(t, names, "ConflictError")
	assert.Contains(t, names, "ConstraintError")
	assert.Contains(t, names, "PermissionError")
	assert.Contains(t, names, "InternalError")
}

func TestDeleteResultUnion_Members(t *testing.T) {
	table := introspection.Table{
		Name: "users",
		Columns: []introspection.Column{
			{Name: "id", IsPrimaryKey: true},
		},
	}
	renamePrimaryKeyID(&table)
	r := NewResolver(nil, &introspection.Schema{Tables: []introspection.Table{table}}, nil, 0, schemafilter.Config{}, naming.DefaultConfig())
	success := r.deleteSuccessType(table, introspection.PrimaryKeyColumns(table))
	union := r.deleteResultUnion(table, success)

	types := union.Types()
	require.Len(t, types, 6)
	names := make([]string, 0, len(types))
	for _, typ := range types {
		names = append(names, typ.Name())
	}
	assert.Contains(t, names, "DeleteUserSuccess")
	assert.Contains(t, names, "InputValidationError")
	assert.Contains(t, names, "NotFoundError")
	assert.Contains(t, names, "ConstraintError")
	assert.Contains(t, names, "PermissionError")
	assert.Contains(t, names, "InternalError")
}

func TestResolveType_AllErrorTypesRoute(t *testing.T) {
	table := introspection.Table{
		Name: "users",
		Columns: []introspection.Column{
			{Name: "id", IsPrimaryKey: true},
		},
	}
	renamePrimaryKeyID(&table)
	r := NewResolver(nil, &introspection.Schema{Tables: []introspection.Table{table}}, nil, 0, schemafilter.Config{}, naming.DefaultConfig())
	success := r.createSuccessType(table, r.buildGraphQLType(table))
	union := r.createResultUnion(table, success)

	cases := map[string]string{
		"InputValidationError": "InputValidationError",
		"ConflictError":        "ConflictError",
		"ConstraintError":      "ConstraintError",
		"PermissionError":      "PermissionError",
		"NotFoundError":        "NotFoundError",
		"InternalError":        "InternalError",
	}
	for typename, want := range cases {
		got := union.ResolveType(graphql.ResolveTypeParams{
			Value: map[string]interface{}{"__typename": typename},
		})
		require.NotNil(t, got)
		assert.Equal(t, want, got.Name())
	}
}

func TestBuildGraphQLSchema_MutationFieldTypes(t *testing.T) {
	table := introspection.Table{
		Name: "users",
		Columns: []introspection.Column{
			{Name: "id", DataType: "int", IsPrimaryKey: true, IsAutoIncrement: true},
			{Name: "username", DataType: "varchar"},
		},
	}
	renamePrimaryKeyID(&table)
	r := NewResolver(nil, &introspection.Schema{Tables: []introspection.Table{table}}, nil, 0, schemafilter.Config{}, naming.DefaultConfig())

	schema, err := r.BuildGraphQLSchema()
	require.NoError(t, err)
	mutationType := schema.MutationType()
	require.NotNil(t, mutationType)

	for _, fieldName := range []string{"createUser", "updateUser", "deleteUser"} {
		field, ok := mutationType.Fields()[fieldName]
		require.True(t, ok)
		nonNull, ok := field.Type.(*graphql.NonNull)
		require.True(t, ok, "field %s should be non-null", fieldName)
		_, ok = nonNull.OfType.(*graphql.Union)
		require.True(t, ok, "field %s should return a union", fieldName)
	}
}

func TestMutationErrToPayload_AllCodes(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want string
	}{
		{name: "invalid input", err: newMutationError("bad input", "invalid_input", 0), want: "InputValidationError"},
		{name: "unique", err: newMutationError("dup", "unique_violation", 1062), want: "ConflictError"},
		{name: "foreign key", err: newMutationError("fk", "foreign_key_violation", 1451), want: "ConstraintError"},
		{name: "not null", err: newMutationError("null", "not_null_violation", 1048), want: "ConstraintError"},
		{name: "access denied", err: newMutationError("denied", "access_denied", 1142), want: "PermissionError"},
		{name: "plain", err: errors.New("boom"), want: "InternalError"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			payload := mutationErrToPayload(tc.err)
			assert.Equal(t, tc.want, payload["__typename"])
			message, _ := payload["message"].(string)
			if tc.want == "InternalError" {
				assert.Equal(t, "internal server error", message)
			}
		})
	}
}

func TestBuildGraphQLSchema_ReservedTypeName_Fails(t *testing.T) {
	schema := &introspection.Schema{
		Tables: []introspection.Table{
			{
				Name: "constraint_error",
				Columns: []introspection.Column{
					{Name: "id", IsPrimaryKey: true},
					{Name: "message"},
				},
			},
		},
	}
	r := NewResolver(nil, schema, nil, 0, schemafilter.Config{}, naming.DefaultConfig())
	_, err := r.BuildGraphQLSchema()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "ConstraintError")
	assert.Contains(t, err.Error(), "<reserved mutation type>")
}

func TestBuildGraphQLSchema_TypeOverrideCanMatchSingleType(t *testing.T) {
	schema := &introspection.Schema{
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
	cfg := naming.DefaultConfig()
	cfg.TypeOverrides["users"] = "User"
	r := NewResolver(nil, schema, nil, 0, schemafilter.Config{}, cfg)

	_, err := r.BuildGraphQLSchema()
	require.NoError(t, err)
}

func TestDateTimeFieldType(t *testing.T) {
	table := introspection.Table{
		Name: "events",
		Columns: []introspection.Column{
			{Name: "id", IsPrimaryKey: true},
			{Name: "created_at", DataType: "datetime", IsNullable: false},
		},
	}
	dbSchema := &introspection.Schema{Tables: []introspection.Table{table}}
	r := NewResolver(nil, dbSchema, nil, 0, schemafilter.Config{}, naming.DefaultConfig())

	objType := r.buildGraphQLType(table)
	fields := objType.Fields()

	nonNull, ok := fields["createdAt"].Type.(*graphql.NonNull)
	require.True(t, ok)
	_, ok = nonNull.OfType.(*graphql.Scalar)
	require.True(t, ok)
	assert.Equal(t, "DateTime", nonNull.OfType.Name())
}

func TestDateFieldType(t *testing.T) {
	table := introspection.Table{
		Name: "events",
		Columns: []introspection.Column{
			{Name: "id", IsPrimaryKey: true},
			{Name: "event_date", DataType: "date", IsNullable: false},
		},
	}
	dbSchema := &introspection.Schema{Tables: []introspection.Table{table}}
	r := NewResolver(nil, dbSchema, nil, 0, schemafilter.Config{}, naming.DefaultConfig())

	objType := r.buildGraphQLType(table)
	fields := objType.Fields()

	nonNull, ok := fields["eventDate"].Type.(*graphql.NonNull)
	require.True(t, ok)
	_, ok = nonNull.OfType.(*graphql.Scalar)
	require.True(t, ok)
	assert.Equal(t, "Date", nonNull.OfType.Name())
}

func TestSetFieldType(t *testing.T) {
	table := introspection.Table{
		Name: "products",
		Columns: []introspection.Column{
			{Name: "id", IsPrimaryKey: true},
			{Name: "tags", DataType: "set", EnumValues: []string{"featured", "sale", "clearance"}, IsNullable: false},
		},
	}
	dbSchema := &introspection.Schema{Tables: []introspection.Table{table}}
	r := NewResolver(nil, dbSchema, nil, 0, schemafilter.Config{}, naming.DefaultConfig())

	objType := r.buildGraphQLType(table)
	fields := objType.Fields()

	nonNull, ok := fields["tags"].Type.(*graphql.NonNull)
	require.True(t, ok)

	listType, ok := nonNull.OfType.(*graphql.List)
	require.True(t, ok)
	itemNonNull, ok := listType.OfType.(*graphql.NonNull)
	require.True(t, ok)
	enumType, ok := itemNonNull.OfType.(*graphql.Enum)
	require.True(t, ok)
	assert.Equal(t, "ProductTags", enumType.Name())
}

func TestTimeFieldType(t *testing.T) {
	table := introspection.Table{
		Name: "events",
		Columns: []introspection.Column{
			{Name: "id", IsPrimaryKey: true},
			{Name: "event_time", DataType: "time", IsNullable: false},
		},
	}
	dbSchema := &introspection.Schema{Tables: []introspection.Table{table}}
	r := NewResolver(nil, dbSchema, nil, 0, schemafilter.Config{}, naming.DefaultConfig())

	objType := r.buildGraphQLType(table)
	fields := objType.Fields()

	nonNull, ok := fields["eventTime"].Type.(*graphql.NonNull)
	require.True(t, ok)
	_, ok = nonNull.OfType.(*graphql.Scalar)
	require.True(t, ok)
	assert.Equal(t, "Time", nonNull.OfType.Name())
}

func TestYearFieldType(t *testing.T) {
	table := introspection.Table{
		Name: "events",
		Columns: []introspection.Column{
			{Name: "id", IsPrimaryKey: true},
			{Name: "event_year", DataType: "year", IsNullable: false},
		},
	}
	dbSchema := &introspection.Schema{Tables: []introspection.Table{table}}
	r := NewResolver(nil, dbSchema, nil, 0, schemafilter.Config{}, naming.DefaultConfig())

	objType := r.buildGraphQLType(table)
	fields := objType.Fields()

	nonNull, ok := fields["eventYear"].Type.(*graphql.NonNull)
	require.True(t, ok)
	_, ok = nonNull.OfType.(*graphql.Scalar)
	require.True(t, ok)
	assert.Equal(t, "Year", nonNull.OfType.Name())
}

func TestDateTimeFilterType(t *testing.T) {
	table := introspection.Table{
		Name: "events",
		Columns: []introspection.Column{
			{Name: "created_at", DataType: "datetime", IsNullable: false},
		},
	}
	dbSchema := &introspection.Schema{Tables: []introspection.Table{table}}
	r := NewResolver(nil, dbSchema, nil, 0, schemafilter.Config{}, naming.DefaultConfig())

	filterType := r.getFilterInputType(table, table.Columns[0])
	require.NotNil(t, filterType)
	fields := filterType.Fields()
	assert.NotNil(t, fields["eq"])
	assert.NotNil(t, fields["ne"])
	assert.NotNil(t, fields["lt"])
	assert.NotNil(t, fields["lte"])
	assert.NotNil(t, fields["gt"])
	assert.NotNil(t, fields["gte"])
	assert.NotNil(t, fields["in"])
	assert.NotNil(t, fields["notIn"])
	assert.NotNil(t, fields["isNull"])
	assert.Nil(t, fields["like"])
	assert.Nil(t, fields["notLike"])
}

func TestDateFilterType(t *testing.T) {
	table := introspection.Table{
		Name: "events",
		Columns: []introspection.Column{
			{Name: "event_date", DataType: "date", IsNullable: false},
		},
	}
	dbSchema := &introspection.Schema{Tables: []introspection.Table{table}}
	r := NewResolver(nil, dbSchema, nil, 0, schemafilter.Config{}, naming.DefaultConfig())

	filterType := r.getFilterInputType(table, table.Columns[0])
	require.NotNil(t, filterType)
	fields := filterType.Fields()
	assert.NotNil(t, fields["eq"])
	assert.NotNil(t, fields["ne"])
	assert.NotNil(t, fields["lt"])
	assert.NotNil(t, fields["lte"])
	assert.NotNil(t, fields["gt"])
	assert.NotNil(t, fields["gte"])
	assert.NotNil(t, fields["in"])
	assert.NotNil(t, fields["notIn"])
	assert.NotNil(t, fields["isNull"])
	assert.Nil(t, fields["like"])
	assert.Nil(t, fields["notLike"])
}

func TestTimeFilterType(t *testing.T) {
	table := introspection.Table{
		Name: "events",
		Columns: []introspection.Column{
			{Name: "event_time", DataType: "time", IsNullable: false},
		},
	}
	dbSchema := &introspection.Schema{Tables: []introspection.Table{table}}
	r := NewResolver(nil, dbSchema, nil, 0, schemafilter.Config{}, naming.DefaultConfig())

	filterType := r.getFilterInputType(table, table.Columns[0])
	require.NotNil(t, filterType)
	fields := filterType.Fields()
	assert.NotNil(t, fields["eq"])
	assert.NotNil(t, fields["ne"])
	assert.NotNil(t, fields["lt"])
	assert.NotNil(t, fields["lte"])
	assert.NotNil(t, fields["gt"])
	assert.NotNil(t, fields["gte"])
	assert.NotNil(t, fields["in"])
	assert.NotNil(t, fields["notIn"])
	assert.NotNil(t, fields["isNull"])
	assert.Nil(t, fields["like"])
	assert.Nil(t, fields["notLike"])
}

func TestYearFilterType(t *testing.T) {
	table := introspection.Table{
		Name: "events",
		Columns: []introspection.Column{
			{Name: "event_year", DataType: "year", IsNullable: false},
		},
	}
	dbSchema := &introspection.Schema{Tables: []introspection.Table{table}}
	r := NewResolver(nil, dbSchema, nil, 0, schemafilter.Config{}, naming.DefaultConfig())

	filterType := r.getFilterInputType(table, table.Columns[0])
	require.NotNil(t, filterType)
	fields := filterType.Fields()
	assert.NotNil(t, fields["eq"])
	assert.NotNil(t, fields["ne"])
	assert.NotNil(t, fields["lt"])
	assert.NotNil(t, fields["lte"])
	assert.NotNil(t, fields["gt"])
	assert.NotNil(t, fields["gte"])
	assert.NotNil(t, fields["in"])
	assert.NotNil(t, fields["notIn"])
	assert.NotNil(t, fields["isNull"])
	assert.Nil(t, fields["like"])
	assert.Nil(t, fields["notLike"])
}

func TestBytesFieldType(t *testing.T) {
	table := introspection.Table{
		Name: "files",
		Columns: []introspection.Column{
			{Name: "id", IsPrimaryKey: true},
			{Name: "payload", DataType: "blob", IsNullable: false},
		},
	}
	dbSchema := &introspection.Schema{Tables: []introspection.Table{table}}
	r := NewResolver(nil, dbSchema, nil, 0, schemafilter.Config{}, naming.DefaultConfig())

	objType := r.buildGraphQLType(table)
	fields := objType.Fields()

	nonNull, ok := fields["payload"].Type.(*graphql.NonNull)
	require.True(t, ok)
	_, ok = nonNull.OfType.(*graphql.Scalar)
	require.True(t, ok)
	assert.Equal(t, "Bytes", nonNull.OfType.Name())
}

func TestBytesFilterType(t *testing.T) {
	table := introspection.Table{
		Name: "files",
		Columns: []introspection.Column{
			{Name: "payload", DataType: "blob", IsNullable: true},
		},
	}
	dbSchema := &introspection.Schema{Tables: []introspection.Table{table}}
	r := NewResolver(nil, dbSchema, nil, 0, schemafilter.Config{}, naming.DefaultConfig())

	filterType := r.getFilterInputType(table, table.Columns[0])
	require.NotNil(t, filterType)
	fields := filterType.Fields()
	assert.NotNil(t, fields["eq"])
	assert.NotNil(t, fields["ne"])
	assert.NotNil(t, fields["in"])
	assert.NotNil(t, fields["notIn"])
	assert.NotNil(t, fields["isNull"])
	assert.Nil(t, fields["lt"])
	assert.Nil(t, fields["like"])
}

func TestUUIDFieldAndFilterType(t *testing.T) {
	table := introspection.Table{
		Name: "orders",
		Columns: []introspection.Column{
			{Name: "id", DataType: "binary", ColumnType: "binary(16)", HasOverrideType: true, OverrideType: sqltype.TypeUUID, IsPrimaryKey: true, IsNullable: false},
		},
	}
	renamePrimaryKeyID(&table)
	dbSchema := &introspection.Schema{Tables: []introspection.Table{table}}
	r := NewResolver(nil, dbSchema, nil, 0, schemafilter.Config{}, naming.DefaultConfig())

	objType := r.buildGraphQLType(table)
	fields := objType.Fields()
	nonNull, ok := fields["databaseId"].Type.(*graphql.NonNull)
	require.True(t, ok)
	scalarType, ok := nonNull.OfType.(*graphql.Scalar)
	require.True(t, ok)
	assert.Equal(t, "UUID", scalarType.Name())

	filterType := r.getFilterInputType(table, table.Columns[0])
	require.NotNil(t, filterType)
	filterFields := filterType.Fields()
	assert.NotNil(t, filterFields["eq"])
	assert.NotNil(t, filterFields["ne"])
	assert.NotNil(t, filterFields["in"])
	assert.NotNil(t, filterFields["notIn"])
	assert.NotNil(t, filterFields["isNull"])
	assert.Nil(t, filterFields["like"])
	assert.Nil(t, filterFields["lt"])
}

func TestSetFilterType(t *testing.T) {
	table := introspection.Table{
		Name: "products",
		Columns: []introspection.Column{
			{Name: "tags", DataType: "set", EnumValues: []string{"featured", "sale"}},
		},
	}
	dbSchema := &introspection.Schema{Tables: []introspection.Table{table}}
	r := NewResolver(nil, dbSchema, nil, 0, schemafilter.Config{}, naming.DefaultConfig())

	filterType := r.getFilterInputType(table, table.Columns[0])
	require.NotNil(t, filterType)
	fields := filterType.Fields()
	assert.NotNil(t, fields["has"])
	assert.NotNil(t, fields["hasAnyOf"])
	assert.NotNil(t, fields["hasAllOf"])
	assert.NotNil(t, fields["hasNoneOf"])
	assert.NotNil(t, fields["eq"])
	assert.NotNil(t, fields["ne"])
	assert.NotNil(t, fields["isNull"])
	assert.Nil(t, fields["in"])
	assert.Nil(t, fields["notIn"])

	_, ok := fields["has"].Type.(*graphql.Enum)
	require.True(t, ok)

	listHasAny, ok := fields["hasAnyOf"].Type.(*graphql.List)
	require.True(t, ok)
	_, ok = listHasAny.OfType.(*graphql.NonNull)
	require.True(t, ok)

	listEq, ok := fields["eq"].Type.(*graphql.List)
	require.True(t, ok)
	_, ok = listEq.OfType.(*graphql.NonNull)
	require.True(t, ok)
}

func TestSetFilterAppearsInWhere(t *testing.T) {
	table := introspection.Table{
		Name: "products",
		Columns: []introspection.Column{
			{Name: "id", DataType: "int", IsPrimaryKey: true, IsNullable: false},
			{Name: "tags", DataType: "set", EnumValues: []string{"featured", "sale"}},
		},
	}
	dbSchema := &introspection.Schema{Tables: []introspection.Table{table}}
	r := NewResolver(nil, dbSchema, nil, 0, schemafilter.Config{}, naming.DefaultConfig())

	whereType := r.whereInput(table)
	require.NotNil(t, whereType)
	fields := whereType.Fields()
	assert.NotNil(t, fields["tags"])
}

func TestDateScalarSerialize(t *testing.T) {
	input := time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC)
	serialized := scalars.Date().Serialize(input)
	assert.Equal(t, "2024-01-15", serialized)
}

func TestDateTimeScalarSerialize(t *testing.T) {
	input := time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC)
	serialized := graphql.DateTime.Serialize(input)
	assert.Equal(t, "2024-01-15T10:30:00Z", serialized)
}

func TestConvertValue_PreservesTime(t *testing.T) {
	now := time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)
	converted := convertValue(now)
	assert.Equal(t, now, converted)
}

func TestConvertColumnValue_BytesPreserved(t *testing.T) {
	col := introspection.Column{Name: "payload", DataType: "blob"}
	raw := []byte{0x01, 0x02, 0x03}
	converted := convertColumnValue(col, raw)
	require.IsType(t, []byte{}, converted)
	assert.Equal(t, raw, converted)
}

func TestConvertColumnValue_BooleanCoercion(t *testing.T) {
	col := introspection.Column{Name: "is_active", DataType: "tinyint", ColumnType: "tinyint(1)"}

	assert.Equal(t, false, convertColumnValue(col, int64(0)))
	assert.Equal(t, true, convertColumnValue(col, int64(1)))
	assert.Equal(t, true, convertColumnValue(col, int64(2)))
	assert.Equal(t, true, convertColumnValue(col, []byte("2")))
	assert.Equal(t, false, convertColumnValue(col, "0"))
}

func TestConvertColumnValue_EnumCoercion(t *testing.T) {
	col := introspection.Column{
		Name:       "rating",
		DataType:   "enum",
		EnumValues: []string{"thumbs_up", "thumbs_down"},
	}

	assert.Equal(t, "thumbs_up", convertColumnValue(col, int64(1)))
	assert.Equal(t, "thumbs_down", convertColumnValue(col, uint64(2)))
	assert.Equal(t, "thumbs_up", convertColumnValue(col, []byte("1")))
	assert.Equal(t, "thumbs_down", convertColumnValue(col, "thumbs_down"))
	assert.Equal(t, int64(9), convertColumnValue(col, int64(9)))
}

func TestUUIDColumnResolver_NormalizesBinaryValue(t *testing.T) {
	col := introspection.Column{
		Name:             "id",
		DataType:         "binary",
		ColumnType:       "binary(16)",
		GraphQLFieldName: "databaseId",
		HasOverrideType:  true,
		OverrideType:     sqltype.TypeUUID,
	}
	r := NewResolver(nil, &introspection.Schema{}, nil, 0, schemafilter.Config{}, naming.DefaultConfig())
	resolve := r.uuidColumnResolver(col)

	resolved, err := resolve(graphql.ResolveParams{
		Source: map[string]interface{}{
			"databaseId": []byte{0x55, 0x0e, 0x84, 0x00, 0xe2, 0x9b, 0x41, 0xd4, 0xa7, 0x16, 0x44, 0x66, 0x55, 0x44, 0x00, 0x00},
		},
	})
	require.NoError(t, err)
	assert.Equal(t, "550e8400-e29b-41d4-a716-446655440000", resolved)
}

func TestUUIDColumnResolver_NormalizesStringValue(t *testing.T) {
	col := introspection.Column{
		Name:             "uuid_text",
		DataType:         "char",
		ColumnType:       "char(36)",
		GraphQLFieldName: "uuidText",
		HasOverrideType:  true,
		OverrideType:     sqltype.TypeUUID,
	}
	r := NewResolver(nil, &introspection.Schema{}, nil, 0, schemafilter.Config{}, naming.DefaultConfig())
	resolve := r.uuidColumnResolver(col)

	resolved, err := resolve(graphql.ResolveParams{
		Source: map[string]interface{}{
			"uuidText": "550E8400-E29B-41D4-A716-446655440000",
		},
	})
	require.NoError(t, err)
	assert.Equal(t, "550e8400-e29b-41d4-a716-446655440000", resolved)
}

func TestUUIDColumnResolver_NormalizesByteStringValue(t *testing.T) {
	col := introspection.Column{
		Name:             "uuid_text",
		DataType:         "char",
		ColumnType:       "char(36)",
		GraphQLFieldName: "uuidText",
		HasOverrideType:  true,
		OverrideType:     sqltype.TypeUUID,
	}
	r := NewResolver(nil, &introspection.Schema{}, nil, 0, schemafilter.Config{}, naming.DefaultConfig())
	resolve := r.uuidColumnResolver(col)

	resolved, err := resolve(graphql.ResolveParams{
		Source: map[string]interface{}{
			"uuidText": []byte("550E8400-E29B-41D4-A716-446655440000"),
		},
	})
	require.NoError(t, err)
	assert.Equal(t, "550e8400-e29b-41d4-a716-446655440000", resolved)
}

func TestParseSetColumnValue(t *testing.T) {
	assert.Equal(t, []string{"featured", "sale"}, parseSetColumnValue("featured,sale"))
	assert.Equal(t, []string{}, parseSetColumnValue(""))
	assert.Equal(t, []string{"featured"}, parseSetColumnValue([]byte("featured")))
}

func TestMapInputColumns_SetValueNormalization(t *testing.T) {
	table := introspection.Table{
		Name: "products",
		Columns: []introspection.Column{
			{Name: "tags", DataType: "set", EnumValues: []string{"featured", "sale", "clearance"}},
		},
	}
	input := map[string]interface{}{
		"tags": []interface{}{"sale", "featured", "sale"},
	}
	allowed := map[string]bool{"tags": true}

	cols, vals, err := mapInputColumns(table, input, allowed)
	require.NoError(t, err)
	require.Equal(t, []string{"tags"}, cols)
	require.Equal(t, []interface{}{"featured,sale"}, vals)
}

func TestMapInputColumns_BytesPreserved(t *testing.T) {
	table := introspection.Table{
		Name: "files",
		Columns: []introspection.Column{
			{Name: "payload", DataType: "blob"},
		},
	}
	input := map[string]interface{}{
		"payload": []byte{0xDE, 0xAD, 0xBE, 0xEF},
	}
	allowed := map[string]bool{"payload": true}

	cols, vals, err := mapInputColumns(table, input, allowed)
	require.NoError(t, err)
	require.Equal(t, []string{"payload"}, cols)
	require.Equal(t, []interface{}{[]byte{0xDE, 0xAD, 0xBE, 0xEF}}, vals)
}

func TestMapInputColumns_UUIDBinaryNormalization(t *testing.T) {
	table := introspection.Table{
		Name: "orders",
		Columns: []introspection.Column{
			{Name: "id", DataType: "binary", ColumnType: "binary(16)", HasOverrideType: true, OverrideType: sqltype.TypeUUID},
		},
	}
	input := map[string]interface{}{
		"id": "550E8400-E29B-41D4-A716-446655440000",
	}
	allowed := map[string]bool{"id": true}

	cols, vals, err := mapInputColumns(table, input, allowed)
	require.NoError(t, err)
	require.Equal(t, []string{"id"}, cols)
	require.Len(t, vals, 1)
	require.IsType(t, []byte{}, vals[0])
	require.Len(t, vals[0].([]byte), 16)
}

func TestManyToOneResolver(t *testing.T) {
	db, mock := newMockDB(t)
	defer db.Close()

	users := introspection.Table{
		Name: "users",
		Columns: []introspection.Column{
			{Name: "id", IsPrimaryKey: true},
			{Name: "username"},
		},
	}
	posts := introspection.Table{
		Name: "posts",
		Columns: []introspection.Column{
			{Name: "id", IsPrimaryKey: true},
			{Name: "user_id"},
			{Name: "title"},
		},
	}
	renamePrimaryKeyID(&users)
	renamePrimaryKeyID(&posts)
	dbSchema := &introspection.Schema{Tables: []introspection.Table{users, posts}}
	r := NewResolver(dbexec.NewStandardExecutor(db), dbSchema, nil, 0, schemafilter.Config{}, naming.DefaultConfig())

	rel := introspection.Relationship{
		IsManyToOne:      true,
		LocalColumns:     []string{"user_id"},
		RemoteTable:      "users",
		RemoteColumns:    []string{"id"},
		GraphQLFieldName: "user",
	}
	field := &ast.Field{Name: &ast.Name{Value: "user"}}
	plan, err := planner.PlanQuery(dbSchema, field, nil, planner.WithRelationship(planner.RelationshipContext{
		RelatedTable:  users,
		RemoteColumns: []string{"id"},
		Value:         7,
		IsManyToOne:   true,
	}))
	require.NoError(t, err)

	rows := sqlmock.NewRows([]string{"id", "username"}).
		AddRow(7, []byte("alice"))
	expectQuery(t, mock, plan.Root.SQL, plan.Root.Args, rows)

	resolverFn := r.makeManyToOneResolver(posts, rel)
	result, err := resolverFn(graphql.ResolveParams{
		Source:  map[string]interface{}{"userId": 7},
		Context: context.Background(),
		Info: graphql.ResolveInfo{
			FieldASTs: []*ast.Field{field},
		},
	})
	require.NoError(t, err)

	record, ok := result.(map[string]interface{})
	require.True(t, ok)
	assert.EqualValues(t, 7, record["databaseId"])
	assert.Equal(t, "alice", record["username"])

	require.NoError(t, mock.ExpectationsWereMet())
}

func TestCollectionFieldTypesAreNonNullConnections(t *testing.T) {
	users := introspection.Table{
		Name: "users",
		Columns: []introspection.Column{
			{Name: "id", IsPrimaryKey: true},
		},
		Relationships: []introspection.Relationship{
			{
				IsOneToMany:      true,
				LocalColumns:     []string{"id"},
				RemoteTable:      "posts",
				RemoteColumns:    []string{"user_id"},
				GraphQLFieldName: "posts",
			},
			{
				IsManyToMany:            true,
				LocalColumns:            []string{"id"},
				RemoteTable:             "roles",
				RemoteColumns:           []string{"id"},
				JunctionTable:           "user_roles",
				JunctionLocalFKColumns:  []string{"user_id"},
				JunctionRemoteFKColumns: []string{"role_id"},
				GraphQLFieldName:        "roles",
			},
			{
				IsEdgeList:             true,
				LocalColumns:           []string{"id"},
				JunctionTable:          "user_roles",
				JunctionLocalFKColumns: []string{"user_id"},
				GraphQLFieldName:       "userRoles",
			},
		},
	}
	posts := introspection.Table{
		Name: "posts",
		Columns: []introspection.Column{
			{Name: "id", IsPrimaryKey: true},
			{Name: "user_id"},
		},
	}
	roles := introspection.Table{
		Name: "roles",
		Columns: []introspection.Column{
			{Name: "id", IsPrimaryKey: true},
		},
	}
	userRoles := introspection.Table{
		Name: "user_roles",
		Columns: []introspection.Column{
			{Name: "user_id", IsPrimaryKey: true},
			{Name: "role_id", IsPrimaryKey: true},
			{Name: "assigned_at"},
		},
	}

	dbSchema := &introspection.Schema{Tables: []introspection.Table{users, posts, roles, userRoles}}
	r := NewResolver(nil, dbSchema, nil, 0, schemafilter.Config{}, naming.DefaultConfig())

	schema, err := r.BuildGraphQLSchema()
	require.NoError(t, err)

	rootCollectionField := schema.QueryType().Fields()["users"]
	require.NotNil(t, rootCollectionField)
	require.True(t, hasArg(rootCollectionField, "first"), "expected users first arg")
	require.True(t, hasArg(rootCollectionField, "after"), "expected users after arg")
	require.True(t, hasArg(rootCollectionField, "last"), "expected users last arg")
	require.True(t, hasArg(rootCollectionField, "before"), "expected users before arg")
	_, ok := rootCollectionField.Type.(*graphql.NonNull)
	require.True(t, ok, "expected non-null users root field")
	_, ok = unwrapObjectType(t, rootCollectionField.Type).Fields()["nodes"]
	require.True(t, ok, "expected users root field to be a connection type")

	userType := r.buildGraphQLType(users)
	fields := userType.Fields()
	require.True(t, hasArg(fields["posts"], "last"), "expected posts relationship last arg")
	require.True(t, hasArg(fields["posts"], "before"), "expected posts relationship before arg")
	require.True(t, hasArg(fields["roles"], "last"), "expected roles relationship last arg")
	require.True(t, hasArg(fields["roles"], "before"), "expected roles relationship before arg")
	require.True(t, hasArg(fields["userRoles"], "last"), "expected userRoles relationship last arg")
	require.True(t, hasArg(fields["userRoles"], "before"), "expected userRoles relationship before arg")
	_, ok = fields["posts"].Type.(*graphql.NonNull)
	require.True(t, ok, "expected posts to be non-null")
	_, ok = unwrapObjectType(t, fields["posts"].Type).Fields()["nodes"]
	require.True(t, ok, "expected posts to be a connection type")

	_, ok = fields["roles"].Type.(*graphql.NonNull)
	require.True(t, ok, "expected roles to be non-null")
	_, ok = unwrapObjectType(t, fields["roles"].Type).Fields()["nodes"]
	require.True(t, ok, "expected roles to be a connection type")

	_, ok = fields["userRoles"].Type.(*graphql.NonNull)
	require.True(t, ok, "expected userRoles to be non-null")
	_, ok = unwrapObjectType(t, fields["userRoles"].Type).Fields()["nodes"]
	require.True(t, ok, "expected userRoles to be a connection type")
}

func TestManyToOneResolverBatch(t *testing.T) {
	db, mock := newMockDB(t)
	defer db.Close()

	users := introspection.Table{
		Name: "users",
		Columns: []introspection.Column{
			{Name: "id", IsPrimaryKey: true},
			{Name: "username"},
		},
	}
	posts := introspection.Table{
		Name: "posts",
		Columns: []introspection.Column{
			{Name: "id", IsPrimaryKey: true},
			{Name: "user_id"},
			{Name: "title"},
		},
	}
	renamePrimaryKeyID(&users)
	renamePrimaryKeyID(&posts)
	dbSchema := &introspection.Schema{Tables: []introspection.Table{users, posts}}
	r := NewResolver(dbexec.NewStandardExecutor(db), dbSchema, nil, 0, schemafilter.Config{}, naming.DefaultConfig())

	ctx := NewBatchingContext(context.Background())

	listField := &ast.Field{
		Name: &ast.Name{Value: "posts"},
		SelectionSet: &ast.SelectionSet{Selections: []ast.Selection{
			&ast.Field{
				Name: &ast.Name{Value: "nodes"},
				SelectionSet: &ast.SelectionSet{Selections: []ast.Selection{
					&ast.Field{Name: &ast.Name{Value: "id"}},
					&ast.Field{Name: &ast.Name{Value: "userId"}},
					&ast.Field{Name: &ast.Name{Value: "title"}},
				}},
			},
		}},
	}
	listArgs := map[string]interface{}{"first": 2}
	listPlan, err := planner.PlanConnection(dbSchema, posts, listField, listArgs)
	require.NoError(t, err)

	postRows := sqlmock.NewRows([]string{"id", "user_id", "title"}).
		AddRow(10, 1, "first").
		AddRow(11, 2, "second")
	expectQuery(t, mock, listPlan.Root.SQL, listPlan.Root.Args, postRows)

	listResolver := r.makeConnectionResolver(posts)
	listResult, err := listResolver(graphql.ResolveParams{
		Args:    listArgs,
		Context: ctx,
		Info: graphql.ResolveInfo{
			FieldASTs: []*ast.Field{listField},
		},
	})
	require.NoError(t, err)
	connResult, ok := listResult.(map[string]interface{})
	require.True(t, ok)
	parentRows, ok := connResult["nodes"].([]map[string]interface{})
	require.True(t, ok)
	require.Len(t, parentRows, 2)

	batchPlan, err := planner.PlanManyToOneBatch(users, nil, []string{"id"}, []planner.ParentTuple{
		{Values: []interface{}{1}},
		{Values: []interface{}{2}},
	})
	require.NoError(t, err)
	userRows := sqlmock.NewRows([]string{"id", "username", "__batch_parent_id"}).
		AddRow(1, "alice", 1).
		AddRow(2, "bob", 2)
	expectQuery(t, mock, batchPlan.SQL, batchPlan.Args, userRows)

	rel := introspection.Relationship{
		IsManyToOne:      true,
		LocalColumns:     []string{"user_id"},
		RemoteTable:      "users",
		RemoteColumns:    []string{"id"},
		GraphQLFieldName: "user",
	}
	childField := &ast.Field{Name: &ast.Name{Value: "user"}}
	childResolver := r.makeManyToOneResolver(posts, rel)

	first, err := childResolver(graphql.ResolveParams{
		Source:  parentRows[0],
		Context: ctx,
		Info: graphql.ResolveInfo{
			FieldASTs: []*ast.Field{childField},
		},
	})
	require.NoError(t, err)
	firstRow, ok := first.(map[string]interface{})
	require.True(t, ok)
	assert.EqualValues(t, 1, firstRow["databaseId"])

	second, err := childResolver(graphql.ResolveParams{
		Source:  parentRows[1],
		Context: ctx,
		Info: graphql.ResolveInfo{
			FieldASTs: []*ast.Field{childField},
		},
	})
	require.NoError(t, err)
	secondRow, ok := second.(map[string]interface{})
	require.True(t, ok)
	assert.EqualValues(t, 2, secondRow["databaseId"])

	require.NoError(t, mock.ExpectationsWereMet())
}

func TestWhereInput_SkipsViews(t *testing.T) {
	r := NewResolver(nil, &introspection.Schema{}, nil, 0, schemafilter.Config{}, naming.DefaultConfig())
	input := r.whereInput(introspection.Table{Name: "active_users", IsView: true})
	assert.Nil(t, input)
}

func TestTryBatchOneToManyConnection_NoBatchState(t *testing.T) {
	users := introspection.Table{Name: "users"}
	posts := introspection.Table{Name: "posts"}
	r := NewResolver(nil, &introspection.Schema{Tables: []introspection.Table{users, posts}}, nil, 0, schemafilter.Config{}, naming.DefaultConfig())

	rel := introspection.Relationship{
		IsOneToMany:      true,
		LocalColumns:     []string{"id"},
		RemoteTable:      "posts",
		RemoteColumns:    []string{"user_id"},
		GraphQLFieldName: "posts",
	}
	field := &ast.Field{
		Name: &ast.Name{Value: "posts"},
		SelectionSet: &ast.SelectionSet{Selections: []ast.Selection{
			&ast.Field{
				Name: &ast.Name{Value: "nodes"},
				SelectionSet: &ast.SelectionSet{Selections: []ast.Selection{
					&ast.Field{Name: &ast.Name{Value: "databaseId"}},
				}},
			},
		}},
	}

	result, ok, err := r.tryBatchOneToManyConnection(graphql.ResolveParams{
		Source:  map[string]interface{}{"databaseId": 1},
		Context: context.Background(),
		Info: graphql.ResolveInfo{
			FieldASTs: []*ast.Field{field},
		},
	}, users, rel, []interface{}{1})
	require.NoError(t, err)
	assert.False(t, ok)
	assert.Nil(t, result)
}

func TestTryBatchOneToManyConnection_InvalidMapping(t *testing.T) {
	users := introspection.Table{
		Name: "users",
		Columns: []introspection.Column{
			{Name: "id", IsPrimaryKey: true},
		},
	}
	posts := introspection.Table{
		Name: "posts",
		Columns: []introspection.Column{
			{Name: "id", IsPrimaryKey: true},
			{Name: "user_id"},
		},
	}
	renamePrimaryKeyID(&users)
	renamePrimaryKeyID(&posts)
	r := NewResolver(nil, &introspection.Schema{Tables: []introspection.Table{users, posts}}, nil, 0, schemafilter.Config{}, naming.DefaultConfig())

	ctx := NewBatchingContext(context.Background())
	state, ok := GetBatchState(ctx)
	require.True(t, ok)
	parentKey := "users|list|"
	source := map[string]interface{}{"databaseId": 1, batchParentKeyField: parentKey}
	state.setParentRows(parentKey, []map[string]interface{}{source})

	rel := introspection.Relationship{
		IsOneToMany:      true,
		RemoteTable:      "posts",
		RemoteColumns:    []string{"user_id"},
		GraphQLFieldName: "posts",
	}
	field := &ast.Field{
		Name: &ast.Name{Value: "posts"},
		SelectionSet: &ast.SelectionSet{Selections: []ast.Selection{
			&ast.Field{
				Name: &ast.Name{Value: "nodes"},
				SelectionSet: &ast.SelectionSet{Selections: []ast.Selection{
					&ast.Field{Name: &ast.Name{Value: "databaseId"}},
				}},
			},
		}},
	}

	result, ok, err := r.tryBatchOneToManyConnection(graphql.ResolveParams{
		Source:  source,
		Context: ctx,
		Info: graphql.ResolveInfo{
			FieldASTs: []*ast.Field{field},
		},
	}, users, rel, 1)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid one-to-many mapping")
	assert.True(t, ok)
	assert.Nil(t, result)
}

func TestTryBatchOneToManyConnection_CachesResults(t *testing.T) {
	db, mock := newMockDB(t)
	defer db.Close()

	users := introspection.Table{
		Name: "users",
		Columns: []introspection.Column{
			{Name: "id", IsPrimaryKey: true},
			{Name: "username"},
		},
	}
	posts := introspection.Table{
		Name: "posts",
		Columns: []introspection.Column{
			{Name: "id", IsPrimaryKey: true},
			{Name: "user_id"},
			{Name: "title"},
		},
		Indexes: []introspection.Index{
			{Name: "PRIMARY", Unique: true, Columns: []string{"id"}},
		},
	}
	renamePrimaryKeyID(&users)
	renamePrimaryKeyID(&posts)
	dbSchema := &introspection.Schema{Tables: []introspection.Table{users, posts}}
	r := NewResolver(dbexec.NewStandardExecutor(db), dbSchema, nil, 0, schemafilter.Config{}, naming.DefaultConfig())

	ctx := NewBatchingContext(context.Background())
	state, ok := GetBatchState(ctx)
	require.True(t, ok)

	parentKey := "users|list|"
	parentRows := []map[string]interface{}{
		{"databaseId": 1, batchParentKeyField: parentKey},
		{"databaseId": 2, batchParentKeyField: parentKey},
	}
	state.setParentRows(parentKey, parentRows)

	rel := introspection.Relationship{
		IsOneToMany:      true,
		LocalColumns:     []string{"id"},
		RemoteTable:      "posts",
		RemoteColumns:    []string{"user_id"},
		GraphQLFieldName: "posts",
	}
	field := &ast.Field{
		Name: &ast.Name{Value: "posts"},
		SelectionSet: &ast.SelectionSet{Selections: []ast.Selection{
			&ast.Field{
				Name: &ast.Name{Value: "nodes"},
				SelectionSet: &ast.SelectionSet{Selections: []ast.Selection{
					&ast.Field{Name: &ast.Name{Value: "databaseId"}},
					&ast.Field{Name: &ast.Name{Value: "userId"}},
					&ast.Field{Name: &ast.Name{Value: "title"}},
				}},
			},
		}},
	}

	orderBy := &planner.OrderBy{Columns: []string{"id"}, Directions: []string{"ASC"}}
	selection := planner.SelectedColumnsForConnection(posts, field, nil, orderBy)
	batchPlan, err := planner.PlanOneToManyConnectionBatch(posts, rel.RemoteColumns[0], selection, []interface{}{1, 2}, 1, orderBy, nil)
	require.NoError(t, err)
	batchRows := sqlmock.NewRows([]string{"id", "user_id", "title", "__batch_parent_id"}).
		AddRow(101, 1, "first", 1).
		AddRow(102, 1, "second", 1).
		AddRow(201, 2, "other", 2)
	expectQuery(t, mock, batchPlan.SQL, batchPlan.Args, batchRows)

	args := map[string]interface{}{"first": 1}
	firstResult, ok, err := r.tryBatchOneToManyConnection(graphql.ResolveParams{
		Source:  parentRows[0],
		Args:    args,
		Context: ctx,
		Info: graphql.ResolveInfo{
			FieldASTs: []*ast.Field{field},
		},
	}, users, rel, 1)
	require.NoError(t, err)
	require.True(t, ok)
	firstNodes, ok := firstResult["nodes"].([]map[string]interface{})
	require.True(t, ok)
	require.Len(t, firstNodes, 1)
	assert.EqualValues(t, 101, firstNodes[0]["databaseId"])
	firstPageInfo, ok := firstResult["pageInfo"].(map[string]interface{})
	require.True(t, ok)
	assert.Equal(t, true, firstPageInfo["hasNextPage"])

	secondResult, ok, err := r.tryBatchOneToManyConnection(graphql.ResolveParams{
		Source:  parentRows[1],
		Args:    args,
		Context: ctx,
		Info: graphql.ResolveInfo{
			FieldASTs: []*ast.Field{field},
		},
	}, users, rel, 2)
	require.NoError(t, err)
	require.True(t, ok)
	secondNodes, ok := secondResult["nodes"].([]map[string]interface{})
	require.True(t, ok)
	require.Len(t, secondNodes, 1)
	assert.EqualValues(t, 201, secondNodes[0]["databaseId"])

	assert.EqualValues(t, 1, state.GetCacheMisses())
	assert.EqualValues(t, 1, state.GetCacheHits())
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestTryBatchOneToManyConnection_FirstZero(t *testing.T) {
	db, mock := newMockDB(t)
	defer db.Close()

	users := introspection.Table{
		Name: "users",
		Columns: []introspection.Column{
			{Name: "id", IsPrimaryKey: true},
		},
	}
	posts := introspection.Table{
		Name: "posts",
		Columns: []introspection.Column{
			{Name: "id", IsPrimaryKey: true},
			{Name: "user_id"},
			{Name: "title"},
		},
		Indexes: []introspection.Index{
			{Name: "PRIMARY", Unique: true, Columns: []string{"id"}},
		},
	}
	renamePrimaryKeyID(&users)
	renamePrimaryKeyID(&posts)
	dbSchema := &introspection.Schema{Tables: []introspection.Table{users, posts}}
	r := NewResolver(dbexec.NewStandardExecutor(db), dbSchema, nil, 0, schemafilter.Config{}, naming.DefaultConfig())

	ctx := NewBatchingContext(context.Background())
	state, ok := GetBatchState(ctx)
	require.True(t, ok)
	parentKey := "users|list|"
	parentRows := []map[string]interface{}{
		{"databaseId": 1, batchParentKeyField: parentKey},
		{"databaseId": 2, batchParentKeyField: parentKey},
	}
	state.setParentRows(parentKey, parentRows)

	rel := introspection.Relationship{
		IsOneToMany:      true,
		LocalColumns:     []string{"id"},
		RemoteTable:      "posts",
		RemoteColumns:    []string{"user_id"},
		GraphQLFieldName: "posts",
	}
	field := &ast.Field{
		Name: &ast.Name{Value: "posts"},
		SelectionSet: &ast.SelectionSet{Selections: []ast.Selection{
			&ast.Field{
				Name: &ast.Name{Value: "nodes"},
				SelectionSet: &ast.SelectionSet{Selections: []ast.Selection{
					&ast.Field{Name: &ast.Name{Value: "databaseId"}},
				}},
			},
		}},
	}

	orderBy := &planner.OrderBy{Columns: []string{"id"}, Directions: []string{"ASC"}}
	selection := planner.SelectedColumnsForConnection(posts, field, nil, orderBy)
	batchPlan, err := planner.PlanOneToManyConnectionBatch(posts, rel.RemoteColumns[0], selection, []interface{}{1, 2}, 0, orderBy, nil)
	require.NoError(t, err)
	batchRows := sqlmock.NewRows([]string{"id", "__batch_parent_id"}).
		AddRow(101, 1).
		AddRow(201, 2)
	expectQuery(t, mock, batchPlan.SQL, batchPlan.Args, batchRows)

	result, ok, err := r.tryBatchOneToManyConnection(graphql.ResolveParams{
		Source:  parentRows[0],
		Args:    map[string]interface{}{"first": 0},
		Context: ctx,
		Info: graphql.ResolveInfo{
			FieldASTs: []*ast.Field{field},
		},
	}, users, rel, 1)
	require.NoError(t, err)
	require.True(t, ok)

	nodes, ok := result["nodes"].([]map[string]interface{})
	require.True(t, ok)
	assert.Empty(t, nodes)

	edges, ok := result["edges"].([]map[string]interface{})
	require.True(t, ok)
	assert.Empty(t, edges)

	pageInfo, ok := result["pageInfo"].(map[string]interface{})
	require.True(t, ok)
	assert.Equal(t, true, pageInfo["hasNextPage"])
	assert.Equal(t, false, pageInfo["hasPreviousPage"])

	require.NoError(t, mock.ExpectationsWereMet())
}

func TestOneToManyConnectionResolver_WithAfterSkipsBatching(t *testing.T) {
	db, mock := newMockDB(t)
	defer db.Close()

	users := introspection.Table{
		Name: "users",
		Columns: []introspection.Column{
			{Name: "id", IsPrimaryKey: true},
		},
	}
	posts := introspection.Table{
		Name: "posts",
		Columns: []introspection.Column{
			{Name: "id", IsPrimaryKey: true},
			{Name: "user_id"},
			{Name: "title"},
		},
		Indexes: []introspection.Index{
			{Name: "PRIMARY", Unique: true, Columns: []string{"id"}},
		},
	}
	renamePrimaryKeyID(&users)
	renamePrimaryKeyID(&posts)
	dbSchema := &introspection.Schema{Tables: []introspection.Table{users, posts}}
	r := NewResolver(dbexec.NewStandardExecutor(db), dbSchema, nil, 0, schemafilter.Config{}, naming.DefaultConfig())

	ctx := NewBatchingContext(context.Background())
	state, ok := GetBatchState(ctx)
	require.True(t, ok)
	parentKey := "users|list|"
	source := map[string]interface{}{"databaseId": 1, batchParentKeyField: parentKey}
	state.setParentRows(parentKey, []map[string]interface{}{source})

	rel := introspection.Relationship{
		IsOneToMany:      true,
		LocalColumns:     []string{"id"},
		RemoteTable:      "posts",
		RemoteColumns:    []string{"user_id"},
		GraphQLFieldName: "posts",
	}
	field := &ast.Field{
		Name: &ast.Name{Value: "posts"},
		SelectionSet: &ast.SelectionSet{Selections: []ast.Selection{
			&ast.Field{
				Name: &ast.Name{Value: "nodes"},
				SelectionSet: &ast.SelectionSet{Selections: []ast.Selection{
					&ast.Field{Name: &ast.Name{Value: "databaseId"}},
				}},
			},
		}},
	}

	after := cursor.EncodeCursor("Posts", "databaseId", []string{"ASC"}, 100)
	args := map[string]interface{}{
		"first": 1,
		"after": after,
	}
	plan, err := planner.PlanOneToManyConnection(posts, rel.RemoteColumns[0], 1, field, args)
	require.NoError(t, err)
	rows := sqlmock.NewRows([]string{"id"}).AddRow(101)
	expectQuery(t, mock, plan.Root.SQL, plan.Root.Args, rows)

	resolverFn := r.makeOneToManyConnectionResolver(users, rel)
	result, err := resolverFn(graphql.ResolveParams{
		Source:  source,
		Args:    args,
		Context: ctx,
		Info: graphql.ResolveInfo{
			FieldASTs: []*ast.Field{field},
		},
	})
	require.NoError(t, err)
	conn, ok := result.(map[string]interface{})
	require.True(t, ok)
	nodes, ok := conn["nodes"].([]map[string]interface{})
	require.True(t, ok)
	require.Len(t, nodes, 1)
	assert.EqualValues(t, 101, nodes[0]["databaseId"])
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestOneToManyConnectionResolver_WithLastSkipsBatching(t *testing.T) {
	db, mock := newMockDB(t)
	defer db.Close()

	users := introspection.Table{
		Name: "users",
		Columns: []introspection.Column{
			{Name: "id", IsPrimaryKey: true},
		},
	}
	posts := introspection.Table{
		Name: "posts",
		Columns: []introspection.Column{
			{Name: "id", IsPrimaryKey: true},
			{Name: "user_id"},
			{Name: "title"},
		},
		Indexes: []introspection.Index{
			{Name: "PRIMARY", Unique: true, Columns: []string{"id"}},
		},
	}
	renamePrimaryKeyID(&users)
	renamePrimaryKeyID(&posts)
	dbSchema := &introspection.Schema{Tables: []introspection.Table{users, posts}}
	r := NewResolver(dbexec.NewStandardExecutor(db), dbSchema, nil, 0, schemafilter.Config{}, naming.DefaultConfig())

	ctx := NewBatchingContext(context.Background())
	state, ok := GetBatchState(ctx)
	require.True(t, ok)
	parentKey := "users|list|"
	source := map[string]interface{}{"databaseId": 1, batchParentKeyField: parentKey}
	state.setParentRows(parentKey, []map[string]interface{}{source})

	rel := introspection.Relationship{
		IsOneToMany:      true,
		LocalColumns:     []string{"id"},
		RemoteTable:      "posts",
		RemoteColumns:    []string{"user_id"},
		GraphQLFieldName: "posts",
	}
	field := &ast.Field{
		Name: &ast.Name{Value: "posts"},
		SelectionSet: &ast.SelectionSet{Selections: []ast.Selection{
			&ast.Field{
				Name: &ast.Name{Value: "nodes"},
				SelectionSet: &ast.SelectionSet{Selections: []ast.Selection{
					&ast.Field{Name: &ast.Name{Value: "databaseId"}},
				}},
			},
		}},
	}

	args := map[string]interface{}{
		"last": 1,
	}
	plan, err := planner.PlanOneToManyConnection(posts, rel.RemoteColumns[0], 1, field, args)
	require.NoError(t, err)
	rows := sqlmock.NewRows([]string{"id"}).AddRow(101)
	expectQuery(t, mock, plan.Root.SQL, plan.Root.Args, rows)

	resolverFn := r.makeOneToManyConnectionResolver(users, rel)
	result, err := resolverFn(graphql.ResolveParams{
		Source:  source,
		Args:    args,
		Context: ctx,
		Info: graphql.ResolveInfo{
			FieldASTs: []*ast.Field{field},
		},
	})
	require.NoError(t, err)
	conn, ok := result.(map[string]interface{})
	require.True(t, ok)
	nodes, ok := conn["nodes"].([]map[string]interface{})
	require.True(t, ok)
	require.Len(t, nodes, 1)
	assert.EqualValues(t, 101, nodes[0]["databaseId"])
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestOneToManyConnectionResolver_InvalidMapping(t *testing.T) {
	users := introspection.Table{
		Name: "users",
		Columns: []introspection.Column{
			{Name: "id", IsPrimaryKey: true},
		},
	}
	renamePrimaryKeyID(&users)
	r := NewResolver(nil, &introspection.Schema{Tables: []introspection.Table{users}}, nil, 0, schemafilter.Config{}, naming.DefaultConfig())

	rel := introspection.Relationship{
		IsOneToMany:      true,
		RemoteTable:      "posts",
		RemoteColumns:    []string{"user_id"},
		GraphQLFieldName: "posts",
	}

	resolverFn := r.makeOneToManyConnectionResolver(users, rel)
	_, err := resolverFn(graphql.ResolveParams{
		Source:  map[string]interface{}{"databaseId": 1},
		Context: context.Background(),
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid one-to-many mapping")
}

func TestOneToManyConnectionResolver_UsesMutationTx(t *testing.T) {
	users := introspection.Table{
		Name: "users",
		Columns: []introspection.Column{
			{Name: "id", IsPrimaryKey: true},
		},
	}
	posts := introspection.Table{
		Name: "posts",
		Columns: []introspection.Column{
			{Name: "id", IsPrimaryKey: true},
			{Name: "user_id"},
		},
		Indexes: []introspection.Index{
			{Name: "PRIMARY", Unique: true, Columns: []string{"id"}},
		},
	}
	renamePrimaryKeyID(&users)
	renamePrimaryKeyID(&posts)
	dbSchema := &introspection.Schema{Tables: []introspection.Table{users, posts}}

	exec := &txAwareFakeExecutor{
		txResponses: [][][]any{
			{{int64(101)}},
		},
	}
	r := NewResolver(exec, dbSchema, nil, 0, schemafilter.Config{}, naming.DefaultConfig())

	tx, err := exec.BeginTx(context.Background())
	require.NoError(t, err)
	ctx := WithMutationContext(context.Background(), NewMutationContext(tx))

	rel := introspection.Relationship{
		IsOneToMany:      true,
		LocalColumns:     []string{"id"},
		RemoteTable:      "posts",
		RemoteColumns:    []string{"user_id"},
		GraphQLFieldName: "posts",
	}
	field := &ast.Field{
		Name: &ast.Name{Value: "posts"},
		SelectionSet: &ast.SelectionSet{Selections: []ast.Selection{
			&ast.Field{
				Name: &ast.Name{Value: "nodes"},
				SelectionSet: &ast.SelectionSet{Selections: []ast.Selection{
					&ast.Field{Name: &ast.Name{Value: "databaseId"}},
				}},
			},
		}},
	}

	resolverFn := r.makeOneToManyConnectionResolver(users, rel)
	result, err := resolverFn(graphql.ResolveParams{
		Source:  map[string]interface{}{"databaseId": int64(1)},
		Args:    map[string]interface{}{"first": 10},
		Context: ctx,
		Info: graphql.ResolveInfo{
			FieldASTs: []*ast.Field{field},
		},
	})
	require.NoError(t, err)

	conn, ok := result.(map[string]interface{})
	require.True(t, ok)
	nodes, ok := conn["nodes"].([]map[string]interface{})
	require.True(t, ok)
	require.Len(t, nodes, 1)
	assert.EqualValues(t, 101, nodes[0]["databaseId"])
	assert.Equal(t, 0, exec.baseCalls)
	assert.Equal(t, 1, exec.txCalls)
}

func TestTryBatchManyToOne_NoBatchState(t *testing.T) {
	users := introspection.Table{Name: "users"}
	r := NewResolver(nil, &introspection.Schema{Tables: []introspection.Table{users}}, nil, 0, schemafilter.Config{}, naming.DefaultConfig())

	rel := introspection.Relationship{
		IsManyToOne:      true,
		LocalColumns:     []string{"user_id"},
		RemoteTable:      "users",
		RemoteColumns:    []string{"id"},
		GraphQLFieldName: "user",
	}
	field := &ast.Field{Name: &ast.Name{Value: "user"}}

	// No batching context means we should fall back to non-batched execution.
	result, ok, err := r.tryBatchManyToOne(graphql.ResolveParams{
		Source:  map[string]interface{}{"userId": 1},
		Context: context.Background(),
		Info: graphql.ResolveInfo{
			FieldASTs: []*ast.Field{field},
		},
	}, users, rel, []interface{}{1})
	require.NoError(t, err)
	assert.False(t, ok)
	assert.Nil(t, result)
}

func TestTryBatchManyToOne_CachesResults(t *testing.T) {
	db, mock := newMockDB(t)
	defer db.Close()

	users := introspection.Table{
		Name: "users",
		Columns: []introspection.Column{
			{Name: "id", IsPrimaryKey: true},
			{Name: "username"},
		},
	}
	posts := introspection.Table{
		Name: "posts",
		Columns: []introspection.Column{
			{Name: "id", IsPrimaryKey: true},
			{Name: "user_id"},
			{Name: "title"},
		},
	}
	renamePrimaryKeyID(&users)
	renamePrimaryKeyID(&posts)
	dbSchema := &introspection.Schema{Tables: []introspection.Table{users, posts}}
	r := NewResolver(dbexec.NewStandardExecutor(db), dbSchema, nil, 0, schemafilter.Config{}, naming.DefaultConfig())

	// Seed parent rows into the batch state via the root connection resolver.
	ctx := NewBatchingContext(context.Background())

	listField := &ast.Field{
		Name: &ast.Name{Value: "posts"},
		SelectionSet: &ast.SelectionSet{Selections: []ast.Selection{
			&ast.Field{
				Name: &ast.Name{Value: "nodes"},
				SelectionSet: &ast.SelectionSet{Selections: []ast.Selection{
					&ast.Field{Name: &ast.Name{Value: "id"}},
					&ast.Field{Name: &ast.Name{Value: "userId"}},
					&ast.Field{Name: &ast.Name{Value: "title"}},
				}},
			},
		}},
	}
	listArgs := map[string]interface{}{"first": 2}
	listPlan, err := planner.PlanConnection(dbSchema, posts, listField, listArgs)
	require.NoError(t, err)

	postRows := sqlmock.NewRows([]string{"id", "user_id", "title"}).
		AddRow(10, 1, "first").
		AddRow(11, 2, "second")
	expectQuery(t, mock, listPlan.Root.SQL, listPlan.Root.Args, postRows)

	listResolver := r.makeConnectionResolver(posts)
	listResult, err := listResolver(graphql.ResolveParams{
		Args:    listArgs,
		Context: ctx,
		Info: graphql.ResolveInfo{
			FieldASTs: []*ast.Field{listField},
		},
	})
	require.NoError(t, err)
	connResult, ok := listResult.(map[string]interface{})
	require.True(t, ok)
	parentRows, ok := connResult["nodes"].([]map[string]interface{})
	require.True(t, ok)
	require.Len(t, parentRows, 2)

	// Expect the batched many-to-one query to execute once.
	batchPlan, err := planner.PlanManyToOneBatch(users, nil, []string{"id"}, []planner.ParentTuple{
		{Values: []interface{}{1}},
		{Values: []interface{}{2}},
	})
	require.NoError(t, err)
	userRows := sqlmock.NewRows([]string{"id", "username", "__batch_parent_id"}).
		AddRow(1, "alice", 1).
		AddRow(2, "bob", 2)
	expectQuery(t, mock, batchPlan.SQL, batchPlan.Args, userRows)

	rel := introspection.Relationship{
		IsManyToOne:      true,
		LocalColumns:     []string{"user_id"},
		RemoteTable:      "users",
		RemoteColumns:    []string{"id"},
		GraphQLFieldName: "user",
	}
	childField := &ast.Field{
		Name: &ast.Name{Value: "user"},
		SelectionSet: &ast.SelectionSet{Selections: []ast.Selection{
			&ast.Field{Name: &ast.Name{Value: "id"}},
			&ast.Field{Name: &ast.Name{Value: "username"}},
		}},
	}

	// First child resolution should populate the cache (miss).
	first, ok, err := r.tryBatchManyToOne(graphql.ResolveParams{
		Source:  parentRows[0],
		Context: ctx,
		Info: graphql.ResolveInfo{
			FieldASTs: []*ast.Field{childField},
		},
	}, posts, rel, []interface{}{1})
	require.NoError(t, err)
	require.True(t, ok)
	assert.EqualValues(t, 1, first["databaseId"])

	// Second child resolution should hit the cached batch results.
	second, ok, err := r.tryBatchManyToOne(graphql.ResolveParams{
		Source:  parentRows[1],
		Context: ctx,
		Info: graphql.ResolveInfo{
			FieldASTs: []*ast.Field{childField},
		},
	}, posts, rel, []interface{}{2})
	require.NoError(t, err)
	require.True(t, ok)
	assert.EqualValues(t, 2, second["databaseId"])

	// Validate cache counters to ensure N+1 avoidance behavior.
	state, ok := GetBatchState(ctx)
	require.True(t, ok)
	assert.EqualValues(t, 1, state.GetCacheMisses())
	assert.EqualValues(t, 1, state.GetCacheHits())

	require.NoError(t, mock.ExpectationsWereMet())
}

func newMockDB(t *testing.T) (*sql.DB, sqlmock.Sqlmock) {
	t.Helper()

	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	require.NoError(t, err)
	return db, mock
}

func expectQuery(t *testing.T, mock sqlmock.Sqlmock, sql string, args []interface{}, rows *sqlmock.Rows) {
	t.Helper()

	query := regexp.QuoteMeta(sql)
	expectation := mock.ExpectQuery(query)
	if len(args) > 0 {
		expectation = expectation.WithArgs(toDriverValues(args)...)
	}
	expectation.WillReturnRows(rows)
}

func toDriverValues(args []interface{}) []driver.Value {
	values := make([]driver.Value, len(args))
	for i, arg := range args {
		values[i] = arg
	}
	return values
}
