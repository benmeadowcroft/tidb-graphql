package resolver

import (
	"context"
	"database/sql"
	"database/sql/driver"
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

func unwrapObjectType(t *testing.T, typ graphql.Type) *graphql.Object {
	t.Helper()
	if nonNull, ok := typ.(*graphql.NonNull); ok {
		typ = nonNull.OfType
	}
	obj, ok := typ.(*graphql.Object)
	require.True(t, ok, "expected object type, got %T", typ)
	return obj
}

func TestListResolver(t *testing.T) {
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

	field := &ast.Field{Name: &ast.Name{Value: "users"}}
	args := map[string]interface{}{"limit": 2, "offset": 1}
	plan, err := planner.PlanQuery(dbSchema, field, args)
	require.NoError(t, err)

	rows := sqlmock.NewRows([]string{"id", "username"}).
		AddRow(1, []byte("alice")).
		AddRow(2, "bob")
	expectQuery(t, mock, plan.Root.SQL, plan.Root.Args, rows)

	resolverFn := r.makeListResolver(users)
	result, err := resolverFn(graphql.ResolveParams{
		Args:    args,
		Context: context.Background(),
		Info: graphql.ResolveInfo{
			FieldASTs: []*ast.Field{field},
		},
	})
	require.NoError(t, err)

	records, ok := result.([]map[string]interface{})
	require.True(t, ok)
	require.Len(t, records, 2)
	assert.EqualValues(t, 1, records[0]["databaseId"])
	assert.Equal(t, "alice", records[0]["username"])
	assert.EqualValues(t, 2, records[1]["databaseId"])
	assert.Equal(t, "bob", records[1]["username"])

	require.NoError(t, mock.ExpectationsWereMet())
}

func TestListResolver_Empty(t *testing.T) {
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

	field := &ast.Field{Name: &ast.Name{Value: "users"}}
	args := map[string]interface{}{"limit": 2, "offset": 0}
	plan, err := planner.PlanQuery(dbSchema, field, args)
	require.NoError(t, err)

	rows := sqlmock.NewRows([]string{"id", "username"})
	expectQuery(t, mock, plan.Root.SQL, plan.Root.Args, rows)

	resolverFn := r.makeListResolver(users)
	result, err := resolverFn(graphql.ResolveParams{
		Args:    args,
		Context: context.Background(),
		Info: graphql.ResolveInfo{
			FieldASTs: []*ast.Field{field},
		},
	})
	require.NoError(t, err)

	records, ok := result.([]map[string]interface{})
	require.True(t, ok)
	assert.NotNil(t, records)
	assert.Len(t, records, 0)

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
			LocalColumn:      "id",
			RemoteTable:      "posts",
			RemoteColumn:     "user_id",
			GraphQLFieldName: "posts",
		},
		{
			IsManyToMany:     true,
			LocalColumn:      "id",
			RemoteTable:      "tags",
			RemoteColumn:     "id",
			JunctionTable:    "user_tags",
			JunctionLocalFK:  "user_id",
			JunctionRemoteFK: "tag_id",
			GraphQLFieldName: "tags",
		},
		{
			IsEdgeList:       true,
			LocalColumn:      "id",
			JunctionTable:    "user_tags",
			JunctionLocalFK:  "user_id",
			GraphQLFieldName: "userTags",
		},
	}

	dbSchema := &introspection.Schema{Tables: []introspection.Table{users, posts, tags, userTags}}
	r := NewResolver(nil, dbSchema, nil, 0, schemafilter.Config{}, naming.DefaultConfig())

	userType := r.buildGraphQLType(users)
	fields := userType.Fields()

	tagsConn, ok := fields["tagsConnection"]
	require.True(t, ok, "expected tagsConnection field")
	require.True(t, hasArg(tagsConn, "where"), "expected tagsConnection where arg")

	userTagsConn, ok := fields["userTagsConnection"]
	require.True(t, ok, "expected userTagsConnection field")
	require.True(t, hasArg(userTagsConn, "where"), "expected userTagsConnection where arg")

	postsConn, ok := fields["postsConnection"]
	require.True(t, ok, "expected postsConnection field")
	require.True(t, hasArg(postsConn, "where"), "expected postsConnection where arg")
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
			LocalColumn:      "id",
			RemoteTable:      "posts",
			RemoteColumn:     "user_id",
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

	usersConnection := queryFields["usersConnection"]
	require.NotNil(t, usersConnection, "expected usersConnection root field")
	usersConnObj := unwrapObjectType(t, usersConnection.Type)
	_, hasConnectionAggregate := usersConnObj.Fields()["aggregate"]
	assert.True(t, hasConnectionAggregate, "expected aggregate field on connection type")

	userType := r.buildGraphQLType(users)
	userFields := userType.Fields()
	_, hasRelationshipAggregate := userFields["posts_aggregate"]
	assert.False(t, hasRelationshipAggregate, "standalone relationship aggregate field should not be generated")

	postsConnection, ok := userFields["postsConnection"]
	require.True(t, ok, "expected postsConnection relationship field")
	postsConnObj := unwrapObjectType(t, postsConnection.Type)
	_, hasRelationshipConnectionAggregate := postsConnObj.Fields()["aggregate"]
	assert.True(t, hasRelationshipConnectionAggregate, "expected aggregate field on relationship connection type")
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

	resolverFn := r.makeCreateResolver(table, insertable)
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

	record, ok := result.(map[string]interface{})
	require.True(t, ok)
	assert.EqualValues(t, 1, record["databaseId"])
	assert.Equal(t, "ready", record["status"])

	require.NoError(t, mock.ExpectationsWereMet())
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
		LocalColumn:      "user_id",
		RemoteTable:      "users",
		RemoteColumn:     "id",
		GraphQLFieldName: "user",
	}
	field := &ast.Field{Name: &ast.Name{Value: "user"}}
	plan, err := planner.PlanQuery(dbSchema, field, nil, planner.WithRelationship(planner.RelationshipContext{
		RelatedTable: users,
		RemoteColumn: "id",
		Value:        7,
		IsManyToOne:  true,
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

func TestOneToManyResolver(t *testing.T) {
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
		IsOneToMany:      true,
		LocalColumn:      "id",
		RemoteTable:      "posts",
		RemoteColumn:     "user_id",
		GraphQLFieldName: "posts",
	}
	field := &ast.Field{Name: &ast.Name{Value: "posts"}}
	args := map[string]interface{}{"limit": 2, "offset": 0}
	plan, err := planner.PlanQuery(dbSchema, field, args, planner.WithRelationship(planner.RelationshipContext{
		RelatedTable: posts,
		RemoteColumn: "user_id",
		Value:        3,
		IsOneToMany:  true,
	}))
	require.NoError(t, err)

	rows := sqlmock.NewRows([]string{"id", "user_id", "title"}).
		AddRow(101, 3, "first").
		AddRow(102, 3, "second")
	expectQuery(t, mock, plan.Root.SQL, plan.Root.Args, rows)

	resolverFn := r.makeOneToManyResolver(users, rel)
	result, err := resolverFn(graphql.ResolveParams{
		Source:  map[string]interface{}{"databaseId": 3},
		Args:    args,
		Context: context.Background(),
		Info: graphql.ResolveInfo{
			FieldASTs: []*ast.Field{field},
		},
	})
	require.NoError(t, err)

	records, ok := result.([]map[string]interface{})
	require.True(t, ok)
	require.Len(t, records, 2)
	assert.EqualValues(t, 101, records[0]["databaseId"])
	assert.EqualValues(t, 3, records[0]["userId"])
	assert.Equal(t, "first", records[0]["title"])

	require.NoError(t, mock.ExpectationsWereMet())
}

func TestOneToManyResolver_Empty(t *testing.T) {
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
		IsOneToMany:      true,
		LocalColumn:      "id",
		RemoteTable:      "posts",
		RemoteColumn:     "user_id",
		GraphQLFieldName: "posts",
	}
	field := &ast.Field{Name: &ast.Name{Value: "posts"}}
	args := map[string]interface{}{"limit": 10, "offset": 0}
	plan, err := planner.PlanQuery(dbSchema, field, args, planner.WithRelationship(planner.RelationshipContext{
		RelatedTable: posts,
		RemoteColumn: "user_id",
		Value:        1,
		IsOneToMany:  true,
	}))
	require.NoError(t, err)

	rows := sqlmock.NewRows([]string{"id", "user_id", "title"})
	expectQuery(t, mock, plan.Root.SQL, plan.Root.Args, rows)

	resolverFn := r.makeOneToManyResolver(users, rel)
	result, err := resolverFn(graphql.ResolveParams{
		Source:  map[string]interface{}{"databaseId": 1},
		Args:    args,
		Context: context.Background(),
		Info: graphql.ResolveInfo{
			FieldASTs: []*ast.Field{field},
		},
	})
	require.NoError(t, err)

	records, ok := result.([]map[string]interface{})
	require.True(t, ok)
	assert.NotNil(t, records)
	assert.Len(t, records, 0)

	require.NoError(t, mock.ExpectationsWereMet())
}

func TestListFieldTypesNonNull(t *testing.T) {
	users := introspection.Table{
		Name: "users",
		Columns: []introspection.Column{
			{Name: "id", IsPrimaryKey: true},
		},
		Relationships: []introspection.Relationship{
			{
				IsOneToMany:      true,
				LocalColumn:      "id",
				RemoteTable:      "posts",
				RemoteColumn:     "user_id",
				GraphQLFieldName: "posts",
			},
			{
				IsManyToMany:     true,
				LocalColumn:      "id",
				RemoteTable:      "roles",
				RemoteColumn:     "id",
				JunctionTable:    "user_roles",
				JunctionLocalFK:  "user_id",
				JunctionRemoteFK: "role_id",
				GraphQLFieldName: "roles",
			},
			{
				IsEdgeList:       true,
				LocalColumn:      "id",
				JunctionTable:    "user_roles",
				JunctionLocalFK:  "user_id",
				GraphQLFieldName: "userRoles",
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

	rootListField := schema.QueryType().Fields()["users"]
	require.NotNil(t, rootListField)
	assertNonNullListOfNonNullObject(t, rootListField.Type)

	userType := r.buildGraphQLType(users)
	fields := userType.Fields()
	assertNonNullListOfNonNullObject(t, fields["posts"].Type)
	assertNonNullListOfNonNullObject(t, fields["roles"].Type)
	assertNonNullListOfNonNullObject(t, fields["userRoles"].Type)
}

func TestOneToManyResolverBatch(t *testing.T) {
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

	listField := &ast.Field{Name: &ast.Name{Value: "users"}}
	listArgs := map[string]interface{}{"limit": 2, "offset": 0}
	listPlan, err := planner.PlanQuery(dbSchema, listField, listArgs)
	require.NoError(t, err)

	userRows := sqlmock.NewRows([]string{"id", "username"}).
		AddRow(1, "alice").
		AddRow(2, "bob")
	expectQuery(t, mock, listPlan.Root.SQL, listPlan.Root.Args, userRows)

	listResolver := r.makeListResolver(users)
	listResult, err := listResolver(graphql.ResolveParams{
		Args:    listArgs,
		Context: ctx,
		Info: graphql.ResolveInfo{
			FieldASTs: []*ast.Field{listField},
		},
	})
	require.NoError(t, err)
	parentRows, ok := listResult.([]map[string]interface{})
	require.True(t, ok)
	require.Len(t, parentRows, 2)

	batchPlan, err := planner.PlanOneToManyBatch(posts, nil, "user_id", []interface{}{1, 2}, 2, 0, nil, nil)
	require.NoError(t, err)
	postRows := sqlmock.NewRows([]string{"id", "user_id", "title", "__batch_parent_id"}).
		AddRow(101, 1, "first", 1).
		AddRow(102, 2, "second", 2)
	expectQuery(t, mock, batchPlan.SQL, batchPlan.Args, postRows)

	rel := introspection.Relationship{
		IsOneToMany:      true,
		LocalColumn:      "id",
		RemoteTable:      "posts",
		RemoteColumn:     "user_id",
		GraphQLFieldName: "posts",
	}
	childField := &ast.Field{Name: &ast.Name{Value: "posts"}}
	childArgs := map[string]interface{}{"limit": 2, "offset": 0}
	childResolver := r.makeOneToManyResolver(users, rel)

	first, err := childResolver(graphql.ResolveParams{
		Source:  parentRows[0],
		Args:    childArgs,
		Context: ctx,
		Info: graphql.ResolveInfo{
			FieldASTs: []*ast.Field{childField},
		},
	})
	require.NoError(t, err)
	firstRows, ok := first.([]map[string]interface{})
	require.True(t, ok)
	require.Len(t, firstRows, 1)
	assert.EqualValues(t, 101, firstRows[0]["databaseId"])

	second, err := childResolver(graphql.ResolveParams{
		Source:  parentRows[1],
		Args:    childArgs,
		Context: ctx,
		Info: graphql.ResolveInfo{
			FieldASTs: []*ast.Field{childField},
		},
	})
	require.NoError(t, err)
	secondRows, ok := second.([]map[string]interface{})
	require.True(t, ok)
	require.Len(t, secondRows, 1)
	assert.EqualValues(t, 102, secondRows[0]["databaseId"])

	require.NoError(t, mock.ExpectationsWereMet())
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

	listField := &ast.Field{Name: &ast.Name{Value: "posts"}}
	listArgs := map[string]interface{}{"limit": 2, "offset": 0}
	listPlan, err := planner.PlanQuery(dbSchema, listField, listArgs)
	require.NoError(t, err)

	postRows := sqlmock.NewRows([]string{"id", "user_id", "title"}).
		AddRow(10, 1, "first").
		AddRow(11, 2, "second")
	expectQuery(t, mock, listPlan.Root.SQL, listPlan.Root.Args, postRows)

	listResolver := r.makeListResolver(posts)
	listResult, err := listResolver(graphql.ResolveParams{
		Args:    listArgs,
		Context: ctx,
		Info: graphql.ResolveInfo{
			FieldASTs: []*ast.Field{listField},
		},
	})
	require.NoError(t, err)
	parentRows, ok := listResult.([]map[string]interface{})
	require.True(t, ok)
	require.Len(t, parentRows, 2)

	batchPlan, err := planner.PlanManyToOneBatch(users, nil, "id", []interface{}{1, 2})
	require.NoError(t, err)
	userRows := sqlmock.NewRows([]string{"id", "username", "__batch_parent_id"}).
		AddRow(1, "alice", 1).
		AddRow(2, "bob", 2)
	expectQuery(t, mock, batchPlan.SQL, batchPlan.Args, userRows)

	rel := introspection.Relationship{
		IsManyToOne:      true,
		LocalColumn:      "user_id",
		RemoteTable:      "users",
		RemoteColumn:     "id",
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

func TestTryBatchOneToMany_NoBatchState(t *testing.T) {
	users := introspection.Table{Name: "users"}
	posts := introspection.Table{Name: "posts"}
	r := NewResolver(nil, &introspection.Schema{Tables: []introspection.Table{users, posts}}, nil, 0, schemafilter.Config{}, naming.DefaultConfig())

	rel := introspection.Relationship{
		IsOneToMany:      true,
		LocalColumn:      "id",
		RemoteTable:      "posts",
		RemoteColumn:     "user_id",
		GraphQLFieldName: "posts",
	}
	field := &ast.Field{Name: &ast.Name{Value: "posts"}}

	// No batching context means we should fall back to non-batched execution.
	results, ok, err := r.tryBatchOneToMany(graphql.ResolveParams{
		Source:  map[string]interface{}{"id": 1},
		Context: context.Background(),
		Info: graphql.ResolveInfo{
			FieldASTs: []*ast.Field{field},
		},
	}, users, rel, 1)
	require.NoError(t, err)
	assert.False(t, ok)
	assert.Nil(t, results)
}

func TestTryBatchOneToMany_NoPrimaryKeyFallback(t *testing.T) {
	users := introspection.Table{
		Name: "users",
		Columns: []introspection.Column{
			{Name: "id", IsPrimaryKey: true},
		},
	}
	posts := introspection.Table{
		Name: "posts",
		Columns: []introspection.Column{
			{Name: "user_id"},
			{Name: "title"},
		},
	}
	r := NewResolver(nil, &introspection.Schema{Tables: []introspection.Table{users, posts}}, nil, 0, schemafilter.Config{}, naming.DefaultConfig())

	ctx := NewBatchingContext(context.Background())
	state, ok := GetBatchState(ctx)
	require.True(t, ok)

	parentKey := "users|list|"
	parentRows := []map[string]interface{}{
		{"id": 1, batchParentKeyField: parentKey},
	}
	state.setParentRows(parentKey, parentRows)

	rel := introspection.Relationship{
		IsOneToMany:      true,
		LocalColumn:      "id",
		RemoteTable:      "posts",
		RemoteColumn:     "user_id",
		GraphQLFieldName: "posts",
	}
	field := &ast.Field{
		Name: &ast.Name{Value: "posts"},
		SelectionSet: &ast.SelectionSet{Selections: []ast.Selection{
			&ast.Field{Name: &ast.Name{Value: "userId"}},
		}},
	}

	results, ok, err := r.tryBatchOneToMany(graphql.ResolveParams{
		Source:  parentRows[0],
		Context: ctx,
		Info: graphql.ResolveInfo{
			FieldASTs: []*ast.Field{field},
		},
	}, users, rel, 1)
	require.NoError(t, err)
	assert.False(t, ok)
	assert.Nil(t, results)
}

func TestTryBatchOneToMany_CachesResults(t *testing.T) {
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

	// Seed parent rows into the batch state via the list resolver.
	ctx := NewBatchingContext(context.Background())

	listField := &ast.Field{Name: &ast.Name{Value: "users"}}
	listArgs := map[string]interface{}{"limit": 2, "offset": 0}
	listPlan, err := planner.PlanQuery(dbSchema, listField, listArgs)
	require.NoError(t, err)

	userRows := sqlmock.NewRows([]string{"id", "username"}).
		AddRow(1, "alice").
		AddRow(2, "bob")
	expectQuery(t, mock, listPlan.Root.SQL, listPlan.Root.Args, userRows)

	listResolver := r.makeListResolver(users)
	listResult, err := listResolver(graphql.ResolveParams{
		Args:    listArgs,
		Context: ctx,
		Info: graphql.ResolveInfo{
			FieldASTs: []*ast.Field{listField},
		},
	})
	require.NoError(t, err)
	parentRows, ok := listResult.([]map[string]interface{})
	require.True(t, ok)
	require.Len(t, parentRows, 2)

	// Expect the batched one-to-many query to execute once.
	batchPlan, err := planner.PlanOneToManyBatch(posts, nil, "user_id", []interface{}{1, 2}, 2, 0, nil, nil)
	require.NoError(t, err)
	postRows := sqlmock.NewRows([]string{"id", "user_id", "title", "__batch_parent_id"}).
		AddRow(101, 1, "first", 1).
		AddRow(102, 2, "second", 2)
	expectQuery(t, mock, batchPlan.SQL, batchPlan.Args, postRows)

	rel := introspection.Relationship{
		IsOneToMany:      true,
		LocalColumn:      "id",
		RemoteTable:      "posts",
		RemoteColumn:     "user_id",
		GraphQLFieldName: "posts",
	}
	childField := &ast.Field{
		Name: &ast.Name{Value: "posts"},
		SelectionSet: &ast.SelectionSet{Selections: []ast.Selection{
			&ast.Field{Name: &ast.Name{Value: "id"}},
			&ast.Field{Name: &ast.Name{Value: "userId"}},
			&ast.Field{Name: &ast.Name{Value: "title"}},
		}},
	}
	childArgs := map[string]interface{}{"limit": 2, "offset": 0}

	// First child resolution should populate the cache (miss).
	first, ok, err := r.tryBatchOneToMany(graphql.ResolveParams{
		Source:  parentRows[0],
		Args:    childArgs,
		Context: ctx,
		Info: graphql.ResolveInfo{
			FieldASTs: []*ast.Field{childField},
		},
	}, users, rel, 1)
	require.NoError(t, err)
	require.True(t, ok)
	require.Len(t, first, 1)
	assert.EqualValues(t, 101, first[0]["databaseId"])

	// Second child resolution should hit the cached batch results.
	second, ok, err := r.tryBatchOneToMany(graphql.ResolveParams{
		Source:  parentRows[1],
		Args:    childArgs,
		Context: ctx,
		Info: graphql.ResolveInfo{
			FieldASTs: []*ast.Field{childField},
		},
	}, users, rel, 2)
	require.NoError(t, err)
	require.True(t, ok)
	require.Len(t, second, 1)
	assert.EqualValues(t, 102, second[0]["databaseId"])

	// Validate cache counters to ensure N+1 avoidance behavior.
	state, ok := GetBatchState(ctx)
	require.True(t, ok)
	assert.EqualValues(t, 1, state.GetCacheMisses())
	assert.EqualValues(t, 1, state.GetCacheHits())

	require.NoError(t, mock.ExpectationsWereMet())
}

func TestTryBatchOneToMany_CacheKeyIsolation(t *testing.T) {
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
		LocalColumn:      "id",
		RemoteTable:      "posts",
		RemoteColumn:     "user_id",
		GraphQLFieldName: "posts",
	}
	childField := &ast.Field{
		Name: &ast.Name{Value: "posts"},
		SelectionSet: &ast.SelectionSet{Selections: []ast.Selection{
			&ast.Field{Name: &ast.Name{Value: "id"}},
			&ast.Field{Name: &ast.Name{Value: "userId"}},
			&ast.Field{Name: &ast.Name{Value: "title"}},
		}},
	}

	selection := planner.SelectedColumns(posts, childField, nil)

	argsFirst := map[string]interface{}{"limit": 1, "offset": 0}
	planFirst, err := planner.PlanOneToManyBatch(posts, selection, rel.RemoteColumn, []interface{}{1, 2}, 1, 0, nil, nil)
	require.NoError(t, err)
	firstRows := sqlmock.NewRows([]string{"id", "user_id", "title", "__batch_parent_id"}).
		AddRow(101, 1, "first", 1)
	expectQuery(t, mock, planFirst.SQL, planFirst.Args, firstRows)

	argsSecond := map[string]interface{}{"limit": 1, "offset": 1}
	planSecond, err := planner.PlanOneToManyBatch(posts, selection, rel.RemoteColumn, []interface{}{1, 2}, 1, 1, nil, nil)
	require.NoError(t, err)
	secondRows := sqlmock.NewRows([]string{"id", "user_id", "title", "__batch_parent_id"}).
		AddRow(201, 1, "second", 1)
	expectQuery(t, mock, planSecond.SQL, planSecond.Args, secondRows)

	first, ok, err := r.tryBatchOneToMany(graphql.ResolveParams{
		Source:  parentRows[0],
		Args:    argsFirst,
		Context: ctx,
		Info: graphql.ResolveInfo{
			FieldASTs: []*ast.Field{childField},
		},
	}, users, rel, 1)
	require.NoError(t, err)
	require.True(t, ok)
	require.Len(t, first, 1)
	assert.EqualValues(t, 101, first[0]["databaseId"])

	second, ok, err := r.tryBatchOneToMany(graphql.ResolveParams{
		Source:  parentRows[0],
		Args:    argsSecond,
		Context: ctx,
		Info: graphql.ResolveInfo{
			FieldASTs: []*ast.Field{childField},
		},
	}, users, rel, 1)
	require.NoError(t, err)
	require.True(t, ok)
	require.Len(t, second, 1)
	assert.EqualValues(t, 201, second[0]["databaseId"])

	assert.EqualValues(t, 2, state.GetCacheMisses())
	assert.EqualValues(t, 0, state.GetCacheHits())

	require.NoError(t, mock.ExpectationsWereMet())
}

func TestTryBatchOneToManyConnection_NoBatchState(t *testing.T) {
	users := introspection.Table{Name: "users"}
	posts := introspection.Table{Name: "posts"}
	r := NewResolver(nil, &introspection.Schema{Tables: []introspection.Table{users, posts}}, nil, 0, schemafilter.Config{}, naming.DefaultConfig())

	rel := introspection.Relationship{
		IsOneToMany:      true,
		LocalColumn:      "id",
		RemoteTable:      "posts",
		RemoteColumn:     "user_id",
		GraphQLFieldName: "postsConnection",
	}
	field := &ast.Field{
		Name: &ast.Name{Value: "postsConnection"},
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
	}, users, rel, 1)
	require.NoError(t, err)
	assert.False(t, ok)
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
		LocalColumn:      "id",
		RemoteTable:      "posts",
		RemoteColumn:     "user_id",
		GraphQLFieldName: "postsConnection",
	}
	field := &ast.Field{
		Name: &ast.Name{Value: "postsConnection"},
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

	orderBy := &planner.OrderBy{Columns: []string{"id"}, Direction: "ASC"}
	selection := planner.SelectedColumnsForConnection(posts, field, nil, orderBy)
	batchPlan, err := planner.PlanOneToManyConnectionBatch(posts, rel.RemoteColumn, selection, []interface{}{1, 2}, 1, orderBy, nil)
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
		LocalColumn:      "id",
		RemoteTable:      "posts",
		RemoteColumn:     "user_id",
		GraphQLFieldName: "postsConnection",
	}
	field := &ast.Field{
		Name: &ast.Name{Value: "postsConnection"},
		SelectionSet: &ast.SelectionSet{Selections: []ast.Selection{
			&ast.Field{
				Name: &ast.Name{Value: "nodes"},
				SelectionSet: &ast.SelectionSet{Selections: []ast.Selection{
					&ast.Field{Name: &ast.Name{Value: "databaseId"}},
				}},
			},
		}},
	}

	orderBy := &planner.OrderBy{Columns: []string{"id"}, Direction: "ASC"}
	selection := planner.SelectedColumnsForConnection(posts, field, nil, orderBy)
	batchPlan, err := planner.PlanOneToManyConnectionBatch(posts, rel.RemoteColumn, selection, []interface{}{1, 2}, 0, orderBy, nil)
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
		LocalColumn:      "id",
		RemoteTable:      "posts",
		RemoteColumn:     "user_id",
		GraphQLFieldName: "postsConnection",
	}
	field := &ast.Field{
		Name: &ast.Name{Value: "postsConnection"},
		SelectionSet: &ast.SelectionSet{Selections: []ast.Selection{
			&ast.Field{
				Name: &ast.Name{Value: "nodes"},
				SelectionSet: &ast.SelectionSet{Selections: []ast.Selection{
					&ast.Field{Name: &ast.Name{Value: "databaseId"}},
				}},
			},
		}},
	}

	after := cursor.EncodeCursor("Posts", "databaseId", "ASC", 100)
	args := map[string]interface{}{
		"first": 1,
		"after": after,
	}
	plan, err := planner.PlanOneToManyConnection(posts, rel.RemoteColumn, 1, field, args)
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

func TestTryBatchManyToOne_NoBatchState(t *testing.T) {
	users := introspection.Table{Name: "users"}
	r := NewResolver(nil, &introspection.Schema{Tables: []introspection.Table{users}}, nil, 0, schemafilter.Config{}, naming.DefaultConfig())

	rel := introspection.Relationship{
		IsManyToOne:      true,
		LocalColumn:      "user_id",
		RemoteTable:      "users",
		RemoteColumn:     "id",
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
	}, users, rel, 1)
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

	// Seed parent rows into the batch state via the list resolver.
	ctx := NewBatchingContext(context.Background())

	listField := &ast.Field{Name: &ast.Name{Value: "posts"}}
	listArgs := map[string]interface{}{"limit": 2, "offset": 0}
	listPlan, err := planner.PlanQuery(dbSchema, listField, listArgs)
	require.NoError(t, err)

	postRows := sqlmock.NewRows([]string{"id", "user_id", "title"}).
		AddRow(10, 1, "first").
		AddRow(11, 2, "second")
	expectQuery(t, mock, listPlan.Root.SQL, listPlan.Root.Args, postRows)

	listResolver := r.makeListResolver(posts)
	listResult, err := listResolver(graphql.ResolveParams{
		Args:    listArgs,
		Context: ctx,
		Info: graphql.ResolveInfo{
			FieldASTs: []*ast.Field{listField},
		},
	})
	require.NoError(t, err)
	parentRows, ok := listResult.([]map[string]interface{})
	require.True(t, ok)
	require.Len(t, parentRows, 2)

	// Expect the batched many-to-one query to execute once.
	batchPlan, err := planner.PlanManyToOneBatch(users, nil, "id", []interface{}{1, 2})
	require.NoError(t, err)
	userRows := sqlmock.NewRows([]string{"id", "username", "__batch_parent_id"}).
		AddRow(1, "alice", 1).
		AddRow(2, "bob", 2)
	expectQuery(t, mock, batchPlan.SQL, batchPlan.Args, userRows)

	rel := introspection.Relationship{
		IsManyToOne:      true,
		LocalColumn:      "user_id",
		RemoteTable:      "users",
		RemoteColumn:     "id",
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
	}, posts, rel, 1)
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
	}, posts, rel, 2)
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

func TestTryBatchRelationshipAggregate_SkipsWhenFiltered(t *testing.T) {
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
	dbSchema := &introspection.Schema{Tables: []introspection.Table{users, posts}}
	r := NewResolver(nil, dbSchema, nil, 0, schemafilter.Config{}, naming.DefaultConfig())

	ctx := NewBatchingContext(context.Background())
	state, ok := GetBatchState(ctx)
	require.True(t, ok)

	parentKey := "users|list|"
	parentRows := []map[string]interface{}{
		{"id": 1, batchParentKeyField: parentKey},
	}
	state.setParentRows(parentKey, parentRows)

	rel := introspection.Relationship{
		LocalColumn:      "id",
		RemoteTable:      "posts",
		RemoteColumn:     "user_id",
		GraphQLFieldName: "postsAggregate",
	}
	selection := planner.AggregateSelection{Count: true}

	limit := 10
	result, ok, err := r.tryBatchRelationshipAggregate(graphql.ResolveParams{
		Source:  parentRows[0],
		Context: ctx,
	}, users, rel, 1, selection, &planner.AggregateFilters{Limit: &limit})
	require.NoError(t, err)
	assert.False(t, ok)
	assert.Nil(t, result)

	orderBy := &planner.OrderBy{Columns: []string{"id"}, Direction: "ASC"}
	result, ok, err = r.tryBatchRelationshipAggregate(graphql.ResolveParams{
		Source:  parentRows[0],
		Context: ctx,
	}, users, rel, 1, selection, &planner.AggregateFilters{OrderBy: orderBy})
	require.NoError(t, err)
	assert.False(t, ok)
	assert.Nil(t, result)
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
