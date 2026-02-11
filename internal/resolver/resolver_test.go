package resolver

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"regexp"
	"testing"
	"time"

	"tidb-graphql/internal/dbexec"
	"tidb-graphql/internal/introspection"
	"tidb-graphql/internal/naming"
	"tidb-graphql/internal/nodeid"
	"tidb-graphql/internal/planner"
	"tidb-graphql/internal/scalars"
	"tidb-graphql/internal/schemafilter"

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

	batchPlan, err := planner.PlanOneToManyBatch(posts, nil, "user_id", []interface{}{1, 2}, 2, 0, nil)
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
	batchPlan, err := planner.PlanOneToManyBatch(posts, nil, "user_id", []interface{}{1, 2}, 2, 0, nil)
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
	planFirst, err := planner.PlanOneToManyBatch(posts, selection, rel.RemoteColumn, []interface{}{1, 2}, 1, 0, nil)
	require.NoError(t, err)
	firstRows := sqlmock.NewRows([]string{"id", "user_id", "title", "__batch_parent_id"}).
		AddRow(101, 1, "first", 1)
	expectQuery(t, mock, planFirst.SQL, planFirst.Args, firstRows)

	argsSecond := map[string]interface{}{"limit": 1, "offset": 1}
	planSecond, err := planner.PlanOneToManyBatch(posts, selection, rel.RemoteColumn, []interface{}{1, 2}, 1, 1, nil)
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
