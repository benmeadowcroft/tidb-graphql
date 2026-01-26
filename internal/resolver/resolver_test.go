package resolver

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"regexp"
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

func TestListResolver(t *testing.T) {
	db, mock := newMockDB(t)
	defer db.Close()

	users := introspection.Table{
		Name: "users",
		Columns: []introspection.Column{
			{Name: "id", IsPrimaryKey: true},
			{Name: "username"},
		},
	}
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
	assert.EqualValues(t, 1, records[0]["id"])
	assert.Equal(t, "alice", records[0]["username"])
	assert.EqualValues(t, 2, records[1]["id"])
	assert.Equal(t, "bob", records[1]["username"])

	require.NoError(t, mock.ExpectationsWereMet())
}

func TestPKResolver(t *testing.T) {
	db, mock := newMockDB(t)
	defer db.Close()

	users := introspection.Table{
		Name: "users",
		Columns: []introspection.Column{
			{Name: "id", IsPrimaryKey: true},
			{Name: "username"},
		},
	}
	dbSchema := &introspection.Schema{Tables: []introspection.Table{users}}
	r := NewResolver(dbexec.NewStandardExecutor(db), dbSchema, nil, 0, schemafilter.Config{}, naming.DefaultConfig())

	field := &ast.Field{Name: &ast.Name{Value: "user_by_pk"}}
	args := map[string]interface{}{"id": 1}
	plan, err := planner.PlanQuery(dbSchema, field, args)
	require.NoError(t, err)

	rows := sqlmock.NewRows([]string{"id", "username"}).
		AddRow(1, []byte("alice"))
	expectQuery(t, mock, plan.Root.SQL, plan.Root.Args, rows)

	resolverFn := r.makeSingleRowResolver(users)
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
	assert.EqualValues(t, 1, record["id"])
	assert.Equal(t, "alice", record["username"])

	require.NoError(t, mock.ExpectationsWereMet())
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
	assert.EqualValues(t, 7, record["id"])
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
		Source:  map[string]interface{}{"id": 3},
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
	assert.EqualValues(t, 101, records[0]["id"])
	assert.EqualValues(t, 3, records[0]["userId"])
	assert.Equal(t, "first", records[0]["title"])

	require.NoError(t, mock.ExpectationsWereMet())
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
	postRows := sqlmock.NewRows([]string{"id", "user_id", "title"}).
		AddRow(101, 1, "first").
		AddRow(102, 2, "second")
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
	assert.EqualValues(t, 101, firstRows[0]["id"])

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
	assert.EqualValues(t, 102, secondRows[0]["id"])

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
	userRows := sqlmock.NewRows([]string{"id", "username"}).
		AddRow(1, "alice").
		AddRow(2, "bob")
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
	assert.EqualValues(t, 1, firstRow["id"])

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
	assert.EqualValues(t, 2, secondRow["id"])

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
	postRows := sqlmock.NewRows([]string{"id", "user_id", "title"}).
		AddRow(101, 1, "first").
		AddRow(102, 2, "second")
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
	assert.EqualValues(t, 101, first[0]["id"])

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
	assert.EqualValues(t, 102, second[0]["id"])

	// Validate cache counters to ensure N+1 avoidance behavior.
	state, ok := GetBatchState(ctx)
	require.True(t, ok)
	assert.EqualValues(t, 1, state.GetCacheMisses())
	assert.EqualValues(t, 1, state.GetCacheHits())

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
	userRows := sqlmock.NewRows([]string{"id", "username"}).
		AddRow(1, "alice").
		AddRow(2, "bob")
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
	assert.EqualValues(t, 1, first["id"])

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
	assert.EqualValues(t, 2, second["id"])

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
