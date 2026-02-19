package resolver

import (
	"context"
	"regexp"
	"testing"

	"tidb-graphql/internal/dbexec"
	"tidb-graphql/internal/introspection"
	"tidb-graphql/internal/naming"
	"tidb-graphql/internal/nodeid"
	"tidb-graphql/internal/planner"
	"tidb-graphql/internal/schemafilter"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/graphql-go/graphql"
	"github.com/graphql-go/graphql/language/ast"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func TestMutationDeleteResolver_EmitsTracingSpan(t *testing.T) {
	db, mock := newMockDB(t)
	defer db.Close()

	recorder, cleanup := installResolverSpanRecorder(t)
	defer cleanup()

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
	if err != nil {
		t.Fatalf("PlanDelete failed: %v", err)
	}

	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta(plan.SQL)).
		WithArgs(toDriverValues(plan.Args)...).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	tx, err := dbexec.NewStandardExecutor(db).BeginTx(context.Background())
	if err != nil {
		t.Fatalf("BeginTx failed: %v", err)
	}
	mc := NewMutationContext(tx)
	ctx := WithMutationContext(context.Background(), mc)

	id := nodeid.Encode(introspection.GraphQLTypeName(table), 7)
	resolverFn := r.makeDeleteResolver(table, pkCols, r.deleteSuccessType(table, pkCols))
	_, err = resolverFn(graphql.ResolveParams{
		Args:    map[string]interface{}{"id": id},
		Context: ctx,
		Info: graphql.ResolveInfo{
			FieldName: "deleteUser",
		},
	})
	if err != nil {
		t.Fatalf("delete resolver returned error: %v", err)
	}

	if err := mc.Finalize(); err != nil {
		t.Fatalf("Finalize failed: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}

	span := findEndedSpanByName(recorder.Ended(), "graphql.mutation.delete")
	if span == nil {
		t.Fatalf("expected graphql.mutation.delete span")
	}
	if got := readSpanString(span.Attributes(), "db.table"); got != "users" {
		t.Fatalf("db.table = %q, want users", got)
	}
	if got := readSpanString(span.Attributes(), "graphql.resolver.outcome"); got != "success" {
		t.Fatalf("graphql.resolver.outcome = %q, want success", got)
	}
}

func TestBatchManyToMany_EmitsTracingSpan(t *testing.T) {
	recorder, cleanup := installResolverSpanRecorder(t)
	defer cleanup()

	executor := &fakeExecutor{responses: [][][]any{
		{{11, 1}},
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
	if !ok {
		t.Fatalf("expected batch state")
	}

	parentKey := "users|list|"
	parentRows := []map[string]interface{}{
		{"id": 1, batchParentKeyField: parentKey},
	}
	state.setParentRows(parentKey, parentRows)

	rel := introspection.Relationship{
		IsManyToMany:            true,
		LocalColumns:            []string{"id"},
		RemoteTable:             "tags",
		RemoteColumns:           []string{"id"},
		JunctionTable:           "user_tags",
		JunctionLocalFKColumns:  []string{"user_id"},
		JunctionRemoteFKColumns: []string{"tag_id"},
		GraphQLFieldName:        "tags",
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

	_, ok, err := r.tryBatchManyToManyConnection(graphql.ResolveParams{
		Source:  parentRows[0],
		Args:    map[string]interface{}{"first": 10},
		Context: ctx,
		Info: graphql.ResolveInfo{
			FieldASTs: []*ast.Field{field},
		},
	}, users, rel, []interface{}{1})
	if err != nil {
		t.Fatalf("tryBatchManyToManyConnection failed: %v", err)
	}
	if !ok {
		t.Fatalf("expected batched result")
	}

	span := findEndedSpanByName(recorder.Ended(), "graphql.batch.connection")
	if span == nil {
		t.Fatalf("expected graphql.batch.connection span")
	}
	if got := readSpanString(span.Attributes(), "relation_type"); got != relationManyToMany {
		t.Fatalf("relation_type = %q, want %q", got, relationManyToMany)
	}
	if got := readSpanString(span.Attributes(), "graphql.resolver.outcome"); got != "success" {
		t.Fatalf("graphql.resolver.outcome = %q, want success", got)
	}
}

func installResolverSpanRecorder(t *testing.T) (*tracetest.SpanRecorder, func()) {
	t.Helper()
	recorder := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSampler(sdktrace.AlwaysSample()))
	tp.RegisterSpanProcessor(recorder)

	oldProvider := otel.GetTracerProvider()
	otel.SetTracerProvider(tp)

	return recorder, func() {
		_ = tp.Shutdown(context.Background())
		otel.SetTracerProvider(oldProvider)
	}
}

func findEndedSpanByName(spans []sdktrace.ReadOnlySpan, name string) sdktrace.ReadOnlySpan {
	for _, span := range spans {
		if span.Name() == name {
			return span
		}
	}
	return nil
}

func readSpanString(attrs []attribute.KeyValue, key string) string {
	for _, attr := range attrs {
		if string(attr.Key) == key {
			return attr.Value.AsString()
		}
	}
	return ""
}
