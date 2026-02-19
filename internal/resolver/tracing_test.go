package resolver

import (
	"context"
	"errors"
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
	"go.opentelemetry.io/otel/codes"
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
	if got := readSpanString(span.Attributes(), "graphql.mutation.result.typename"); got != "DeleteUserSuccess" {
		t.Fatalf("graphql.mutation.result.typename = %q, want DeleteUserSuccess", got)
	}
	if got := readSpanString(span.Attributes(), "graphql.mutation.result.class"); got != "success" {
		t.Fatalf("graphql.mutation.result.class = %q, want success", got)
	}
	if got := readSpanString(span.Attributes(), "graphql.mutation.result.code"); got != "success" {
		t.Fatalf("graphql.mutation.result.code = %q, want success", got)
	}
}

func TestMutationDeleteResolver_NotFound_EmitsTypedFailureSpan(t *testing.T) {
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
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectCommit()

	tx, err := dbexec.NewStandardExecutor(db).BeginTx(context.Background())
	if err != nil {
		t.Fatalf("BeginTx failed: %v", err)
	}
	mc := NewMutationContext(tx)
	ctx := WithMutationContext(context.Background(), mc)

	id := nodeid.Encode(introspection.GraphQLTypeName(table), 7)
	resolverFn := r.makeDeleteResolver(table, pkCols, r.deleteSuccessType(table, pkCols))
	result, err := resolverFn(graphql.ResolveParams{
		Args:    map[string]interface{}{"id": id},
		Context: ctx,
		Info: graphql.ResolveInfo{
			FieldName: "deleteUser",
		},
	})
	if err != nil {
		t.Fatalf("delete resolver returned error: %v", err)
	}
	payload, ok := result.(map[string]interface{})
	if !ok {
		t.Fatalf("expected payload map result")
	}
	if got := payload["__typename"]; got != "NotFoundError" {
		t.Fatalf("payload __typename = %v, want NotFoundError", got)
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
	if got := readSpanString(span.Attributes(), "graphql.resolver.outcome"); got != "typed_failure" {
		t.Fatalf("graphql.resolver.outcome = %q, want typed_failure", got)
	}
	if got := readSpanString(span.Attributes(), "graphql.mutation.result.typename"); got != "NotFoundError" {
		t.Fatalf("graphql.mutation.result.typename = %q, want NotFoundError", got)
	}
	if got := readSpanString(span.Attributes(), "graphql.mutation.result.class"); got != "typed_failure" {
		t.Fatalf("graphql.mutation.result.class = %q, want typed_failure", got)
	}
	if got := readSpanString(span.Attributes(), "graphql.mutation.result.code"); got != "not_found" {
		t.Fatalf("graphql.mutation.result.code = %q, want not_found", got)
	}
	if got := span.Status().Code; got == codes.Error {
		t.Fatalf("expected non-error span status for typed failure, got %v", got)
	}
}

func TestMutationCreateResolver_InvalidInput_EmitsTypedFailureSpan(t *testing.T) {
	db, mock := newMockDB(t)
	defer db.Close()

	recorder, cleanup := installResolverSpanRecorder(t)
	defer cleanup()

	table := introspection.Table{
		Name: "users",
		Columns: []introspection.Column{
			{Name: "id", DataType: "int", IsPrimaryKey: true, IsAutoIncrement: true},
			{Name: "name", DataType: "varchar"},
		},
	}
	renamePrimaryKeyID(&table)
	dbSchema := &introspection.Schema{Tables: []introspection.Table{table}}
	r := NewResolver(dbexec.NewStandardExecutor(db), dbSchema, nil, 0, schemafilter.Config{}, naming.DefaultConfig())

	mock.ExpectBegin()
	mock.ExpectRollback()

	tx, err := dbexec.NewStandardExecutor(db).BeginTx(context.Background())
	if err != nil {
		t.Fatalf("BeginTx failed: %v", err)
	}
	mc := NewMutationContext(tx)
	ctx := WithMutationContext(context.Background(), mc)

	insertable := map[string]bool{"name": true}
	successType := r.createSuccessType(table, r.buildGraphQLType(table))
	resolverFn := r.makeCreateResolver(table, insertable, successType)
	result, err := resolverFn(graphql.ResolveParams{
		Args: map[string]interface{}{
			"input": "not-an-input-object",
		},
		Context: ctx,
		Info: graphql.ResolveInfo{
			FieldName: "createUser",
		},
	})
	if err != nil {
		t.Fatalf("create resolver returned error: %v", err)
	}
	payload, ok := result.(map[string]interface{})
	if !ok {
		t.Fatalf("expected payload map result")
	}
	if got := payload["__typename"]; got != "InputValidationError" {
		t.Fatalf("payload __typename = %v, want InputValidationError", got)
	}

	if err := mc.Finalize(); err != nil {
		t.Fatalf("Finalize failed: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}

	span := findEndedSpanByName(recorder.Ended(), "graphql.mutation.create")
	if span == nil {
		t.Fatalf("expected graphql.mutation.create span")
	}
	if got := readSpanString(span.Attributes(), "graphql.resolver.outcome"); got != "typed_failure" {
		t.Fatalf("graphql.resolver.outcome = %q, want typed_failure", got)
	}
	if got := readSpanString(span.Attributes(), "graphql.mutation.result.typename"); got != "InputValidationError" {
		t.Fatalf("graphql.mutation.result.typename = %q, want InputValidationError", got)
	}
	if got := readSpanString(span.Attributes(), "graphql.mutation.result.class"); got != "typed_failure" {
		t.Fatalf("graphql.mutation.result.class = %q, want typed_failure", got)
	}
	if got := readSpanString(span.Attributes(), "graphql.mutation.result.code"); got != "invalid_input" {
		t.Fatalf("graphql.mutation.result.code = %q, want invalid_input", got)
	}
	if got := span.Status().Code; got == codes.Error {
		t.Fatalf("expected non-error span status for typed failure, got %v", got)
	}
}

func TestMutationDeleteResolver_ExecutionError_EmitsErrorStatus(t *testing.T) {
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
		WillReturnError(errors.New("boom"))
	mock.ExpectRollback()

	tx, err := dbexec.NewStandardExecutor(db).BeginTx(context.Background())
	if err != nil {
		t.Fatalf("BeginTx failed: %v", err)
	}
	mc := NewMutationContext(tx)
	ctx := WithMutationContext(context.Background(), mc)

	id := nodeid.Encode(introspection.GraphQLTypeName(table), 7)
	resolverFn := r.makeDeleteResolver(table, pkCols, r.deleteSuccessType(table, pkCols))
	result, err := resolverFn(graphql.ResolveParams{
		Args:    map[string]interface{}{"id": id},
		Context: ctx,
		Info: graphql.ResolveInfo{
			FieldName: "deleteUser",
		},
	})
	if err != nil {
		t.Fatalf("delete resolver returned error: %v", err)
	}
	payload, ok := result.(map[string]interface{})
	if !ok {
		t.Fatalf("expected payload map result")
	}
	if got := payload["__typename"]; got != "InternalError" {
		t.Fatalf("payload __typename = %v, want InternalError", got)
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
	if got := readSpanString(span.Attributes(), "graphql.resolver.outcome"); got != "error" {
		t.Fatalf("graphql.resolver.outcome = %q, want error", got)
	}
	if got := readSpanString(span.Attributes(), "graphql.mutation.result.typename"); got != "InternalError" {
		t.Fatalf("graphql.mutation.result.typename = %q, want InternalError", got)
	}
	if got := readSpanString(span.Attributes(), "graphql.mutation.result.class"); got != "execution_error" {
		t.Fatalf("graphql.mutation.result.class = %q, want execution_error", got)
	}
	if got := readSpanString(span.Attributes(), "graphql.mutation.result.code"); got != "unknown" {
		t.Fatalf("graphql.mutation.result.code = %q, want unknown", got)
	}
	if got := span.Status().Code; got != codes.Error {
		t.Fatalf("expected error span status for execution error, got %v", got)
	}
}

func TestMutationErrToPayloadAndTelemetry_Mappings(t *testing.T) {
	tests := []struct {
		name         string
		err          error
		wantTypeName string
		wantClass    string
		wantCode     string
		wantOutcome  string
	}{
		{
			name:         "invalid input",
			err:          newMutationError("bad input", "invalid_input", 0),
			wantTypeName: "InputValidationError",
			wantClass:    "typed_failure",
			wantCode:     "invalid_input",
			wantOutcome:  "typed_failure",
		},
		{
			name:         "conflict",
			err:          newMutationError("duplicate", "unique_violation", 1062),
			wantTypeName: "ConflictError",
			wantClass:    "typed_failure",
			wantCode:     "unique_violation",
			wantOutcome:  "typed_failure",
		},
		{
			name:         "constraint fk",
			err:          newMutationError("fk", "foreign_key_violation", 1451),
			wantTypeName: "ConstraintError",
			wantClass:    "typed_failure",
			wantCode:     "foreign_key_violation",
			wantOutcome:  "typed_failure",
		},
		{
			name:         "constraint not null",
			err:          newMutationError("not null", "not_null_violation", 1048),
			wantTypeName: "ConstraintError",
			wantClass:    "typed_failure",
			wantCode:     "not_null_violation",
			wantOutcome:  "typed_failure",
		},
		{
			name:         "permission",
			err:          newMutationError("denied", "access_denied", 1142),
			wantTypeName: "PermissionError",
			wantClass:    "typed_failure",
			wantCode:     "access_denied",
			wantOutcome:  "typed_failure",
		},
		{
			name:         "execution error",
			err:          errors.New("boom"),
			wantTypeName: "InternalError",
			wantClass:    "execution_error",
			wantCode:     "unknown",
			wantOutcome:  "error",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			payload, telemetry := mutationErrToPayloadAndTelemetry(tt.err)
			if got := payload["__typename"]; got != tt.wantTypeName {
				t.Fatalf("__typename = %v, want %s", got, tt.wantTypeName)
			}
			if telemetry.typename != tt.wantTypeName {
				t.Fatalf("telemetry typename = %q, want %q", telemetry.typename, tt.wantTypeName)
			}
			if telemetry.class != tt.wantClass {
				t.Fatalf("telemetry class = %q, want %q", telemetry.class, tt.wantClass)
			}
			if telemetry.code != tt.wantCode {
				t.Fatalf("telemetry code = %q, want %q", telemetry.code, tt.wantCode)
			}
			if telemetry.outcome != tt.wantOutcome {
				t.Fatalf("telemetry outcome = %q, want %q", telemetry.outcome, tt.wantOutcome)
			}
		})
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
