package middleware

import (
	"context"
	"database/sql"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"tidb-graphql/internal/dbexec"
	"tidb-graphql/internal/gqlrequest"
)

func TestGraphQLRequestAnalysisMiddleware_PopulatesContextAndRewindsBody(t *testing.T) {
	var (
		seenAnalysis *gqlrequest.Analysis
		seenMeta     gqlrequest.ExecMeta
		seenMetaOK   bool
		bodyCopy     string
	)

	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seenAnalysis = gqlrequest.AnalysisFromContext(r.Context())
		seenMeta, seenMetaOK = gqlrequest.ExecMetaFromContext(r.Context())
		bodyBytes, _ := io.ReadAll(r.Body)
		bodyCopy = string(bodyBytes)
		w.WriteHeader(http.StatusOK)
	})

	handler := GraphQLRequestAnalysisMiddleware(nil)(next)
	req := httptest.NewRequest(http.MethodPost, "/graphql", strings.NewReader(`{"query":"mutation CreateUser { createUser(input: {}) { id } }","operationName":"CreateUser","variables":{"x":1}}`))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if seenAnalysis == nil {
		t.Fatalf("expected analysis in context")
	}
	if !seenMetaOK {
		t.Fatalf("expected exec meta in context")
	}
	if seenAnalysis.OperationType != "mutation" {
		t.Fatalf("operation type = %q, want mutation", seenAnalysis.OperationType)
	}
	if seenMeta.OperationType != "mutation" {
		t.Fatalf("exec meta operation type = %q, want mutation", seenMeta.OperationType)
	}
	if seenAnalysis.OperationHash == "" {
		t.Fatalf("expected non-empty operation hash")
	}
	if !strings.Contains(bodyCopy, `"operationName":"CreateUser"`) {
		t.Fatalf("expected rewound request body to be readable by downstream handler")
	}
}

func TestRequestAnalysisBeforeMutationTx_StartsTransactionForMutation(t *testing.T) {
	executor := &fakeQueryExecutor{tx: &fakeTx{}}
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})

	handler := GraphQLRequestAnalysisMiddleware(nil)(
		MutationTransactionMiddleware(executor)(next),
	)

	req := httptest.NewRequest(http.MethodPost, "/graphql", strings.NewReader(`{"query":"mutation M { createUser(input: {}) { id } }","operationName":"M"}`))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if executor.beginCount != 1 {
		t.Fatalf("BeginTx() count = %d, want %d", executor.beginCount, 1)
	}
}

func TestRequestAnalysisBeforeMutationTx_DoesNotStartTransactionForQuery(t *testing.T) {
	executor := &fakeQueryExecutor{tx: &fakeTx{}}
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})

	handler := GraphQLRequestAnalysisMiddleware(nil)(
		MutationTransactionMiddleware(executor)(next),
	)

	req := httptest.NewRequest(http.MethodPost, "/graphql", strings.NewReader(`{"query":"query Q { users { id } }","operationName":"Q"}`))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if executor.beginCount != 0 {
		t.Fatalf("BeginTx() count = %d, want %d", executor.beginCount, 0)
	}
}

type fakeQueryExecutor struct {
	tx         dbexec.TxExecutor
	beginCount int
}

func (f *fakeQueryExecutor) QueryContext(ctx context.Context, query string, args ...any) (dbexec.Rows, error) {
	return nil, nil
}

func (f *fakeQueryExecutor) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	return nil, nil
}

func (f *fakeQueryExecutor) BeginTx(ctx context.Context) (dbexec.TxExecutor, error) {
	f.beginCount++
	return f.tx, nil
}

type fakeTx struct{}

func (f *fakeTx) QueryContext(ctx context.Context, query string, args ...any) (dbexec.Rows, error) {
	return nil, nil
}

func (f *fakeTx) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	return nil, nil
}

func (f *fakeTx) Commit() error {
	return nil
}

func (f *fakeTx) Rollback() error {
	return nil
}
