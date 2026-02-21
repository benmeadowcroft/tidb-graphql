package observability

import (
	"context"
	"testing"

	"tidb-graphql/internal/gqlrequest"

	"github.com/graphql-go/graphql/language/ast"
	"go.opentelemetry.io/otel/trace"
)

func TestGraphQLSpanAttributes(t *testing.T) {
	analysis := &gqlrequest.Analysis{
		Envelope: gqlrequest.Envelope{
			Query:             "query Q { users { id } }",
			DocumentSizeBytes: 24,
		},
		RequestedOperationName: "Q",
		OperationName:          "Q",
		OperationType:          "query",
		OperationHash:          "hash123",
		FieldCount:             2,
		SelectionDepth:         2,
		VariableCount:          1,
		Operation:              &ast.OperationDefinition{},
	}
	meta := gqlrequest.ExecMeta{
		Role:        "app_viewer",
		Fingerprint: "fp-1",
	}

	attrs := GraphQLSpanAttributes(analysis, meta)
	if len(attrs) == 0 {
		t.Fatalf("expected span attributes")
	}
}

func TestGraphQLLogFieldsIncludesTraceID(t *testing.T) {
	spanCtx := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID: trace.TraceID{1, 2, 3},
		SpanID:  trace.SpanID{4, 5, 6},
		Remote:  true,
	})
	ctx := trace.ContextWithSpanContext(context.Background(), spanCtx)
	fields := GraphQLLogFields(ctx, &gqlrequest.Analysis{
		RequestedOperationName: "Q",
		OperationName:          "Q",
		OperationType:          "query",
		OperationHash:          "hash123",
	}, gqlrequest.ExecMeta{
		Role:        "app_viewer",
		Fingerprint: "fp-1",
	})

	if len(fields) == 0 {
		t.Fatalf("expected log fields")
	}
}
