package observability

import (
	"context"
	"log/slog"

	"tidb-graphql/internal/gqlrequest"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// GraphQLSpanAttributes builds canonical span attributes from request analysis.
func GraphQLSpanAttributes(analysis *gqlrequest.Analysis, meta gqlrequest.ExecMeta) []attribute.KeyValue {
	attrs := make([]attribute.KeyValue, 0, 12)

	if analysis != nil {
		if analysis.RequestedOperationName != "" {
			attrs = append(attrs, attribute.String("graphql.operation.requested_name", analysis.RequestedOperationName))
		}
		if analysis.OperationName != "" {
			attrs = append(attrs, attribute.String("graphql.operation.name", analysis.OperationName))
		}
		if analysis.OperationType != "" {
			attrs = append(attrs, attribute.String("graphql.operation.type", analysis.OperationType))
		}
		if analysis.OperationHash != "" {
			attrs = append(attrs, attribute.String("graphql.operation.hash", analysis.OperationHash))
		}
		if analysis.Envelope.DocumentSizeBytes > 0 {
			attrs = append(attrs, attribute.Int("graphql.document.size_bytes", analysis.Envelope.DocumentSizeBytes))
		}
		if analysis.Operation != nil {
			attrs = append(attrs,
				attribute.Int("graphql.query.field_count", analysis.FieldCount),
				attribute.Int("graphql.query.depth", analysis.SelectionDepth),
				attribute.Int("graphql.query.variable_count", analysis.VariableCount),
			)
		}
	}

	if meta.Role != "" {
		attrs = append(attrs, attribute.String("auth.role", meta.Role))
	}
	if meta.Fingerprint != "" {
		attrs = append(attrs, attribute.String("schema.fingerprint", meta.Fingerprint))
	}

	return attrs
}

// GraphQLLogFields builds canonical structured log fields from request analysis.
func GraphQLLogFields(ctx context.Context, analysis *gqlrequest.Analysis, meta gqlrequest.ExecMeta) []any {
	fields := make([]any, 0, 8)

	if analysis != nil {
		if analysis.RequestedOperationName != "" {
			fields = append(fields, slog.String("operation_requested_name", analysis.RequestedOperationName))
		}
		if analysis.OperationName != "" {
			fields = append(fields, slog.String("operation_name", analysis.OperationName))
		}
		if analysis.OperationType != "" {
			fields = append(fields, slog.String("operation_type", analysis.OperationType))
		}
		if analysis.OperationHash != "" {
			fields = append(fields, slog.String("operation_hash", analysis.OperationHash))
		}
	}

	if meta.Role != "" {
		fields = append(fields, slog.String("role", meta.Role))
	}
	if meta.Fingerprint != "" {
		fields = append(fields, slog.String("schema_fingerprint", meta.Fingerprint))
	}

	spanCtx := trace.SpanContextFromContext(ctx)
	if spanCtx.IsValid() {
		fields = append(fields, slog.String("trace_id", spanCtx.TraceID().String()))
	}

	return fields
}
