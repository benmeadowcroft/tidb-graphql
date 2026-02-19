package resolver

import (
	"context"
	"strings"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

func startResolverSpan(ctx context.Context, name string, attrs ...attribute.KeyValue) (context.Context, trace.Span) {
	tracer := otel.Tracer("tidb-graphql/resolver")
	ctx, span := tracer.Start(ctx, name)
	if len(attrs) > 0 {
		span.SetAttributes(attrs...)
	}
	return ctx, span
}

func finishResolverSpan(span trace.Span, err error, outcome string) {
	if span == nil {
		return
	}
	if outcome == "" {
		if err != nil {
			outcome = "error"
		} else {
			outcome = "success"
		}
	}
	span.SetAttributes(attribute.String("graphql.resolver.outcome", outcome))
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
}

func setMutationResultAttributes(span trace.Span, typename, class, code string) {
	if span == nil {
		return
	}
	if strings.TrimSpace(typename) == "" {
		typename = "unknown"
	}
	if strings.TrimSpace(class) == "" {
		class = "unknown"
	}
	if strings.TrimSpace(code) == "" {
		code = "unknown"
	}
	span.SetAttributes(
		attribute.String("graphql.mutation.result.typename", typename),
		attribute.String("graphql.mutation.result.class", class),
		attribute.String("graphql.mutation.result.code", code),
	)
}
