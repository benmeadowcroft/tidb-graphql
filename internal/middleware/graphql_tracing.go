package middleware

import (
	"log/slog"
	"net/http"
	"strings"

	"tidb-graphql/internal/gqlrequest"
	"tidb-graphql/internal/logging"
	"tidb-graphql/internal/observability"
	"tidb-graphql/internal/resolver"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
)

// GraphQLTracingMiddleware instruments GraphQL execution with an inner span.
func GraphQLTracingMiddleware() func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			analysis := gqlrequest.AnalysisFromContext(r.Context())
			if analysis == nil || strings.TrimSpace(analysis.Envelope.Query) == "" {
				next.ServeHTTP(w, r)
				return
			}
			meta, _ := gqlrequest.ExecMetaFromContext(r.Context())

			tracer := otel.Tracer("tidb-graphql/graphql")
			ctx, span := tracer.Start(r.Context(), "graphql.execute")
			defer span.End()
			if spanCtx := span.SpanContext(); spanCtx.IsValid() {
				reqLogger := logging.FromContext(ctx).WithFields(
					slog.String("trace_id", spanCtx.TraceID().String()),
					slog.String("span_id", spanCtx.SpanID().String()),
				)
				ctx = logging.WithLogger(ctx, reqLogger)
			}

			if span.IsRecording() {
				span.SetAttributes(observability.GraphQLSpanAttributes(analysis, meta)...)
			}

			// Execute the GraphQL handler
			next.ServeHTTP(w, r.WithContext(ctx))

			// Add dynamic attributes at span end
			if batchState, ok := resolver.GetBatchState(ctx); ok {
				hits := batchState.GetCacheHits()
				misses := batchState.GetCacheMisses()

				if span.IsRecording() {
					span.SetAttributes(
						attribute.Int("graphql.execution.cache_hits", int(hits)),
						attribute.Int("graphql.execution.cache_misses", int(misses)),
					)

					// Add hit ratio if there were any cache accesses
					if total := hits + misses; total > 0 {
						ratio := float64(hits) / float64(total)
						span.SetAttributes(attribute.Float64("graphql.execution.cache_hit_ratio", ratio))
					}
				}
			}
		})
	}
}
