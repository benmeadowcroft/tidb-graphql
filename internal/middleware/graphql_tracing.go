package middleware

import (
	"net/http"

	"tidb-graphql/internal/resolver"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
)

// GraphQLTracingMiddleware instruments GraphQL execution with an inner span.
func GraphQLTracingMiddleware() func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			query, operationName := extractGraphQLRequest(r)
			if query == "" {
				next.ServeHTTP(w, r)
				return
			}

			tracer := otel.Tracer("tidb-graphql/graphql")
			ctx, span := tracer.Start(r.Context(), "graphql.execute")
			defer span.End()

			// Add static attributes at span start
			if operationName != "" {
				span.SetAttributes(attribute.String("graphql.operation.name", operationName))
			}

			// Extract query metadata
			metadata, err := extractQueryMetadata(query, operationName)
			if err == nil && metadata != nil {
				span.SetAttributes(
					attribute.String("graphql.operation.type", metadata.operationType),
					attribute.Int("graphql.query.field_count", metadata.fieldCount),
					attribute.Int("graphql.query.depth", metadata.selectionDepth),
					attribute.Int("graphql.query.variable_count", metadata.variableCount),
				)
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
