package middleware

import (
	"net/http"

	"tidb-graphql/internal/gqlrequest"
	"tidb-graphql/internal/logging"
	"tidb-graphql/internal/observability"
	"tidb-graphql/internal/schemarefresh"
)

// GraphQLRequestAnalysisMiddleware decodes and analyzes the GraphQL request once
// and stores derived metadata in request context for downstream middleware.
func GraphQLRequestAnalysisMiddleware(manager *schemarefresh.Manager) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			analysis := gqlrequest.AnalyzeRequest(r)
			ctx := gqlrequest.WithAnalysis(r.Context(), analysis)

			meta := gqlrequest.ExecMeta{}
			if manager != nil {
				_, role, fingerprint, ok := manager.SnapshotForContext(ctx)
				if ok {
					meta.Role = role
				}
				meta.Fingerprint = fingerprint
			}
			if analysis != nil {
				meta.OperationName = analysis.OperationName
				meta.OperationType = analysis.OperationType
				meta.OperationHash = analysis.OperationHash
			}
			ctx = gqlrequest.WithExecMeta(ctx, meta)

			logger := logging.FromContext(ctx)
			logFields := observability.GraphQLLogFields(ctx, analysis, meta)
			if len(logFields) > 0 {
				ctx = logging.WithLogger(ctx, logger.WithFields(logFields...))
			}

			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}
