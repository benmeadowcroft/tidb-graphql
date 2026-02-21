package middleware

import (
	"log/slog"
	"net/http"

	"tidb-graphql/internal/dbexec"
	"tidb-graphql/internal/gqlrequest"
	"tidb-graphql/internal/logging"
	"tidb-graphql/internal/resolver"
)

// MutationTransactionMiddleware wraps GraphQL mutations in a single transaction.
func MutationTransactionMiddleware(executor dbexec.QueryExecutor) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			reqLogger := logging.FromContext(r.Context())

			if executor == nil {
				reqLogger.Debug("mutation middleware: executor is nil, skipping transaction handling")
				next.ServeHTTP(w, r)
				return
			}

			analysis := gqlrequest.AnalysisFromContext(r.Context())
			if analysis == nil || analysis.OperationType != "mutation" {
				next.ServeHTTP(w, r)
				return
			}

			tx, err := executor.BeginTx(r.Context())
			if err != nil {
				reqLogger.Error("failed to start mutation transaction",
					slog.String("error", err.Error()),
				)
				http.Error(w, "failed to start transaction", http.StatusInternalServerError)
				return
			}

			mc := resolver.NewMutationContext(tx)
			ctx := resolver.WithMutationContext(r.Context(), mc)

			defer func() {
				var finalizeErr error
				if rec := recover(); rec != nil {
					mc.MarkError()
					finalizeErr = mc.Finalize()
					if finalizeErr != nil {
						reqLogger.Error("failed to rollback mutation transaction after panic",
							slog.String("error", finalizeErr.Error()),
						)
					}
					panic(rec)
				}
				finalizeErr = mc.Finalize()
				if finalizeErr != nil {
					reqLogger.Error("failed to finalize mutation transaction",
						slog.String("error", finalizeErr.Error()),
					)
				}
			}()

			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}
