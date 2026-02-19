package middleware

import (
	"log/slog"
	"net/http"

	"tidb-graphql/internal/dbexec"
	"tidb-graphql/internal/logging"
	"tidb-graphql/internal/resolver"

	"github.com/graphql-go/graphql/language/ast"
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

			query, operationName := extractGraphQLRequest(r)
			opType, parseErr := resolveOperationType(query, operationName)
			if parseErr != nil {
				reqLogger.Debug("mutation middleware: failed to parse operation type",
					slog.String("error", parseErr.Error()),
				)
				next.ServeHTTP(w, r)
				return
			}
			if opType != ast.OperationTypeMutation {
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

func resolveOperationType(query, operationName string) (string, error) {
	if query == "" {
		return "", nil
	}

	metadata, err := extractQueryMetadata(query, operationName)
	if err != nil {
		return "", err
	}
	if metadata == nil || metadata.operationType == "" {
		return "", nil
	}
	return metadata.operationType, nil
}
