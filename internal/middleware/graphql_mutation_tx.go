package middleware

import (
	"net/http"

	"tidb-graphql/internal/dbexec"
	"tidb-graphql/internal/resolver"

	"github.com/graphql-go/graphql/language/ast"
	"github.com/graphql-go/graphql/language/parser"
	"github.com/graphql-go/graphql/language/source"
)

// MutationTransactionMiddleware wraps GraphQL mutations in a single transaction.
func MutationTransactionMiddleware(executor dbexec.QueryExecutor) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if executor == nil {
				next.ServeHTTP(w, r)
				return
			}

			query, operationName := extractGraphQLRequest(r)
			opType, ok := resolveOperationType(query, operationName)
			if !ok || opType != ast.OperationTypeMutation {
				next.ServeHTTP(w, r)
				return
			}

			tx, err := executor.BeginTx(r.Context())
			if err != nil {
				http.Error(w, "failed to start transaction", http.StatusInternalServerError)
				return
			}

			mc := resolver.NewMutationContext(tx)
			ctx := resolver.WithMutationContext(r.Context(), mc)

			defer func() {
				if rec := recover(); rec != nil {
					mc.MarkError()
					_ = mc.Finalize()
					panic(rec)
				}
				_ = mc.Finalize()
			}()

			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

func resolveOperationType(query, operationName string) (string, bool) {
	if query == "" {
		return "", false
	}

	doc, err := parser.Parse(parser.ParseParams{
		Source: source.NewSource(&source.Source{
			Body: []byte(query),
			Name: "graphql",
		}),
	})
	if err != nil {
		return "", false
	}

	var first *ast.OperationDefinition
	ops := 0
	for _, def := range doc.Definitions {
		op, ok := def.(*ast.OperationDefinition)
		if !ok {
			continue
		}
		ops++
		if first == nil {
			first = op
		}
		if operationName != "" && op.Name != nil && op.Name.Value == operationName {
			return op.Operation, true
		}
	}

	if operationName != "" {
		return "", false
	}
	if ops == 1 && first != nil {
		return first.Operation, true
	}
	return "", false
}
