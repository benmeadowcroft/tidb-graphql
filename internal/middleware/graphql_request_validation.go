package middleware

import (
	"net/http"

	"tidb-graphql/internal/gqlrequest"
)

// GraphQLRequestValidationMiddleware returns pre-execution GraphQL errors for
// request validations discovered during request analysis.
func GraphQLRequestValidationMiddleware() func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			analysis := gqlrequest.AnalysisFromContext(r.Context())
			if analysis != nil && analysis.ValidationError != nil {
				writeGraphQLError(w, http.StatusBadRequest, analysis.ValidationError.Error(), "BAD_REQUEST")
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}
