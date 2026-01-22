package middleware

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
)

type dbRoleContextKey struct{}

// DBRoleContext carries validated database role information.
type DBRoleContext struct {
	Role      string
	Validated bool
}

// WithDBRole attaches the database role to the request context.
func WithDBRole(ctx context.Context, role string, validated bool) context.Context {
	return context.WithValue(ctx, dbRoleContextKey{}, DBRoleContext{
		Role:      role,
		Validated: validated,
	})
}

// DBRoleFromContext extracts the database role from context.
func DBRoleFromContext(ctx context.Context) (DBRoleContext, bool) {
	value := ctx.Value(dbRoleContextKey{})
	if value == nil {
		return DBRoleContext{}, false
	}
	role, ok := value.(DBRoleContext)
	return role, ok
}

// DBRoleMiddleware extracts and validates db_role claims from JWTs.
func DBRoleMiddleware(claimName string, validate bool, availableRoles []string) func(http.Handler) http.Handler {
	if claimName == "" {
		claimName = "db_role"
	}

	allowed := make(map[string]struct{}, len(availableRoles))
	for _, role := range availableRoles {
		allowed[role] = struct{}{}
	}

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			authCtx, authenticated := AuthFromContext(r.Context())
			if !authenticated {
				writeGraphQLError(w, http.StatusUnauthorized, "missing authentication", "UNAUTHENTICATED")
				return
			}

			raw, ok := authCtx.Claims[claimName]
			if !ok {
				writeGraphQLError(w, http.StatusForbidden, "missing db_role claim", "FORBIDDEN")
				return
			}

			role, ok := raw.(string)
			if !ok {
				writeGraphQLError(w, http.StatusBadRequest, "invalid db_role claim type", "BAD_REQUEST")
				return
			}

			if validate {
				if _, allowedRole := allowed[role]; !allowedRole {
					writeGraphQLError(w, http.StatusForbidden, fmt.Sprintf("invalid database role: %s", role), "FORBIDDEN")
					return
				}
			}

			ctx := WithDBRole(r.Context(), role, true)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

func writeGraphQLError(w http.ResponseWriter, status int, message string, code string) {
	payload := map[string]any{
		"errors": []map[string]any{
			{
				"message": message,
				"extensions": map[string]any{
					"code": code,
				},
			},
		},
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}
