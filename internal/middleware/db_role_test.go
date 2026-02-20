package middleware

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestDBRoleMiddleware(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		role, ok := DBRoleFromContext(r.Context())
		if ok {
			w.Header().Set("X-Role", role.Role)
		}
		w.WriteHeader(http.StatusOK)
	})

	tests := []struct {
		name           string
		claims         map[string]interface{}
		availableRoles []string
		expectStatus   int
		expectRole     string
		expectMessage  string
	}{
		{
			name:          "missing auth context",
			expectStatus:  http.StatusUnauthorized,
			expectMessage: "missing authentication",
		},
		{
			name:          "missing db_role claim",
			claims:        map[string]interface{}{},
			expectStatus:  http.StatusForbidden,
			expectMessage: "missing db_role claim",
		},
		{
			name:          "invalid db_role type",
			claims:        map[string]interface{}{"db_role": 123},
			expectStatus:  http.StatusBadRequest,
			expectMessage: "invalid db_role claim type",
		},
		{
			name:           "invalid db_role value",
			claims:         map[string]interface{}{"db_role": "superuser"},
			availableRoles: []string{"app_viewer", "app_analyst"},
			expectStatus:   http.StatusForbidden,
			expectMessage:  "invalid database role: superuser",
		},
		{
			name:           "valid db_role value",
			claims:         map[string]interface{}{"db_role": "app_analyst"},
			availableRoles: []string{"app_viewer", "app_analyst"},
			expectStatus:   http.StatusOK,
			expectRole:     "app_analyst",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/graphql", nil)
			if tt.claims != nil {
				req = req.WithContext(context.WithValue(req.Context(), authContextKey{}, AuthContext{
					Claims: tt.claims,
				}))
			}

			rec := httptest.NewRecorder()
			middleware := DBRoleMiddleware("", tt.availableRoles)
			middleware(handler).ServeHTTP(rec, req)

			if rec.Code != tt.expectStatus {
				t.Fatalf("expected status %d, got %d", tt.expectStatus, rec.Code)
			}

			if tt.expectRole != "" {
				if got := rec.Header().Get("X-Role"); got != tt.expectRole {
					t.Fatalf("expected role %q, got %q", tt.expectRole, got)
				}
			}

			if tt.expectMessage != "" {
				var payload struct {
					Errors []struct {
						Message string `json:"message"`
					} `json:"errors"`
				}
				if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
					t.Fatalf("failed to parse graphql error response: %v", err)
				}
				if len(payload.Errors) != 1 || payload.Errors[0].Message != tt.expectMessage {
					t.Fatalf("expected graphql error %q, got %+v", tt.expectMessage, payload.Errors)
				}
			}
		})
	}
}
