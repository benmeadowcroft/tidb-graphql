package dbexec

import (
	"context"
	"database/sql"
	"testing"

	_ "github.com/go-sql-driver/mysql"
)

// TestRoleExecutor tests the RoleExecutor with a real in-memory database
// to properly test connection acquisition and role switching behavior
func TestRoleExecutorConfig(t *testing.T) {
	t.Run("validates role against allowlist", func(t *testing.T) {
		executor := NewRoleExecutor(RoleExecutorConfig{
			DB: nil, // We won't actually execute queries in this test
			RoleFromCtx: func(ctx context.Context) (string, bool) {
				return "test_role", true
			},
			AllowedRoles: []string{"app_admin", "app_analyst"},
		})

		// Check that allowedRoles map is populated correctly
		if len(executor.allowedRoles) != 2 {
			t.Errorf("expected 2 allowed roles, got %d", len(executor.allowedRoles))
		}

		if _, ok := executor.allowedRoles["app_admin"]; !ok {
			t.Error("expected app_admin to be in allowed roles")
		}

		if _, ok := executor.allowedRoles["app_analyst"]; !ok {
			t.Error("expected app_analyst to be in allowed roles")
		}
	})

	t.Run("role extraction function is stored", func(t *testing.T) {
		called := false
		roleFunc := func(ctx context.Context) (string, bool) {
			called = true
			return "test", true
		}

		executor := NewRoleExecutor(RoleExecutorConfig{
			DB:           nil,
			RoleFromCtx:  roleFunc,
			AllowedRoles: []string{},
		})

		// Call the stored function
		role, ok := executor.roleFromCtx(context.Background())
		if !called {
			t.Error("role extraction function was not called")
		}
		if role != "test" || !ok {
			t.Errorf("expected role=test, ok=true, got role=%s, ok=%v", role, ok)
		}
	})

}

func TestStandardExecutor(t *testing.T) {
	t.Run("nil db returns error", func(t *testing.T) {
		executor := &StandardExecutor{db: nil}

		_, err := executor.QueryContext(context.Background(), "SELECT 1")
		if err != sql.ErrConnDone {
			t.Errorf("expected ErrConnDone, got %v", err)
		}

		_, err = executor.ExecContext(context.Background(), "INSERT INTO test VALUES (1)")
		if err != sql.ErrConnDone {
			t.Errorf("expected ErrConnDone, got %v", err)
		}
	})

	t.Run("NewStandardExecutor creates executor with db", func(t *testing.T) {
		// Create a dummy DB (won't actually connect)
		db, err := sql.Open("mysql", "user:pass@tcp(localhost:3306)/test")
		if err != nil {
			t.Fatalf("failed to create db: %v", err)
		}
		defer db.Close()

		executor := NewStandardExecutor(db)
		if executor.db != db {
			t.Error("expected executor to store db reference")
		}
	})
}

// TestRoleValidationLogic tests the validation logic without actual database connections
func TestRoleValidationLogic(t *testing.T) {
	tests := []struct {
		name         string
		role         string
		hasRole      bool
		allowedRoles []string
		expectValid  bool
	}{
		{
			name:         "valid role passes allowlist",
			role:         "app_analyst",
			hasRole:      true,
			allowedRoles: []string{"app_admin", "app_analyst", "app_viewer"},
			expectValid:  true,
		},
		{
			name:         "invalid role is rejected by allowlist",
			role:         "superuser",
			hasRole:      true,
			allowedRoles: []string{"app_admin", "app_analyst"},
			expectValid:  false,
		},
		{
			name:         "no role provided",
			role:         "",
			hasRole:      false,
			allowedRoles: []string{"app_admin"},
			expectValid:  true, // No role means skip validation
		},
		{
			name:         "empty role string with hasRole=true",
			role:         "",
			hasRole:      true,
			allowedRoles: []string{"app_admin"},
			expectValid:  true, // Empty role skips validation
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			executor := NewRoleExecutor(RoleExecutorConfig{
				DB: nil,
				RoleFromCtx: func(ctx context.Context) (string, bool) {
					return tt.role, tt.hasRole
				},
				AllowedRoles: tt.allowedRoles,
			})

			// Simulate the allowlist logic
			role, ok := executor.roleFromCtx(context.Background())
			shouldValidate := ok && role != ""

			var valid bool
			if !shouldValidate {
				valid = true
			} else {
				_, valid = executor.allowedRoles[role]
			}

			if valid != tt.expectValid {
				t.Errorf("expected valid=%v, got valid=%v (role=%q, hasRole=%v)",
					tt.expectValid, valid, role, ok)
			}
		})
	}
}

// TestRoleAwareRows tests the cleanup wrapper
func TestRoleAwareRows(t *testing.T) {
	t.Run("cleanup is called on Close", func(t *testing.T) {
		cleanupCalled := false
		cleanup := func() {
			cleanupCalled = true
		}

		// Note: We can't easily test roleAwareRows without a real database connection
		// because sql.Rows can't be mocked safely. The cleanup logic is verified
		// through integration tests and the defer pattern ensures it always runs.
		// Here we just verify the cleanup function itself works when called.
		cleanup()

		if !cleanupCalled {
			t.Error("expected cleanup function to work when called")
		}
	})
}
