package dbexec

import (
	"context"
	"database/sql"
	"fmt"

	"tidb-graphql/internal/sqlutil"
)

// RoleExecutor executes queries using SET ROLE on a dedicated connection.
type RoleExecutor struct {
	db           *sql.DB
	databaseName string
	roleFromCtx  func(context.Context) (string, bool)
	allowedRoles map[string]struct{}
	validateRole bool
}

// RoleExecutorConfig controls role execution behavior.
type RoleExecutorConfig struct {
	DB           *sql.DB
	DatabaseName string
	RoleFromCtx  func(context.Context) (string, bool)
	AllowedRoles []string
	ValidateRole bool
}

// NewRoleExecutor creates an executor that applies SET ROLE before each query.
// This enables database enforced security based on database roles extracted from the request context.
func NewRoleExecutor(cfg RoleExecutorConfig) *RoleExecutor {
	allowed := make(map[string]struct{}, len(cfg.AllowedRoles))
	for _, role := range cfg.AllowedRoles {
		allowed[role] = struct{}{}
	}
	return &RoleExecutor{
		db:           cfg.DB,
		databaseName: cfg.DatabaseName,
		roleFromCtx:  cfg.RoleFromCtx,
		allowedRoles: allowed,
		validateRole: cfg.ValidateRole,
	}
}

func (e *RoleExecutor) QueryContext(ctx context.Context, query string, args ...any) (Rows, error) {
	conn, err := e.db.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to acquire connection: %w", err)
	}

	cleanup := func() {
		_, _ = conn.ExecContext(context.Background(), "SET ROLE DEFAULT")
		_ = conn.Close()
	}

	role, ok := e.roleFromCtx(ctx)
	if ok && role != "" {
		if e.validateRole {
			if _, allowed := e.allowedRoles[role]; !allowed {
				cleanup()
				return nil, fmt.Errorf("role not allowed: %s", role)
			}
		}
		// MySQL/TiDB don't support parameterized SET ROLE, use string formatting
		// Safe because role is validated against allowlist above
		setRoleSQL := fmt.Sprintf("SET ROLE %s", sqlutil.QuoteIdentifier(role))
		if _, err := conn.ExecContext(ctx, setRoleSQL); err != nil {
			cleanup()
			return nil, fmt.Errorf("failed to set role %s: %w", role, err)
		}
	}
	if err := e.useDatabase(ctx, conn); err != nil {
		cleanup()
		return nil, err
	}

	rows, err := conn.QueryContext(ctx, query, args...)
	if err != nil {
		cleanup()
		return nil, err
	}

	return &roleAwareRows{
		Rows:    rows,
		cleanup: cleanup,
	}, nil
}

func (e *RoleExecutor) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	conn, err := e.db.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer func() {
		_, _ = conn.ExecContext(context.Background(), "SET ROLE DEFAULT")
		_ = conn.Close()
	}()

	role, ok := e.roleFromCtx(ctx)
	if ok && role != "" {
		if e.validateRole {
			if _, allowed := e.allowedRoles[role]; !allowed {
				return nil, fmt.Errorf("role not allowed: %s", role)
			}
		}
		// First disable all roles to start from a clean slate.
		if _, err := conn.ExecContext(ctx, "SET ROLE NONE"); err != nil {
			return nil, fmt.Errorf("failed to clear roles for database %s: %w", e.databaseName, err)
		}
		// MySQL/TiDB don't support parameterized SET ROLE, use string formatting
		// Safe because role is validated against allowlist above
		setRoleSQL := fmt.Sprintf("SET ROLE %s", sqlutil.QuoteIdentifier(role))
		if _, err := conn.ExecContext(ctx, setRoleSQL); err != nil {
			return nil, fmt.Errorf("failed to set role %s: %w", role, err)
		}
	}
	if err := e.useDatabase(ctx, conn); err != nil {
		return nil, err
	}

	return conn.ExecContext(ctx, query, args...)
}

func (e *RoleExecutor) useDatabase(ctx context.Context, conn *sql.Conn) error {
	if e.databaseName == "" {
		return nil
	}
	useSQL := fmt.Sprintf("USE %s", sqlutil.QuoteIdentifier(e.databaseName))
	if _, err := conn.ExecContext(ctx, useSQL); err != nil {
		return fmt.Errorf("failed to select database %s: %w", e.databaseName, err)
	}
	return nil
}

type roleAwareRows struct {
	*sql.Rows
	cleanup func()
}

func (r *roleAwareRows) Close() error {
	defer r.cleanup()
	return r.Rows.Close()
}
