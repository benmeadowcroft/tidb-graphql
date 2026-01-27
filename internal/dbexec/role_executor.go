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
	conn, cleanup, err := e.prepareRoleConn(ctx)
	if err != nil {
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
	conn, cleanup, err := e.prepareRoleConn(ctx)
	if err != nil {
		return nil, err
	}
	defer cleanup()

	return conn.ExecContext(ctx, query, args...)
}

func (e *RoleExecutor) BeginTx(ctx context.Context) (TxExecutor, error) {
	conn, cleanup, err := e.prepareRoleConn(ctx)
	if err != nil {
		return nil, err
	}

	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		cleanup()
		return nil, err
	}

	return &roleTx{tx: tx, cleanup: cleanup}, nil
}

func (e *RoleExecutor) prepareRoleConn(ctx context.Context) (*sql.Conn, func(), error) {
	conn, err := e.db.Conn(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to acquire connection: %w", err)
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
				return nil, nil, fmt.Errorf("role not allowed: %s", role)
			}
		}
		// MySQL/TiDB don't support parameterized SET ROLE, use string formatting
		// Safe because role is validated against allowlist above
		setRoleSQL := fmt.Sprintf("SET ROLE %s", sqlutil.QuoteIdentifier(role))
		if _, err := conn.ExecContext(ctx, setRoleSQL); err != nil {
			cleanup()
			return nil, nil, fmt.Errorf("failed to set role %s: %w", role, err)
		}
	}
	if err := e.useDatabase(ctx, conn); err != nil {
		cleanup()
		return nil, nil, err
	}

	return conn, cleanup, nil
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

type roleTx struct {
	tx      *sql.Tx
	cleanup func()
}

func (t *roleTx) QueryContext(ctx context.Context, query string, args ...any) (Rows, error) {
	if t.tx == nil {
		return nil, sql.ErrConnDone
	}
	return t.tx.QueryContext(ctx, query, args...)
}

func (t *roleTx) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	if t.tx == nil {
		return nil, sql.ErrConnDone
	}
	return t.tx.ExecContext(ctx, query, args...)
}

func (t *roleTx) Commit() error {
	if t.tx == nil {
		return sql.ErrConnDone
	}
	err := t.tx.Commit()
	t.cleanup()
	return err
}

func (t *roleTx) Rollback() error {
	if t.tx == nil {
		return sql.ErrConnDone
	}
	err := t.tx.Rollback()
	t.cleanup()
	return err
}
