package dbexec

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"tidb-graphql/internal/sqlutil"
)

// RoleExecutor executes queries using SET ROLE on a dedicated connection.
type RoleExecutor struct {
	db           *sql.DB
	databaseName string
	roleFromCtx  func(context.Context) (string, bool)
	allowedRoles map[string]struct{}
	// multiDB disables the USE <database> statement before each query.
	// In multi-database mode all table references are fully qualified
	// (e.g. `shop`.`orders`) so a USE statement is unnecessary and can
	// interfere with cross-database queries.
	multiDB bool
}

// RoleExecutorConfig controls role execution behavior.
type RoleExecutorConfig struct {
	DB           *sql.DB
	DatabaseName string
	RoleFromCtx  func(context.Context) (string, bool)
	AllowedRoles []string
	// MultiDB disables the USE <database> statement before each query.
	// Set this when multiple databases are configured and all table references
	// are fully qualified with their schema prefix.
	MultiDB bool
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
		multiDB:      cfg.MultiDB,
	}
}

func (e *RoleExecutor) QueryContext(ctx context.Context, query string, args ...any) (Rows, error) {
	reserved, err := e.prepareRoleConn(ctx)
	if err != nil {
		return nil, err
	}

	rows, err := reserved.conn.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, errors.Join(err, reserved.cleanup(ctx))
	}

	return &connAwareRows{
		Rows: rows,
		cleanup: func() error {
			return reserved.cleanup(ctx)
		},
	}, nil
}

func (e *RoleExecutor) queryContextWithSnapshot(ctx context.Context, query string, args ...any) (Rows, error) {
	return e.QueryContext(ctx, query, args...)
}

func (e *RoleExecutor) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	reserved, err := e.prepareRoleConn(ctx)
	if err != nil {
		return nil, err
	}

	result, err := reserved.conn.ExecContext(ctx, query, args...)
	return result, errors.Join(err, reserved.cleanup(ctx))
}

func (e *RoleExecutor) BeginTx(ctx context.Context) (TxExecutor, error) {
	reserved, err := e.prepareRoleConn(ctx)
	if err != nil {
		return nil, err
	}

	tx, err := reserved.conn.BeginTx(ctx, nil)
	if err != nil {
		return nil, errors.Join(err, reserved.cleanup(ctx))
	}

	return &roleTx{
		tx: tx,
		cleanup: func() error {
			return reserved.cleanup(ctx)
		},
	}, nil
}

func (e *RoleExecutor) prepareRoleConn(ctx context.Context) (*reservedConn, error) {
	ops := make([]sessionOp, 0, 3)
	role, ok := e.roleFromCtx(ctx)
	if ok && role != "" {
		if _, allowed := e.allowedRoles[role]; !allowed {
			return nil, fmt.Errorf("role not allowed: %s", role)
		}
		ops = append(ops, roleSessionOp(sqlutil.QuoteIdentifier(role)))
	}
	ops = append(ops, e.useDatabaseOp())
	if snapshot, ok := SnapshotReadFromContext(ctx); ok {
		ops = append(ops, snapshotSessionOp(snapshot))
	}
	return acquireConnWithSession(ctx, e.db, ops...)
}

func (e *RoleExecutor) useDatabaseOp() sessionOp {
	// In multi-db mode all queries use fully-qualified table names so no USE is needed.
	if e.multiDB {
		return nil
	}
	if e.databaseName == "" {
		return nil
	}
	return useDatabaseSessionOp(sqlutil.QuoteIdentifier(e.databaseName))
}

type roleTx struct {
	tx      *sql.Tx
	cleanup func() error
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
	return errors.Join(err, t.cleanup())
}

func (t *roleTx) Rollback() error {
	if t.tx == nil {
		return sql.ErrConnDone
	}
	err := t.tx.Rollback()
	return errors.Join(err, t.cleanup())
}
