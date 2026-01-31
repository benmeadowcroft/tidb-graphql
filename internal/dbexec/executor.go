// Package dbexec provides database query execution abstractions.
// It supports both direct execution and role-based execution using TiDB's SET ROLE.
package dbexec

import (
	"context"
	"database/sql"
)

// Rows abstracts sql.Rows to allow wrapped cleanup behavior.
type Rows interface {
	Next() bool
	Scan(dest ...any) error
	Err() error
	Close() error
}

// TxExecutor abstracts SQL execution within a transaction.
type TxExecutor interface {
	QueryContext(ctx context.Context, query string, args ...any) (Rows, error)
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	Commit() error
	Rollback() error
}

// QueryExecutor abstracts SQL execution so callers can swap in role-aware behavior.
type QueryExecutor interface {
	QueryContext(ctx context.Context, query string, args ...any) (Rows, error)
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	BeginTx(ctx context.Context) (TxExecutor, error)
}

// StandardExecutor executes queries directly against a database handle.
type StandardExecutor struct {
	db *sql.DB
}

// NewStandardExecutor creates an executor that runs queries directly against the database.
func NewStandardExecutor(db *sql.DB) *StandardExecutor {
	return &StandardExecutor{db: db}
}

func (e *StandardExecutor) QueryContext(ctx context.Context, query string, args ...any) (Rows, error) {
	if e.db == nil {
		return nil, sql.ErrConnDone
	}
	return e.db.QueryContext(ctx, query, args...)
}

func (e *StandardExecutor) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	if e.db == nil {
		return nil, sql.ErrConnDone
	}
	return e.db.ExecContext(ctx, query, args...)
}

func (e *StandardExecutor) BeginTx(ctx context.Context) (TxExecutor, error) {
	if e.db == nil {
		return nil, sql.ErrConnDone
	}
	tx, err := e.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	return &standardTx{tx: tx}, nil
}

type standardTx struct {
	tx *sql.Tx
}

func (t *standardTx) QueryContext(ctx context.Context, query string, args ...any) (Rows, error) {
	if t.tx == nil {
		return nil, sql.ErrConnDone
	}
	return t.tx.QueryContext(ctx, query, args...)
}

func (t *standardTx) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	if t.tx == nil {
		return nil, sql.ErrConnDone
	}
	return t.tx.ExecContext(ctx, query, args...)
}

func (t *standardTx) Commit() error {
	if t.tx == nil {
		return sql.ErrConnDone
	}
	return t.tx.Commit()
}

func (t *standardTx) Rollback() error {
	if t.tx == nil {
		return sql.ErrConnDone
	}
	return t.tx.Rollback()
}
