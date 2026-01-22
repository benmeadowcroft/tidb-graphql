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

// QueryExecutor abstracts SQL execution so callers can swap in role-aware behavior.
type QueryExecutor interface {
	QueryContext(ctx context.Context, query string, args ...any) (Rows, error)
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
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
