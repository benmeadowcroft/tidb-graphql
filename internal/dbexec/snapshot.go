package dbexec

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

type snapshotContextKey struct{}

// SnapshotRead describes an exact historical snapshot for TiDB read queries.
type SnapshotRead struct {
	Time time.Time
}

// Identity returns a stable representation suitable for cache keys.
func (s SnapshotRead) Identity() string {
	return s.Time.UTC().Format(time.RFC3339Nano)
}

// SessionValue returns the value used for @@tidb_snapshot.
func (s SnapshotRead) SessionValue() string {
	return s.Time.UTC().Format("2006-01-02 15:04:05.999999999")
}

// WithSnapshotRead stores snapshot read configuration on the context.
func WithSnapshotRead(ctx context.Context, snapshot SnapshotRead) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, snapshotContextKey{}, snapshot)
}

// SnapshotReadFromContext retrieves the snapshot read configuration from context.
func SnapshotReadFromContext(ctx context.Context) (SnapshotRead, bool) {
	if ctx == nil {
		return SnapshotRead{}, false
	}
	snapshot, ok := ctx.Value(snapshotContextKey{}).(SnapshotRead)
	return snapshot, ok
}

type snapshotAwareQueryExecutor interface {
	queryContextWithSnapshot(ctx context.Context, query string, args ...any) (Rows, error)
}

type snapshotExecutor struct {
	base QueryExecutor
}

// NewSnapshotExecutor wraps a query executor so read queries honor snapshot context.
func NewSnapshotExecutor(base QueryExecutor) QueryExecutor {
	if base == nil {
		return nil
	}
	if _, ok := base.(*snapshotExecutor); ok {
		return base
	}
	return &snapshotExecutor{base: base}
}

func (e *snapshotExecutor) QueryContext(ctx context.Context, query string, args ...any) (Rows, error) {
	if _, ok := SnapshotReadFromContext(ctx); !ok {
		return e.base.QueryContext(ctx, query, args...)
	}
	if snapshotAware, ok := e.base.(snapshotAwareQueryExecutor); ok {
		return snapshotAware.queryContextWithSnapshot(ctx, query, args...)
	}
	return e.base.QueryContext(ctx, query, args...)
}

func (e *snapshotExecutor) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	return e.base.ExecContext(ctx, query, args...)
}

func (e *snapshotExecutor) BeginTx(ctx context.Context) (TxExecutor, error) {
	return e.base.BeginTx(ctx)
}

func setSnapshotOnConn(ctx context.Context, conn *sql.Conn, snapshot SnapshotRead) error {
	if conn == nil {
		return sql.ErrConnDone
	}
	if _, err := conn.ExecContext(ctx, "SET @@tidb_snapshot = ?", snapshot.SessionValue()); err != nil {
		return fmt.Errorf("failed to set tidb_snapshot: %w", err)
	}
	return nil
}

func resetSnapshotOnConn(ctx context.Context, conn *sql.Conn) {
	if conn == nil {
		return
	}
	_, _ = conn.ExecContext(context.WithoutCancel(ctx), "SET @@tidb_snapshot = ''")
}

type connAwareRows struct {
	*sql.Rows
	cleanup func()
}

func (r *connAwareRows) Close() error {
	defer r.cleanup()
	return r.Rows.Close()
}
