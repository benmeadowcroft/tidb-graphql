package dbexec

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
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
// This timestamp is only unambiguous when the session time_zone is pinned to UTC.
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

type sessionCleanup func(context.Context, *sql.Conn) error
type sessionOp func(context.Context, *sql.Conn) (sessionCleanup, error)

type reservedConn struct {
	conn     *sql.Conn
	cleanups []sessionCleanup
}

func acquireConnWithSession(ctx context.Context, db *sql.DB, ops ...sessionOp) (*reservedConn, error) {
	if db == nil {
		return nil, sql.ErrConnDone
	}
	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to acquire connection: %w", err)
	}

	reserved := &reservedConn{conn: conn}
	for _, op := range ops {
		if op == nil {
			continue
		}
		cleanup, err := op(ctx, conn)
		if err != nil {
			return nil, errors.Join(err, reserved.cleanup(ctx))
		}
		if cleanup != nil {
			reserved.cleanups = append(reserved.cleanups, cleanup)
		}
	}

	return reserved, nil
}

func (r *reservedConn) cleanup(ctx context.Context) error {
	if r == nil || r.conn == nil {
		return nil
	}

	cleanupCtx := context.WithoutCancel(ctx)
	var cleanupErr error
	for i := len(r.cleanups) - 1; i >= 0; i-- {
		cleanupErr = errors.Join(cleanupErr, r.cleanups[i](cleanupCtx, r.conn))
	}
	if cleanupErr != nil {
		cleanupErr = errors.Join(cleanupErr, discardConn(r.conn))
	}
	if err := r.conn.Close(); err != nil && !errors.Is(err, sql.ErrConnDone) {
		cleanupErr = errors.Join(cleanupErr, err)
	}
	r.conn = nil
	r.cleanups = nil
	return cleanupErr
}

func discardConn(conn *sql.Conn) error {
	if conn == nil {
		return nil
	}
	err := conn.Raw(func(any) error {
		return driver.ErrBadConn
	})
	if errors.Is(err, driver.ErrBadConn) {
		return nil
	}
	return err
}

func roleSessionOp(role string) sessionOp {
	if role == "" {
		return nil
	}
	return func(ctx context.Context, conn *sql.Conn) (sessionCleanup, error) {
		if conn == nil {
			return nil, sql.ErrConnDone
		}
		if _, err := conn.ExecContext(ctx, fmt.Sprintf("SET ROLE %s", role)); err != nil {
			return nil, fmt.Errorf("failed to set role %s: %w", role, err)
		}
		return func(cleanupCtx context.Context, conn *sql.Conn) error {
			if conn == nil {
				return sql.ErrConnDone
			}
			if _, err := conn.ExecContext(cleanupCtx, "SET ROLE DEFAULT"); err != nil {
				return fmt.Errorf("failed to reset role: %w", err)
			}
			return nil
		}, nil
	}
}

func useDatabaseSessionOp(databaseName string) sessionOp {
	if databaseName == "" {
		return nil
	}
	return func(ctx context.Context, conn *sql.Conn) (sessionCleanup, error) {
		if conn == nil {
			return nil, sql.ErrConnDone
		}
		if _, err := conn.ExecContext(ctx, fmt.Sprintf("USE %s", databaseName)); err != nil {
			return nil, fmt.Errorf("failed to select database %s: %w", databaseName, err)
		}
		return nil, nil
	}
}

func snapshotSessionOp(snapshot SnapshotRead) sessionOp {
	return func(ctx context.Context, conn *sql.Conn) (sessionCleanup, error) {
		if conn == nil {
			return nil, sql.ErrConnDone
		}

		originalTimeZone, err := currentTimeZone(ctx, conn)
		if err != nil {
			return nil, err
		}
		if _, err := conn.ExecContext(ctx, "SET time_zone = ?", "+00:00"); err != nil {
			return nil, fmt.Errorf("failed to set time_zone to UTC: %w", err)
		}
		if _, err := conn.ExecContext(ctx, "SET @@tidb_snapshot = ?", snapshot.SessionValue()); err != nil {
			restoreErr := restoreTimeZone(context.WithoutCancel(ctx), conn, originalTimeZone)
			return nil, errors.Join(fmt.Errorf("failed to set tidb_snapshot: %w", err), restoreErr)
		}

		return func(cleanupCtx context.Context, conn *sql.Conn) error {
			if conn == nil {
				return sql.ErrConnDone
			}
			var cleanupErr error
			if _, err := conn.ExecContext(cleanupCtx, "SET @@tidb_snapshot = ''"); err != nil {
				cleanupErr = errors.Join(cleanupErr, fmt.Errorf("failed to reset tidb_snapshot: %w", err))
			}
			cleanupErr = errors.Join(cleanupErr, restoreTimeZone(cleanupCtx, conn, originalTimeZone))
			return cleanupErr
		}, nil
	}
}

func currentTimeZone(ctx context.Context, conn *sql.Conn) (string, error) {
	if conn == nil {
		return "", sql.ErrConnDone
	}
	row := conn.QueryRowContext(ctx, "SELECT @@time_zone")
	var value string
	if err := row.Scan(&value); err != nil {
		return "", fmt.Errorf("failed to read time_zone: %w", err)
	}
	return value, nil
}

func restoreTimeZone(ctx context.Context, conn *sql.Conn, value string) error {
	if conn == nil {
		return sql.ErrConnDone
	}
	if _, err := conn.ExecContext(ctx, "SET time_zone = ?", value); err != nil {
		return fmt.Errorf("failed to restore time_zone: %w", err)
	}
	return nil
}

type connAwareRows struct {
	*sql.Rows
	cleanup func() error
}

func (r *connAwareRows) Close() error {
	var err error
	if r.Rows != nil {
		err = r.Rows.Close()
	}
	if r.cleanup != nil {
		err = errors.Join(err, r.cleanup())
	}
	return err
}
