package dbexec

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"sync/atomic"
	"testing"
	"time"
)

func TestSnapshotCleanupFailureDiscardsConnection(t *testing.T) {
	stubDriver := &sessionTestDriver{
		failResetSnapshot: true,
		timeZoneValue:     "SYSTEM",
	}
	db := sql.OpenDB(&sessionTestConnector{driver: stubDriver})
	defer db.Close()
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	executor := NewSnapshotExecutor(NewStandardExecutor(db))
	ctx := WithSnapshotRead(context.Background(), SnapshotRead{
		Time: time.Date(2026, 4, 1, 10, 0, 0, 0, time.UTC),
	})

	rows, err := executor.QueryContext(ctx, "SELECT conn_id")
	if err != nil {
		t.Fatalf("first QueryContext() error = %v", err)
	}
	var firstID int64
	if !rows.Next() {
		t.Fatal("first query returned no rows")
	}
	if err := rows.Scan(&firstID); err != nil {
		t.Fatalf("first Scan() error = %v", err)
	}
	if err := rows.Close(); err == nil || err.Error() != "failed to reset tidb_snapshot: reset failed" {
		t.Fatalf("first Close() error = %v, want reset error", err)
	}

	secondRows, err := executor.QueryContext(context.Background(), "SELECT conn_id")
	if err != nil {
		t.Fatalf("second QueryContext() error = %v", err)
	}
	var secondID int64
	if !secondRows.Next() {
		t.Fatal("second query returned no rows")
	}
	if err := secondRows.Scan(&secondID); err != nil {
		t.Fatalf("second Scan() error = %v", err)
	}
	if err := secondRows.Close(); err != nil {
		t.Fatalf("second Close() error = %v", err)
	}

	if firstID == secondID {
		t.Fatalf("expected cleanup failure to discard the first connection, got reused connection id %d", firstID)
	}
	if opens := stubDriver.openCount.Load(); opens < 2 {
		t.Fatalf("expected a second physical connection after discard, got %d opens", opens)
	}
}

type sessionTestConnector struct {
	driver *sessionTestDriver
}

func (c *sessionTestConnector) Connect(context.Context) (driver.Conn, error) {
	return c.driver.openConn(), nil
}

func (c *sessionTestConnector) Driver() driver.Driver {
	return c.driver
}

type sessionTestDriver struct {
	openCount         atomic.Int64
	failResetSnapshot bool
	timeZoneValue     string
}

func (d *sessionTestDriver) Open(string) (driver.Conn, error) {
	return d.openConn(), nil
}

func (d *sessionTestDriver) openConn() driver.Conn {
	return &sessionTestConn{
		id:                d.openCount.Add(1),
		failResetSnapshot: d.failResetSnapshot,
		timeZoneValue:     d.timeZoneValue,
	}
}

type sessionTestConn struct {
	id                int64
	failResetSnapshot bool
	timeZoneValue     string
}

func (c *sessionTestConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("not implemented")
}

func (c *sessionTestConn) Close() error {
	return nil
}

func (c *sessionTestConn) Begin() (driver.Tx, error) {
	return nil, errors.New("not implemented")
}

func (c *sessionTestConn) ExecContext(_ context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	switch query {
	case "SET time_zone = ?":
		if len(args) == 1 {
			if value, ok := args[0].Value.(string); ok {
				c.timeZoneValue = value
			}
		}
		return driver.RowsAffected(0), nil
	case "SET @@tidb_snapshot = ?":
		return driver.RowsAffected(0), nil
	case "SET @@tidb_snapshot = ''":
		if c.failResetSnapshot {
			return nil, errors.New("reset failed")
		}
		return driver.RowsAffected(0), nil
	default:
		return driver.RowsAffected(0), nil
	}
}

func (c *sessionTestConn) QueryContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Rows, error) {
	switch query {
	case "SELECT @@time_zone":
		return &sessionTestRows{
			columns: []string{"@@time_zone"},
			values:  [][]driver.Value{{c.timeZoneValue}},
		}, nil
	case "SELECT conn_id":
		return &sessionTestRows{
			columns: []string{"conn_id"},
			values:  [][]driver.Value{{c.id}},
		}, nil
	default:
		return nil, errors.New("unexpected query: " + query)
	}
}

type sessionTestRows struct {
	columns []string
	values  [][]driver.Value
	index   int
}

func (r *sessionTestRows) Columns() []string {
	return r.columns
}

func (r *sessionTestRows) Close() error {
	return nil
}

func (r *sessionTestRows) Next(dest []driver.Value) error {
	if r.index >= len(r.values) {
		return io.EOF
	}
	copy(dest, r.values[r.index])
	r.index++
	return nil
}
