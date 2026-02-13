package schemarefresh

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"io"
	"log/slog"
	"testing"
	"time"

	"context"
	"tidb-graphql/internal/logging"

	"github.com/DATA-DOG/go-sqlmock"
)

type fingerprintRow struct {
	tableName  string
	createTime sql.NullTime
	updateTime sql.NullTime
}

func expectedFingerprint(rows []fingerprintRow) string {
	hash := sha256.New()
	for _, row := range rows {
		createTimestamp := ""
		if row.createTime.Valid {
			createTimestamp = row.createTime.Time.UTC().Format(time.RFC3339Nano)
		}
		updateTimestamp := ""
		if row.updateTime.Valid {
			updateTimestamp = row.updateTime.Time.UTC().Format(time.RFC3339Nano)
		}
		fmt.Fprintf(hash, "%s|%s|%s\n", row.tableName, createTimestamp, updateTimestamp)
	}
	return hex.EncodeToString(hash.Sum(nil))
}

func testLogger() *logging.Logger {
	handler := slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelInfo})
	return &logging.Logger{Logger: slog.New(handler)}
}

func TestComputeFingerprint(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create sqlmock: %v", err)
	}
	defer db.Close()

	when := time.Date(2025, 1, 15, 10, 30, 45, 0, time.UTC)
	rows := []fingerprintRow{
		{
			tableName:  "alpha",
			createTime: sql.NullTime{Time: when, Valid: true},
			updateTime: sql.NullTime{Time: when.Add(2 * time.Hour), Valid: true},
		},
		{tableName: "beta", createTime: sql.NullTime{Valid: false}, updateTime: sql.NullTime{Valid: false}},
	}

	mockRows := sqlmock.NewRows([]string{"TABLE_NAME", "CREATE_TIME", "UPDATE_TIME"}).
		AddRow(rows[0].tableName, rows[0].createTime, rows[0].updateTime).
		AddRow(rows[1].tableName, rows[1].createTime, rows[1].updateTime)

	mock.ExpectQuery("SELECT TABLE_NAME, CREATE_TIME, UPDATE_TIME").
		WithArgs("testdb").
		WillReturnRows(mockRows)

	manager := &Manager{
		db:           db,
		databaseName: "testdb",
		logger:       testLogger(),
	}

	fingerprint, err := manager.computeFingerprint(t.Context())
	if err != nil {
		t.Fatalf("computeFingerprint failed: %v", err)
	}

	expected := expectedFingerprint(rows)
	if fingerprint != expected {
		t.Fatalf("fingerprint mismatch: got %s want %s", fingerprint, expected)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestRefreshOnce_NoChange_BacksOff(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create sqlmock: %v", err)
	}
	defer db.Close()

	when := time.Date(2025, 1, 15, 10, 30, 45, 0, time.UTC)
	rows := []fingerprintRow{
		{
			tableName:  "alpha",
			createTime: sql.NullTime{Time: when, Valid: true},
			updateTime: sql.NullTime{Time: when.Add(5 * time.Minute), Valid: true},
		},
	}
	expected := expectedFingerprint(rows)

	mockRows := sqlmock.NewRows([]string{"TABLE_NAME", "CREATE_TIME", "UPDATE_TIME"}).
		AddRow(rows[0].tableName, rows[0].createTime, rows[0].updateTime)

	mock.ExpectQuery("SELECT TABLE_NAME, CREATE_TIME, UPDATE_TIME").
		WithArgs("testdb").
		WillReturnRows(mockRows)

	manager := &Manager{
		db:           db,
		databaseName: "testdb",
		logger:       testLogger(),
		minInterval:  10 * time.Second,
		maxInterval:  time.Minute,
	}
	manager.active.Store(&Snapshot{Fingerprint: expected})

	interval := manager.minInterval
	manager.refreshOnce(context.Background(), &interval)

	if interval <= manager.minInterval {
		t.Fatalf("expected backoff interval > min interval, got %v", interval)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestRefreshOnce_Change_Rebuilds(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create sqlmock: %v", err)
	}
	defer db.Close()

	when := time.Date(2025, 2, 1, 12, 0, 0, 0, time.UTC)
	fingerprintRows := []fingerprintRow{
		{
			tableName:  "alpha",
			createTime: sql.NullTime{Time: when, Valid: true},
			updateTime: sql.NullTime{Time: when.Add(30 * time.Minute), Valid: true},
		},
	}
	expected := expectedFingerprint(fingerprintRows)

	mockFingerprintRows := sqlmock.NewRows([]string{"TABLE_NAME", "CREATE_TIME", "UPDATE_TIME"}).
		AddRow(fingerprintRows[0].tableName, fingerprintRows[0].createTime, fingerprintRows[0].updateTime)

	mock.ExpectQuery("SELECT TABLE_NAME, CREATE_TIME, UPDATE_TIME").
		WithArgs("testdb").
		WillReturnRows(mockFingerprintRows)

	// Introspection getTables query.
	mockTables := sqlmock.NewRows([]string{"TABLE_NAME", "TABLE_TYPE", "TABLE_COMMENT"})
	mock.ExpectQuery("SELECT TABLE_NAME, TABLE_TYPE, TABLE_COMMENT\\s+FROM INFORMATION_SCHEMA.TABLES").
		WithArgs("testdb").
		WillReturnRows(mockTables)

	manager := &Manager{
		db:           db,
		databaseName: "testdb",
		logger:       testLogger(),
		minInterval:  5 * time.Second,
		maxInterval:  time.Minute,
	}
	manager.active.Store(&Snapshot{Fingerprint: "old"})

	interval := manager.minInterval
	manager.refreshOnce(context.Background(), &interval)

	snapshot := manager.CurrentSnapshot()
	if snapshot == nil {
		t.Fatalf("expected snapshot after refresh")
	} else {
		if snapshot.Fingerprint != expected {
			t.Fatalf("fingerprint not updated: got %s want %s", snapshot.Fingerprint, expected)
		}
	}
	if interval != manager.minInterval {
		t.Fatalf("interval should reset to min interval, got %v", interval)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}
