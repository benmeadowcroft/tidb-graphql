package schemarefresh

import (
	"context"
	"crypto/sha256"
	"database/sql/driver"
	"encoding/hex"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"regexp"
	"testing"
	"time"

	"tidb-graphql/internal/dbexec"
	"tidb-graphql/internal/logging"

	"github.com/DATA-DOG/go-sqlmock"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

type structuralFingerprintFixture struct {
	tables      [][]string
	columns     [][]string
	primaryKeys [][]string
	foreignKeys [][]string
	indexes     [][]string
}

func hashRows(rows [][]string) string {
	hash := sha256.New()
	for _, row := range rows {
		for _, value := range row {
			_, _ = fmt.Fprintf(hash, "%d:%s|", len(value), value)
		}
		_, _ = hash.Write([]byte{'\n'})
	}
	return hex.EncodeToString(hash.Sum(nil))
}

func expectedStructuralFingerprint(fixture structuralFingerprintFixture) string {
	return combineComponentHashes(map[string]string{
		"tables":       hashRows(fixture.tables),
		"columns":      hashRows(fixture.columns),
		"primary_keys": hashRows(fixture.primaryKeys),
		"foreign_keys": hashRows(fixture.foreignKeys),
		"indexes":      hashRows(fixture.indexes),
	})
}

func expectedLightweightFingerprint(rows [][]string) string {
	return combineComponentHashes(map[string]string{
		"table_timestamps": hashRows(rows),
	})
}

func rowsFromStrings(columns []string, values [][]string) *sqlmock.Rows {
	rows := sqlmock.NewRows(columns)
	for _, record := range values {
		args := make([]driver.Value, 0, len(record))
		for _, value := range record {
			args = append(args, value)
		}
		rows.AddRow(args...)
	}
	return rows
}

func expectTiDBStructuralFingerprintQueries(mock sqlmock.Sqlmock, databaseName string, fixture structuralFingerprintFixture) {
	mock.ExpectQuery("TABLE_TYPE IN \\('BASE TABLE', 'VIEW'\\)").
		WithArgs(databaseName).
		WillReturnRows(rowsFromStrings(
			[]string{"TABLE_NAME", "TABLE_TYPE"},
			fixture.tables,
		))

	mock.ExpectQuery("FROM INFORMATION_SCHEMA.COLUMNS").
		WithArgs(databaseName).
		WillReturnRows(rowsFromStrings(
			[]string{"TABLE_NAME", "COLUMN_NAME", "ORDINAL_POSITION", "DATA_TYPE", "COLUMN_TYPE", "IS_NULLABLE", "COLUMN_DEFAULT", "EXTRA"},
			fixture.columns,
		))

	mock.ExpectQuery("CONSTRAINT_NAME = 'PRIMARY'").
		WithArgs(databaseName).
		WillReturnRows(rowsFromStrings(
			[]string{"TABLE_NAME", "COLUMN_NAME", "ORDINAL_POSITION"},
			fixture.primaryKeys,
		))

	mock.ExpectQuery("REFERENCED_TABLE_NAME IS NOT NULL").
		WithArgs(databaseName).
		WillReturnRows(rowsFromStrings(
			[]string{"TABLE_NAME", "CONSTRAINT_NAME", "COLUMN_NAME", "REFERENCED_TABLE_NAME", "REFERENCED_COLUMN_NAME", "ORDINAL_POSITION", "POSITION_IN_UNIQUE_CONSTRAINT"},
			fixture.foreignKeys,
		))

	mock.ExpectQuery("FROM INFORMATION_SCHEMA.STATISTICS").
		WithArgs(databaseName).
		WillReturnRows(rowsFromStrings(
			[]string{"TABLE_NAME", "INDEX_NAME", "NON_UNIQUE", "SEQ_IN_INDEX", "COLUMN_NAME", "COLLATION", "SUB_PART", "NULLABLE", "INDEX_TYPE"},
			fixture.indexes,
		))
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

	fixture := structuralFingerprintFixture{
		tables: [][]string{
			{"alpha", "BASE TABLE"},
			{"v_alpha", "VIEW"},
		},
		columns: [][]string{
			{"alpha", "id", "1", "bigint", "bigint(20)", "NO", "", "auto_increment"},
			{"alpha", "name", "2", "varchar", "varchar(255)", "NO", "", ""},
			{"v_alpha", "id", "1", "bigint", "bigint(20)", "NO", "", ""},
		},
		primaryKeys: [][]string{
			{"alpha", "id", "1"},
		},
		foreignKeys: [][]string{
			{"alpha", "fk_alpha_parent", "parent_id", "parent", "id", "1", "1"},
		},
		indexes: [][]string{
			{"alpha", "PRIMARY", "0", "1", "id", "A", "", "", "BTREE"},
			{"alpha", "idx_name", "1", "1", "name", "A", "", "YES", "BTREE"},
		},
	}
	expectTiDBStructuralFingerprintQueries(mock, "testdb", fixture)

	manager := &Manager{
		db:           db,
		databaseName: "testdb",
		logger:       testLogger(),
	}

	fingerprint, err := manager.computeFingerprint(t.Context())
	if err != nil {
		t.Fatalf("computeFingerprint failed: %v", err)
	}

	expected := expectedStructuralFingerprint(fixture)
	if fingerprint != expected {
		t.Fatalf("fingerprint mismatch: got %s want %s", fingerprint, expected)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestComputeFingerprint_FallsBackToLightweight(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create sqlmock: %v", err)
	}
	defer db.Close()

	mock.ExpectQuery("TABLE_TYPE IN \\('BASE TABLE', 'VIEW'\\)").
		WithArgs("testdb").
		WillReturnError(fmt.Errorf("structural query unsupported"))

	lightweightRows := [][]string{
		{"alpha", "2025-02-01 12:00:00", "2025-02-01 12:05:00"},
	}
	mock.ExpectQuery("CREATE_TIME").
		WithArgs("testdb").
		WillReturnRows(rowsFromStrings(
			[]string{"TABLE_NAME", "CREATE_TIME", "UPDATE_TIME"},
			lightweightRows,
		))

	manager := &Manager{
		db:           db,
		databaseName: "testdb",
		logger:       testLogger(),
	}

	fingerprint, err := manager.computeFingerprintDetails(t.Context())
	if err != nil {
		t.Fatalf("computeFingerprintDetails failed: %v", err)
	}
	if fingerprint.Mode != fingerprintModeTiDBLightweight {
		t.Fatalf("fingerprint mode mismatch: got %s want %s", fingerprint.Mode, fingerprintModeTiDBLightweight)
	}
	expected := expectedLightweightFingerprint(lightweightRows)
	if fingerprint.Value != expected {
		t.Fatalf("fingerprint mismatch: got %s want %s", fingerprint.Value, expected)
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

	fixture := structuralFingerprintFixture{
		tables: [][]string{
			{"alpha", "BASE TABLE"},
		},
		columns: [][]string{
			{"alpha", "id", "1", "bigint", "bigint(20)", "NO", "", "auto_increment"},
		},
		primaryKeys: [][]string{
			{"alpha", "id", "1"},
		},
		foreignKeys: [][]string{},
		indexes: [][]string{
			{"alpha", "PRIMARY", "0", "1", "id", "A", "", "", "BTREE"},
		},
	}
	expected := expectedStructuralFingerprint(fixture)
	expectTiDBStructuralFingerprintQueries(mock, "testdb", fixture)

	manager := &Manager{
		db:           db,
		databaseName: "testdb",
		logger:       testLogger(),
		minInterval:  10 * time.Second,
		maxInterval:  time.Minute,
	}
	manager.active.Store(&snapshotSet{
		Default:         &Snapshot{Fingerprint: expected},
		ByRole:          map[string]*Snapshot{},
		Fingerprint:     expected,
		FingerprintMode: fingerprintModeTiDBStructural,
		FingerprintComponents: map[string]string{
			"tables":       hashRows(fixture.tables),
			"columns":      hashRows(fixture.columns),
			"primary_keys": hashRows(fixture.primaryKeys),
			"foreign_keys": hashRows(fixture.foreignKeys),
			"indexes":      hashRows(fixture.indexes),
		},
		BuiltAt: time.Now(),
	})

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

	fixture := structuralFingerprintFixture{
		tables: [][]string{
			{"alpha", "BASE TABLE"},
		},
		columns: [][]string{
			{"alpha", "id", "1", "bigint", "bigint(20)", "NO", "", "auto_increment"},
		},
		primaryKeys: [][]string{
			{"alpha", "id", "1"},
		},
		foreignKeys: [][]string{},
		indexes: [][]string{
			{"alpha", "PRIMARY", "0", "1", "id", "A", "", "", "BTREE"},
		},
	}
	expected := expectedStructuralFingerprint(fixture)
	expectTiDBStructuralFingerprintQueries(mock, "testdb", fixture)

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
	manager.active.Store(&snapshotSet{
		Default:     &Snapshot{Fingerprint: "old"},
		ByRole:      map[string]*Snapshot{},
		Fingerprint: "old",
		BuiltAt:     time.Now(),
	})

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

func TestHandlerForContext_RoleAwareSelection(t *testing.T) {
	manager := &Manager{
		roleSchemas: []string{"viewer"},
		roleFromCtx: func(ctx context.Context) (string, bool) {
			role, ok := ctx.Value("role").(string)
			return role, ok
		},
	}

	defaultHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusAccepted)
	})
	viewerHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	manager.active.Store(&snapshotSet{
		Default: &Snapshot{Handler: defaultHandler},
		ByRole: map[string]*Snapshot{
			"viewer": {Handler: viewerHandler},
		},
		Fingerprint: "fp",
		BuiltAt:     time.Now(),
	})

	tests := []struct {
		name       string
		ctx        context.Context
		wantStatus int
	}{
		{
			name:       "known role uses role handler",
			ctx:        context.WithValue(context.Background(), "role", "viewer"),
			wantStatus: http.StatusOK,
		},
		{
			name:       "unknown role forbidden",
			ctx:        context.WithValue(context.Background(), "role", "admin"),
			wantStatus: http.StatusForbidden,
		},
		{
			name:       "missing role forbidden",
			ctx:        context.Background(),
			wantStatus: http.StatusForbidden,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "/graphql", nil).WithContext(tt.ctx)
			rec := httptest.NewRecorder()
			manager.HandlerForContext(req.Context()).ServeHTTP(rec, req)
			if rec.Code != tt.wantStatus {
				t.Fatalf("status mismatch: got %d want %d", rec.Code, tt.wantStatus)
			}
		})
	}
}

func TestHandlerForContext_DefaultWhenRoleSchemasDisabled(t *testing.T) {
	manager := &Manager{}
	manager.active.Store(&snapshotSet{
		Default: &Snapshot{
			Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusAccepted)
			}),
		},
		ByRole:      map[string]*Snapshot{},
		Fingerprint: "fp",
		BuiltAt:     time.Now(),
	})

	req := httptest.NewRequest(http.MethodPost, "/graphql", nil)
	rec := httptest.NewRecorder()
	manager.HandlerForContext(req.Context()).ServeHTTP(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("status mismatch: got %d want %d", rec.Code, http.StatusAccepted)
	}
}

type refreshMetricsRecorder struct {
	ctx          context.Context
	calls        int
	lastTrigger  string
	lastSuccess  bool
	lastDuration time.Duration
	lastMode     string
}

func (r *refreshMetricsRecorder) RecordRefresh(ctx context.Context, duration time.Duration, success bool, trigger string, fingerprintMode string) {
	r.ctx = ctx
	r.calls++
	r.lastTrigger = trigger
	r.lastSuccess = success
	r.lastDuration = duration
	r.lastMode = fingerprintMode
}

func TestRefreshNowContext_ForwardsContextToMetrics(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create sqlmock: %v", err)
	}
	defer db.Close()

	mock.ExpectQuery("TABLE_TYPE IN \\('BASE TABLE', 'VIEW'\\)").
		WithArgs("testdb").
		WillReturnError(fmt.Errorf("structural fingerprint failed"))
	mock.ExpectQuery("CREATE_TIME").
		WithArgs("testdb").
		WillReturnError(fmt.Errorf("lightweight fingerprint failed"))

	metricsRecorder := &refreshMetricsRecorder{}
	manager := &Manager{
		db:           db,
		databaseName: "testdb",
		logger:       testLogger(),
		metrics:      metricsRecorder,
	}

	type ctxKey struct{}
	ctx := context.WithValue(context.Background(), ctxKey{}, "manual-refresh")
	if err := manager.RefreshNowContext(ctx); err == nil {
		t.Fatalf("expected RefreshNowContext to fail when fingerprint queries fail")
	}

	if metricsRecorder.calls != 1 {
		t.Fatalf("expected exactly one metrics call, got %d", metricsRecorder.calls)
	}
	if got := metricsRecorder.ctx.Value(ctxKey{}); got != "manual-refresh" {
		t.Fatalf("expected forwarded context value %q, got %v", "manual-refresh", got)
	}
	if metricsRecorder.lastTrigger != "manual" {
		t.Fatalf("expected trigger manual, got %q", metricsRecorder.lastTrigger)
	}
	if metricsRecorder.lastSuccess {
		t.Fatalf("expected failed refresh metrics recording")
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestBuildSnapshotSet_EmitsSnapshotSpanPerRole(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create sqlmock: %v", err)
	}
	defer db.Close()

	emptyTables := sqlmock.NewRows([]string{"TABLE_NAME", "TABLE_TYPE", "TABLE_COMMENT"})
	mock.ExpectQuery("SELECT TABLE_NAME, TABLE_TYPE, TABLE_COMMENT\\s+FROM INFORMATION_SCHEMA.TABLES").
		WithArgs("testdb").
		WillReturnRows(emptyTables)

	mock.ExpectExec(regexp.QuoteMeta("SET ROLE NONE")).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(regexp.QuoteMeta("SET ROLE `viewer`")).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery("SELECT TABLE_NAME, TABLE_TYPE, TABLE_COMMENT\\s+FROM INFORMATION_SCHEMA.TABLES").
		WithArgs("testdb").
		WillReturnRows(sqlmock.NewRows([]string{"TABLE_NAME", "TABLE_TYPE", "TABLE_COMMENT"}))
	mock.ExpectExec(regexp.QuoteMeta("SET ROLE DEFAULT")).
		WillReturnResult(sqlmock.NewResult(0, 0))

	recorder := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSampler(sdktrace.AlwaysSample()))
	tp.RegisterSpanProcessor(recorder)
	originalTP := otel.GetTracerProvider()
	otel.SetTracerProvider(tp)
	t.Cleanup(func() {
		_ = tp.Shutdown(context.Background())
		otel.SetTracerProvider(originalTP)
	})

	manager := &Manager{
		db:           db,
		databaseName: "testdb",
		logger:       testLogger(),
		roleSchemas:  []string{"viewer"},
		executor:     dbexec.NewStandardExecutor(db),
	}

	_, err = manager.buildSnapshotSet(context.Background(), fingerprintDetails{
		Value: "fingerprint",
		Mode:  fingerprintModeTiDBStructural,
	})
	if err != nil {
		t.Fatalf("buildSnapshotSet failed: %v", err)
	}

	snapshotSpans := make([]sdktrace.ReadOnlySpan, 0)
	for _, span := range recorder.Ended() {
		if span.Name() == "schema.build_snapshot" {
			snapshotSpans = append(snapshotSpans, span)
		}
	}
	if len(snapshotSpans) != 2 {
		t.Fatalf("expected 2 schema.build_snapshot spans, got %d", len(snapshotSpans))
	}

	roles := map[string]bool{}
	for _, span := range snapshotSpans {
		attrs := span.Attributes()
		role := readSpanStringAttr(attrs, "schema.role")
		success := readSpanBoolAttr(attrs, "schema.build.success")
		mode := readSpanStringAttr(attrs, "schema.fingerprint_mode")
		roles[role] = true
		if !success {
			t.Fatalf("expected schema.build.success=true for role %s", role)
		}
		if mode != fingerprintModeTiDBStructural {
			t.Fatalf("expected schema.fingerprint_mode=%s, got %s", fingerprintModeTiDBStructural, mode)
		}
	}
	if !roles["default"] || !roles["viewer"] {
		t.Fatalf("expected spans for roles default and viewer, got %#v", roles)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func readSpanStringAttr(attrs []attribute.KeyValue, key string) string {
	for _, attr := range attrs {
		if string(attr.Key) == key {
			return attr.Value.AsString()
		}
	}
	return ""
}

func readSpanBoolAttr(attrs []attribute.KeyValue, key string) bool {
	for _, attr := range attrs {
		if string(attr.Key) == key {
			return attr.Value.AsBool()
		}
	}
	return false
}
