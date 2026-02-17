// Package schemarefresh builds schema snapshots and refreshes them on change.
// See docs/explanation/schema-handling.md.
package schemarefresh

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"log/slog"
	"net/http"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"tidb-graphql/internal/dbexec"
	"tidb-graphql/internal/introspection"
	"tidb-graphql/internal/logging"
	"tidb-graphql/internal/naming"
	"tidb-graphql/internal/observability"
	"tidb-graphql/internal/planner"
	"tidb-graphql/internal/schemafilter"
	"tidb-graphql/internal/sqlutil"

	"github.com/graphql-go/graphql"
	"github.com/graphql-go/handler"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
)

// Snapshot contains an immutable view of the current schema state.
type Snapshot struct {
	Schema      *graphql.Schema
	Handler     http.Handler
	DBSchema    *introspection.Schema
	BuiltAt     time.Time
	Fingerprint string
}

// Config controls schema refresh behavior.
type Config struct {
	DB                     *sql.DB
	DatabaseName           string
	Limits                 *planner.PlanLimits
	DefaultLimit           int
	Logger                 *logging.Logger
	Metrics                *observability.SchemaRefreshMetrics
	MinInterval            time.Duration
	MaxInterval            time.Duration
	GraphiQL               bool
	Filters                schemafilter.Config
	UUIDColumns            map[string][]string
	TinyInt1BooleanColumns map[string][]string
	TinyInt1IntColumns     map[string][]string
	Naming                 naming.Config
	VectorRequireIndex     bool
	VectorMaxTopK          int
	Executor               dbexec.QueryExecutor
	IntrospectionRole      string
	RoleSchemas            []string
	RoleFromCtx            func(context.Context) (string, bool)
}

// Manager maintains and refreshes schema snapshots.
type Manager struct {
	db                     *sql.DB
	databaseName           string
	limits                 *planner.PlanLimits
	defaultLimit           int
	logger                 *logging.Logger
	metrics                *observability.SchemaRefreshMetrics
	minInterval            time.Duration
	maxInterval            time.Duration
	graphiQL               bool
	filters                schemafilter.Config
	uuidColumns            map[string][]string
	tinyInt1BooleanColumns map[string][]string
	tinyInt1IntColumns     map[string][]string
	namingConfig           naming.Config
	vectorRequireIndex     bool
	vectorMaxTopK          int
	executor               dbexec.QueryExecutor
	introspectionRole      string
	roleSchemas            []string
	roleFromCtx            func(context.Context) (string, bool)
	active                 atomic.Value
	wg                     sync.WaitGroup
}

type snapshotSet struct {
	Default               *Snapshot
	ByRole                map[string]*Snapshot
	Fingerprint           string
	FingerprintMode       string
	FingerprintComponents map[string]string
	BuiltAt               time.Time
}

type fingerprintDetails struct {
	Value      string
	Mode       string
	Components map[string]string
}

type fingerprintComponent struct {
	name  string
	query string
}

const (
	fingerprintModeTiDBStructural  = "tidb_structural"
	fingerprintModeTiDBLightweight = "tidb_lightweight"
	fingerprintModeUnknown         = "unknown"
)

// NewManager builds the initial schema snapshot and returns a manager.
func NewManager(cfg Config) (*Manager, error) {
	if cfg.DB == nil {
		return nil, fmt.Errorf("schema refresh manager requires a database handle")
	}
	if cfg.Logger == nil {
		cfg.Logger = &logging.Logger{Logger: slog.Default()}
	}

	minInterval := cfg.MinInterval
	maxInterval := cfg.MaxInterval
	if minInterval <= 0 {
		minInterval = 30 * time.Second
	}
	if maxInterval <= 0 {
		maxInterval = 5 * time.Minute
	}
	if maxInterval < minInterval {
		maxInterval = minInterval
	}

	componentLogger := cfg.Logger.WithFields(slog.String("component", "schema_refresh"))
	manager := &Manager{
		db:                     cfg.DB,
		databaseName:           cfg.DatabaseName,
		limits:                 cfg.Limits,
		defaultLimit:           cfg.DefaultLimit,
		logger:                 componentLogger,
		metrics:                cfg.Metrics,
		minInterval:            minInterval,
		maxInterval:            maxInterval,
		graphiQL:               cfg.GraphiQL,
		filters:                cfg.Filters,
		uuidColumns:            cfg.UUIDColumns,
		tinyInt1BooleanColumns: cfg.TinyInt1BooleanColumns,
		tinyInt1IntColumns:     cfg.TinyInt1IntColumns,
		namingConfig:           cfg.Naming,
		vectorRequireIndex:     cfg.VectorRequireIndex,
		vectorMaxTopK:          cfg.VectorMaxTopK,
		executor:               cfg.Executor,
		introspectionRole:      cfg.IntrospectionRole,
		roleSchemas:            append([]string(nil), cfg.RoleSchemas...),
		roleFromCtx:            cfg.RoleFromCtx,
	}
	if manager.executor == nil {
		manager.executor = dbexec.NewStandardExecutor(cfg.DB)
	}
	if manager.defaultLimit <= 0 {
		manager.defaultLimit = planner.DefaultListLimit
	}

	start := time.Now()
	fingerprint, err := manager.computeFingerprintDetails(context.Background())
	if err != nil {
		manager.logger.Warn("failed to compute schema fingerprint", slog.String("error", err.Error()))
	}

	state, err := manager.buildSnapshotSet(context.Background(), fingerprint)
	if err != nil {
		manager.recordRefresh(time.Since(start), false, "startup", fingerprint.Mode)
		return nil, err
	}
	manager.active.Store(state)
	manager.recordRefresh(time.Since(start), true, "startup", state.FingerprintMode)

	return manager, nil
}

// Start begins the background refresh loop.
func (m *Manager) Start(ctx context.Context) {
	if m.minInterval <= 0 || m.maxInterval <= 0 {
		m.logger.Info("schema refresh disabled")
		return
	}

	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		m.refreshLoop(ctx)
	}()
}

// Handler returns the HTTP handler for the current schema snapshot.
func (m *Manager) Handler() http.Handler {
	snapshot := m.CurrentSnapshot()
	if snapshot == nil || snapshot.Handler == nil {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			http.Error(w, "schema not ready", http.StatusServiceUnavailable)
		})
	}
	return snapshot.Handler
}

// HandlerForContext returns the HTTP handler for the caller's role context.
// When role-specific snapshots are configured, missing/unknown roles fail closed.
func (m *Manager) HandlerForContext(ctx context.Context) http.Handler {
	state := m.currentState()
	if state == nil {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			http.Error(w, "schema not ready", http.StatusServiceUnavailable)
		})
	}
	if len(m.roleSchemas) == 0 {
		if state.Default != nil && state.Default.Handler != nil {
			return state.Default.Handler
		}
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			http.Error(w, "schema not ready", http.StatusServiceUnavailable)
		})
	}
	if m.roleFromCtx == nil {
		return forbiddenRoleHandler()
	}
	role, ok := m.roleFromCtx(ctx)
	if !ok || role == "" {
		return forbiddenRoleHandler()
	}
	roleSnapshot, exists := state.ByRole[role]
	if !exists || roleSnapshot == nil || roleSnapshot.Handler == nil {
		return forbiddenRoleHandler()
	}
	return roleSnapshot.Handler
}

// CurrentSnapshot returns the active schema snapshot.
func (m *Manager) CurrentSnapshot() *Snapshot {
	state := m.currentState()
	if state == nil {
		return nil
	}
	return state.Default
}

func (m *Manager) currentState() *snapshotSet {
	if value := m.active.Load(); value != nil {
		switch v := value.(type) {
		case *snapshotSet:
			return v
		case *Snapshot:
			// Backward-compatible fallback for tests/older in-memory values.
			return &snapshotSet{
				Default:               v,
				ByRole:                map[string]*Snapshot{},
				Fingerprint:           v.Fingerprint,
				FingerprintMode:       fingerprintModeUnknown,
				FingerprintComponents: map[string]string{},
				BuiltAt:               v.BuiltAt,
			}
		}
	}
	return nil
}

// RefreshNow forces a schema rebuild and swap.
func (m *Manager) RefreshNow() error {
	return m.RefreshNowContext(context.Background())
}

// RefreshNowContext forces a schema rebuild and swap with context support.
func (m *Manager) RefreshNowContext(ctx context.Context) error {
	start := time.Now()
	fingerprint, err := m.computeFingerprintDetails(ctx)
	if err != nil {
		m.recordRefresh(time.Since(start), false, "manual", fingerprint.Mode)
		return err
	}

	state, err := m.buildSnapshotSet(ctx, fingerprint)
	if err != nil {
		m.recordRefresh(time.Since(start), false, "manual", fingerprint.Mode)
		return err
	}

	m.active.Store(state)
	m.recordRefresh(time.Since(start), true, "manual", state.FingerprintMode)
	return nil
}

// Wait blocks until the refresh loop exits or the context is canceled.
func (m *Manager) Wait(ctx context.Context) error {
	done := make(chan struct{})
	go func() {
		m.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (m *Manager) refreshLoop(ctx context.Context) {
	interval := m.minInterval
	timer := time.NewTimer(interval)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			m.logger.Info("schema refresh stopped")
			return
		case <-timer.C:
			m.refreshOnce(ctx, &interval)
			timer.Reset(interval)
		}
	}
}

func (m *Manager) introspectionQueryer(ctx context.Context) (introspection.Queryer, func(), error) {
	if m.introspectionRole == "" {
		return m.db, nil, nil
	}

	conn, err := m.db.Conn(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to acquire connection: %w", err)
	}

	setRoleSQL := fmt.Sprintf("SET ROLE %s", sqlutil.QuoteIdentifier(m.introspectionRole))
	if _, err := conn.ExecContext(ctx, setRoleSQL); err != nil {
		_ = conn.Close()
		return nil, nil, fmt.Errorf("failed to set introspection role %s: %w", m.introspectionRole, err)
	}

	cleanup := func() {
		_, _ = conn.ExecContext(context.Background(), "SET ROLE DEFAULT")
		_ = conn.Close()
	}

	return conn, cleanup, nil
}

func (m *Manager) roleQueryer(ctx context.Context, role string) (introspection.Queryer, func(), error) {
	conn, err := m.db.Conn(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to acquire connection: %w", err)
	}
	if _, err := conn.ExecContext(ctx, "SET ROLE NONE"); err != nil {
		_ = conn.Close()
		return nil, nil, fmt.Errorf("failed to clear roles before setting role %s: %w", role, err)
	}
	setRoleSQL := fmt.Sprintf("SET ROLE %s", sqlutil.QuoteIdentifier(role))
	if _, err := conn.ExecContext(ctx, setRoleSQL); err != nil {
		_ = conn.Close()
		return nil, nil, fmt.Errorf("failed to set role %s: %w", role, err)
	}

	cleanup := func() {
		_, _ = conn.ExecContext(context.Background(), "SET ROLE DEFAULT")
		_ = conn.Close()
	}

	return conn, cleanup, nil
}

func (m *Manager) refreshOnce(ctx context.Context, interval *time.Duration) {
	start := time.Now()
	fingerprint, err := m.computeFingerprintDetails(ctx)
	if err != nil {
		m.logger.Warn("schema fingerprint check failed", slog.String("error", err.Error()))
		m.recordRefresh(time.Since(start), false, "poll", fingerprint.Mode)
		*interval = m.minInterval
		return
	}

	current := m.currentState()
	if current != nil && fingerprint.Value == current.Fingerprint {
		m.recordRefresh(time.Since(start), true, "poll_no_change", fingerprint.Mode)
		*interval = nextInterval(*interval, m.minInterval, m.maxInterval)
		return
	}

	// Component-level diff keeps refresh logs actionable for operators:
	// they can see whether a rebuild came from indexes, keys, columns, etc.
	changedComponents := changedFingerprintComponents(
		mapOrEmpty(currentFingerprintComponents(current)),
		mapOrEmpty(fingerprint.Components),
	)
	m.logger.Info("schema change detected, rebuilding",
		slog.String("fingerprint", fingerprint.Value),
		slog.String("fingerprint_mode", fingerprint.Mode),
		slog.Any("changed_components", changedComponents),
	)
	state, err := m.buildSnapshotSet(ctx, fingerprint)
	if err != nil {
		m.logger.Error("failed to rebuild schema", slog.String("error", err.Error()))
		m.recordRefresh(time.Since(start), false, "poll", fingerprint.Mode)
		*interval = m.minInterval
		return
	}

	m.active.Store(state)
	*interval = m.minInterval
	m.recordRefresh(time.Since(start), true, "poll", state.FingerprintMode)
	m.logger.Info("schema refresh complete",
		slog.String("fingerprint", state.Fingerprint),
		slog.String("fingerprint_mode", state.FingerprintMode),
	)
}

func (m *Manager) buildSnapshotSet(ctx context.Context, fingerprint fingerprintDetails) (*snapshotSet, error) {
	if fingerprint.Value == "" {
		recomputed, err := m.computeFingerprintDetails(ctx)
		if err == nil {
			fingerprint = recomputed
		}
	}
	defaultQueryer, defaultCleanup, err := m.introspectionQueryer(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize introspection role: %w", err)
	}
	defaultSnapshot, err := m.buildSnapshotWithQueryer(ctx, fingerprint.Value, defaultQueryer)
	if defaultCleanup != nil {
		defaultCleanup()
	}
	if err != nil {
		return nil, err
	}

	state := &snapshotSet{
		Default:               defaultSnapshot,
		ByRole:                map[string]*Snapshot{},
		Fingerprint:           defaultSnapshot.Fingerprint,
		FingerprintMode:       defaultOrUnknownMode(fingerprint.Mode),
		FingerprintComponents: mapOrEmpty(fingerprint.Components),
		BuiltAt:               defaultSnapshot.BuiltAt,
	}

	if len(m.roleSchemas) == 0 {
		return state, nil
	}

	// Role snapshots are built as a single unit so requests never observe a
	// partially refreshed role set after a schema change.
	roleSnapshots := make(map[string]*Snapshot, len(m.roleSchemas))
	for _, role := range m.roleSchemas {
		roleQueryer, roleCleanup, err := m.roleQueryer(ctx, role)
		if err != nil {
			return nil, fmt.Errorf("failed to initialize role queryer for %s: %w", role, err)
		}
		roleSnapshot, err := m.buildSnapshotWithQueryer(ctx, fingerprint.Value, roleQueryer)
		if roleCleanup != nil {
			roleCleanup()
		}
		if err != nil {
			return nil, fmt.Errorf("failed to build role-specific schema for %s: %w", role, err)
		}
		roleSnapshots[role] = roleSnapshot
	}
	state.ByRole = roleSnapshots
	return state, nil
}

func (m *Manager) buildSnapshotWithQueryer(ctx context.Context, fingerprint string, queryer introspection.Queryer) (*Snapshot, error) {
	start := time.Now()
	executor := m.executor
	if executor == nil {
		executor = dbexec.NewStandardExecutor(m.db)
	}

	m.logger.Info("introspecting database schema")
	buildResult, err := BuildSchema(ctx, BuildSchemaConfig{
		Queryer:                queryer,
		Executor:               executor,
		DatabaseName:           m.databaseName,
		Filters:                m.filters,
		UUIDColumns:            m.uuidColumns,
		TinyInt1BooleanColumns: m.tinyInt1BooleanColumns,
		TinyInt1IntColumns:     m.tinyInt1IntColumns,
		Naming:                 m.namingConfig,
		Limits:                 m.limits,
		DefaultLimit:           m.defaultLimit,
		VectorRequireIndex:     m.vectorRequireIndex,
		VectorMaxTopK:          m.vectorMaxTopK,
	})
	if err != nil {
		return nil, err
	}
	dbSchema := buildResult.DBSchema
	graphqlSchema := buildResult.GraphQLSchema

	m.logger.Info("discovered tables", slog.Int("count", len(dbSchema.Tables)))
	for _, table := range dbSchema.Tables {
		m.logger.Debug("table discovered",
			slog.String("table", table.Name),
			slog.Int("columns", len(table.Columns)),
			slog.Int("foreignKeys", len(table.ForeignKeys)),
			slog.Int("indexes", len(table.Indexes)),
		)
	}

	graphqlHandler := handler.New(&handler.Config{
		Schema:     &graphqlSchema,
		Pretty:     true,
		GraphiQL:   m.graphiQL,
		Playground: true,
	})

	if fingerprint == "" {
		fingerprint, _ = m.computeFingerprint(ctx)
	}

	m.logger.Info("schema snapshot built", slog.Duration("duration", time.Since(start)))

	return &Snapshot{
		Schema:      &graphqlSchema,
		Handler:     graphqlHandler,
		DBSchema:    dbSchema,
		BuiltAt:     time.Now(),
		Fingerprint: fingerprint,
	}, nil
}

func (m *Manager) computeFingerprint(ctx context.Context) (string, error) {
	fingerprint, err := m.computeFingerprintDetails(ctx)
	if err != nil {
		return "", err
	}
	return fingerprint.Value, nil
}

func (m *Manager) computeFingerprintDetails(ctx context.Context) (fingerprintDetails, error) {
	fallback := fingerprintDetails{
		Mode:       fingerprintModeUnknown,
		Components: map[string]string{},
	}

	tracer := otel.Tracer("tidb-graphql/introspection")
	ctx, span := tracer.Start(ctx, "introspection.compute_fingerprint")
	defer span.End()

	queryer, cleanup, err := m.introspectionQueryer(ctx)
	if err != nil {
		span.RecordError(err)
		return fallback, err
	}
	if cleanup != nil {
		defer cleanup()
	}

	details, err := m.computeTiDBStructuralFingerprint(ctx, queryer)
	if err == nil {
		span.SetAttributes(
			attribute.String("db.schema", m.databaseName),
			attribute.String("schema.fingerprint_mode", details.Mode),
		)
		return details, nil
	}

	// Fallback preserves availability when structural metadata is unavailable
	// (engine differences or metadata access limits), while still detecting broad changes.
	if m.logger != nil {
		m.logger.Warn("tidb structural fingerprint failed, falling back to lightweight fingerprint",
			slog.String("error", err.Error()),
		)
	}
	fallback, fallbackErr := m.computeTiDBLightweightFingerprint(ctx, queryer)
	if fallbackErr != nil {
		span.RecordError(err)
		span.RecordError(fallbackErr)
		return fingerprintDetails{
			Mode:       fingerprintModeUnknown,
			Components: map[string]string{},
		}, fmt.Errorf("failed to compute fingerprints (tidb_structural and tidb_lightweight): structural error: %w; fallback error: %v", err, fallbackErr)
	}

	span.SetAttributes(
		attribute.String("db.schema", m.databaseName),
		attribute.String("schema.fingerprint_mode", fallback.Mode),
	)
	return fallback, nil
}

func (m *Manager) computeTiDBStructuralFingerprint(ctx context.Context, queryer introspection.Queryer) (fingerprintDetails, error) {
	// Structural mode fingerprints only behavior-relevant metadata.
	// Comments are intentionally excluded to avoid churn without API/runtime impact.
	components := []fingerprintComponent{
		{
			name: "tables",
			query: `
				SELECT TABLE_NAME, TABLE_TYPE
				FROM INFORMATION_SCHEMA.TABLES
				WHERE TABLE_SCHEMA = ?
					AND TABLE_TYPE IN ('BASE TABLE', 'VIEW')
				ORDER BY TABLE_NAME, TABLE_TYPE
			`,
		},
		{
			name: "columns",
			query: `
				SELECT
					TABLE_NAME,
					COLUMN_NAME,
					CAST(ORDINAL_POSITION AS CHAR),
					DATA_TYPE,
					COLUMN_TYPE,
					IS_NULLABLE,
					COALESCE(COLUMN_DEFAULT, ''),
					EXTRA
				FROM INFORMATION_SCHEMA.COLUMNS
				WHERE TABLE_SCHEMA = ?
				ORDER BY TABLE_NAME, ORDINAL_POSITION, COLUMN_NAME
			`,
		},
		{
			name: "primary_keys",
			query: `
				SELECT
					TABLE_NAME,
					COLUMN_NAME,
					CAST(ORDINAL_POSITION AS CHAR)
				FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
				WHERE TABLE_SCHEMA = ?
					AND CONSTRAINT_NAME = 'PRIMARY'
				ORDER BY TABLE_NAME, ORDINAL_POSITION, COLUMN_NAME
			`,
		},
		{
			name: "foreign_keys",
			query: `
				SELECT
					TABLE_NAME,
					CONSTRAINT_NAME,
					COLUMN_NAME,
					COALESCE(REFERENCED_TABLE_NAME, ''),
					COALESCE(REFERENCED_COLUMN_NAME, ''),
					CAST(ORDINAL_POSITION AS CHAR),
					COALESCE(CAST(POSITION_IN_UNIQUE_CONSTRAINT AS CHAR), '')
				FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
				WHERE TABLE_SCHEMA = ?
					AND REFERENCED_TABLE_NAME IS NOT NULL
				ORDER BY TABLE_NAME, CONSTRAINT_NAME, ORDINAL_POSITION, COLUMN_NAME
			`,
		},
		{
			name: "indexes",
			query: `
				SELECT
					TABLE_NAME,
					INDEX_NAME,
					CAST(NON_UNIQUE AS CHAR),
					CAST(SEQ_IN_INDEX AS CHAR),
					COALESCE(COLUMN_NAME, ''),
					COALESCE(COLLATION, ''),
					COALESCE(CAST(SUB_PART AS CHAR), ''),
					COALESCE(NULLABLE, ''),
					COALESCE(INDEX_TYPE, '')
				FROM INFORMATION_SCHEMA.STATISTICS
				WHERE TABLE_SCHEMA = ?
				ORDER BY TABLE_NAME, INDEX_NAME, SEQ_IN_INDEX, COLUMN_NAME
			`,
		},
	}

	componentHashes := make(map[string]string, len(components))
	for _, component := range components {
		hash, _, err := m.hashComponentQuery(ctx, queryer, component.query, m.databaseName)
		if err != nil {
			return fingerprintDetails{}, fmt.Errorf("failed to hash %s component: %w", component.name, err)
		}
		componentHashes[component.name] = hash
	}

	return fingerprintDetails{
		Value:      combineComponentHashes(componentHashes),
		Mode:       fingerprintModeTiDBStructural,
		Components: componentHashes,
	}, nil
}

func (m *Manager) computeTiDBLightweightFingerprint(ctx context.Context, queryer introspection.Queryer) (fingerprintDetails, error) {
	query := `
		SELECT
			TABLE_NAME,
			COALESCE(CAST(CREATE_TIME AS CHAR), ''),
			COALESCE(CAST(UPDATE_TIME AS CHAR), '')
		FROM INFORMATION_SCHEMA.TABLES
		WHERE TABLE_SCHEMA = ?
			AND TABLE_TYPE = 'BASE TABLE'
		ORDER BY TABLE_NAME
	`
	componentHash, _, err := m.hashComponentQuery(ctx, queryer, query, m.databaseName)
	if err != nil {
		return fingerprintDetails{}, err
	}

	componentHashes := map[string]string{
		"table_timestamps": componentHash,
	}
	return fingerprintDetails{
		Value:      combineComponentHashes(componentHashes),
		Mode:       fingerprintModeTiDBLightweight,
		Components: componentHashes,
	}, nil
}

func (m *Manager) hashComponentQuery(ctx context.Context, queryer introspection.Queryer, query string, args ...any) (string, int, error) {
	rows, err := queryer.QueryContext(ctx, query, args...)
	if err != nil {
		return "", 0, err
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return "", 0, err
	}
	values := make([]sql.NullString, len(columns))
	scanTargets := make([]any, len(columns))
	for i := range values {
		scanTargets[i] = &values[i]
	}

	hash := sha256.New()
	rowCount := 0
	for rows.Next() {
		rowCount++
		if err := rows.Scan(scanTargets...); err != nil {
			return "", 0, err
		}

		// Length-prefixed cells avoid hash ambiguity from delimiter collisions.
		for _, value := range values {
			cell := ""
			if value.Valid {
				cell = value.String
			}
			_, _ = fmt.Fprintf(hash, "%d:%s|", len(cell), cell)
		}
		_, _ = hash.Write([]byte{'\n'})
	}
	if err := rows.Err(); err != nil {
		return "", 0, err
	}

	return hex.EncodeToString(hash.Sum(nil)), rowCount, nil
}

func nextInterval(current, minInterval, maxInterval time.Duration) time.Duration {
	if current < minInterval {
		return minInterval
	}
	next := current + current/2
	if next > maxInterval {
		return maxInterval
	}
	return next
}

func (m *Manager) recordRefresh(duration time.Duration, success bool, trigger string, fingerprintMode string) {
	if m.metrics == nil {
		return
	}
	m.metrics.RecordRefresh(context.Background(), duration, success, trigger, defaultOrUnknownMode(fingerprintMode))
}

func combineComponentHashes(componentHashes map[string]string) string {
	if len(componentHashes) == 0 {
		return ""
	}
	keys := make([]string, 0, len(componentHashes))
	for key := range componentHashes {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	hash := sha256.New()
	for _, key := range keys {
		_, _ = fmt.Fprintf(hash, "%s=%s\n", key, componentHashes[key])
	}
	return hex.EncodeToString(hash.Sum(nil))
}

func changedFingerprintComponents(previous map[string]string, current map[string]string) []string {
	// Compare over the union of keys so added/removed components are surfaced too.
	keySet := make(map[string]struct{}, len(previous)+len(current))
	for key := range previous {
		keySet[key] = struct{}{}
	}
	for key := range current {
		keySet[key] = struct{}{}
	}
	keys := make([]string, 0, len(keySet))
	for key := range keySet {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	changed := make([]string, 0, len(keys))
	for _, key := range keys {
		if previous[key] != current[key] {
			changed = append(changed, key)
		}
	}
	return changed
}

func mapOrEmpty(input map[string]string) map[string]string {
	if len(input) == 0 {
		return map[string]string{}
	}
	result := make(map[string]string, len(input))
	for key, value := range input {
		result[key] = value
	}
	return result
}

func currentFingerprintComponents(state *snapshotSet) map[string]string {
	if state == nil {
		return nil
	}
	return state.FingerprintComponents
}

func defaultOrUnknownMode(mode string) string {
	if strings.TrimSpace(mode) == "" {
		return fingerprintModeUnknown
	}
	return mode
}

func forbiddenRoleHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "role schema not available", http.StatusForbidden)
	})
}
