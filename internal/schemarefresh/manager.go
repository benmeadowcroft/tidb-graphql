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
	"sync"
	"sync/atomic"
	"time"

	"tidb-graphql/internal/dbexec"
	"tidb-graphql/internal/introspection"
	"tidb-graphql/internal/junction"
	"tidb-graphql/internal/logging"
	"tidb-graphql/internal/naming"
	"tidb-graphql/internal/observability"
	"tidb-graphql/internal/planner"
	"tidb-graphql/internal/resolver"
	"tidb-graphql/internal/schemafilter"
	"tidb-graphql/internal/schemanaming"
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
	DB                *sql.DB
	DatabaseName      string
	Limits            *planner.PlanLimits
	DefaultLimit      int
	Logger            *logging.Logger
	Metrics           *observability.SchemaRefreshMetrics
	MinInterval       time.Duration
	MaxInterval       time.Duration
	GraphiQL          bool
	Filters           schemafilter.Config
	UUIDColumns       map[string][]string
	Naming            naming.Config
	Executor          dbexec.QueryExecutor
	IntrospectionRole string
	RoleSchemas       []string
	RoleFromCtx       func(context.Context) (string, bool)
}

// Manager maintains and refreshes schema snapshots.
type Manager struct {
	db                *sql.DB
	databaseName      string
	limits            *planner.PlanLimits
	defaultLimit      int
	logger            *logging.Logger
	metrics           *observability.SchemaRefreshMetrics
	minInterval       time.Duration
	maxInterval       time.Duration
	graphiQL          bool
	filters           schemafilter.Config
	uuidColumns       map[string][]string
	namingConfig      naming.Config
	executor          dbexec.QueryExecutor
	introspectionRole string
	roleSchemas       []string
	roleFromCtx       func(context.Context) (string, bool)
	active            atomic.Value
	wg                sync.WaitGroup
}

type snapshotSet struct {
	Default     *Snapshot
	ByRole      map[string]*Snapshot
	Fingerprint string
	BuiltAt     time.Time
}

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
		db:                cfg.DB,
		databaseName:      cfg.DatabaseName,
		limits:            cfg.Limits,
		defaultLimit:      cfg.DefaultLimit,
		logger:            componentLogger,
		metrics:           cfg.Metrics,
		minInterval:       minInterval,
		maxInterval:       maxInterval,
		graphiQL:          cfg.GraphiQL,
		filters:           cfg.Filters,
		uuidColumns:       cfg.UUIDColumns,
		namingConfig:      cfg.Naming,
		executor:          cfg.Executor,
		introspectionRole: cfg.IntrospectionRole,
		roleSchemas:       append([]string(nil), cfg.RoleSchemas...),
		roleFromCtx:       cfg.RoleFromCtx,
	}
	if manager.executor == nil {
		manager.executor = dbexec.NewStandardExecutor(cfg.DB)
	}
	if manager.defaultLimit <= 0 {
		manager.defaultLimit = planner.DefaultListLimit
	}

	start := time.Now()
	fingerprint, err := manager.computeFingerprint(context.Background())
	if err != nil {
		manager.logger.Warn("failed to compute schema fingerprint", slog.String("error", err.Error()))
	}

	state, err := manager.buildSnapshotSet(context.Background(), fingerprint)
	manager.recordRefresh(time.Since(start), err == nil, "startup")
	if err != nil {
		return nil, err
	}
	manager.active.Store(state)

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
				Default:     v,
				ByRole:      map[string]*Snapshot{},
				Fingerprint: v.Fingerprint,
				BuiltAt:     v.BuiltAt,
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
	fingerprint, err := m.computeFingerprint(ctx)
	if err != nil {
		m.recordRefresh(time.Since(start), false, "manual")
		return err
	}

	state, err := m.buildSnapshotSet(ctx, fingerprint)
	if err != nil {
		m.recordRefresh(time.Since(start), false, "manual")
		return err
	}

	m.active.Store(state)
	m.recordRefresh(time.Since(start), true, "manual")
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
	fingerprint, err := m.computeFingerprint(ctx)
	if err != nil {
		m.logger.Warn("schema fingerprint check failed", slog.String("error", err.Error()))
		m.recordRefresh(time.Since(start), false, "poll")
		*interval = m.minInterval
		return
	}

	current := m.currentState()
	if current != nil && fingerprint == current.Fingerprint {
		m.recordRefresh(time.Since(start), true, "poll_no_change")
		*interval = nextInterval(*interval, m.minInterval, m.maxInterval)
		return
	}

	m.logger.Info("schema change detected, rebuilding", slog.String("fingerprint", fingerprint))
	state, err := m.buildSnapshotSet(ctx, fingerprint)
	if err != nil {
		m.logger.Error("failed to rebuild schema", slog.String("error", err.Error()))
		m.recordRefresh(time.Since(start), false, "poll")
		*interval = m.minInterval
		return
	}

	m.active.Store(state)
	*interval = m.minInterval
	m.recordRefresh(time.Since(start), true, "poll")
	m.logger.Info("schema refresh complete", slog.String("fingerprint", fingerprint))
}

func (m *Manager) buildSnapshotSet(ctx context.Context, fingerprint string) (*snapshotSet, error) {
	defaultQueryer, defaultCleanup, err := m.introspectionQueryer(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize introspection role: %w", err)
	}
	defaultSnapshot, err := m.buildSnapshotWithQueryer(ctx, fingerprint, defaultQueryer)
	if defaultCleanup != nil {
		defaultCleanup()
	}
	if err != nil {
		return nil, err
	}

	state := &snapshotSet{
		Default:     defaultSnapshot,
		ByRole:      map[string]*Snapshot{},
		Fingerprint: defaultSnapshot.Fingerprint,
		BuiltAt:     defaultSnapshot.BuiltAt,
	}

	if len(m.roleSchemas) == 0 {
		return state, nil
	}

	roleSnapshots := make(map[string]*Snapshot, len(m.roleSchemas))
	for _, role := range m.roleSchemas {
		roleQueryer, roleCleanup, err := m.roleQueryer(ctx, role)
		if err != nil {
			return nil, fmt.Errorf("failed to initialize role queryer for %s: %w", role, err)
		}
		roleSnapshot, err := m.buildSnapshotWithQueryer(ctx, fingerprint, roleQueryer)
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

	m.logger.Info("introspecting database schema")
	dbSchema, err := introspection.IntrospectDatabaseContext(ctx, queryer, m.databaseName)
	if err != nil {
		return nil, fmt.Errorf("failed to introspect database: %w", err)
	}
	schemafilter.Apply(dbSchema, m.filters)
	if err := introspection.ApplyUUIDTypeOverrides(dbSchema, m.uuidColumns); err != nil {
		return nil, fmt.Errorf("failed to apply UUID type mappings: %w", err)
	}
	junctions := junction.ClassifyJunctions(dbSchema)
	dbSchema.Junctions = junctions.ToIntrospectionMap()
	namer := naming.New(m.namingConfig, m.logger.Logger)
	if err := introspection.RebuildRelationshipsWithJunctions(dbSchema, namer, dbSchema.Junctions); err != nil {
		return nil, fmt.Errorf("failed to rebuild relationships: %w", err)
	}
	schemanaming.Apply(dbSchema, namer)

	m.logger.Info("discovered tables", slog.Int("count", len(dbSchema.Tables)))
	for _, table := range dbSchema.Tables {
		m.logger.Debug("table discovered",
			slog.String("table", table.Name),
			slog.Int("columns", len(table.Columns)),
			slog.Int("foreignKeys", len(table.ForeignKeys)),
			slog.Int("indexes", len(table.Indexes)),
		)
	}

	res := resolver.NewResolver(m.executor, dbSchema, m.limits, m.defaultLimit, m.filters, m.namingConfig)
	graphqlSchema, err := res.BuildGraphQLSchema()
	if err != nil {
		return nil, fmt.Errorf("failed to build GraphQL schema: %w", err)
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
	tracer := otel.Tracer("tidb-graphql/introspection")
	ctx, span := tracer.Start(ctx, "introspection.compute_fingerprint")
	defer span.End()

	queryer, cleanup, err := m.introspectionQueryer(ctx)
	if err != nil {
		span.RecordError(err)
		return "", err
	}
	if cleanup != nil {
		defer cleanup()
	}

	query := `
		SELECT TABLE_NAME, CREATE_TIME, UPDATE_TIME
		FROM INFORMATION_SCHEMA.TABLES
		WHERE TABLE_SCHEMA = ?
			AND TABLE_TYPE = 'BASE TABLE'
		ORDER BY TABLE_NAME
	`

	rows, err := queryer.QueryContext(ctx, query, m.databaseName)
	if err != nil {
		span.RecordError(err)
		return "", err
	}
	defer rows.Close()

	hash := sha256.New()
	tableCount := 0
	for rows.Next() {
		tableCount++
		var tableName string
		var createTime sql.NullTime
		var updateTime sql.NullTime
		if err := rows.Scan(&tableName, &createTime, &updateTime); err != nil {
			span.RecordError(err)
			return "", err
		}
		createTimestamp := ""
		if createTime.Valid {
			createTimestamp = createTime.Time.UTC().Format(time.RFC3339Nano)
		}
		updateTimestamp := ""
		if updateTime.Valid {
			updateTimestamp = updateTime.Time.UTC().Format(time.RFC3339Nano)
		}
		_, _ = fmt.Fprintf(hash, "%s|%s|%s\n", tableName, createTimestamp, updateTimestamp)
	}
	if err := rows.Err(); err != nil {
		span.RecordError(err)
		return "", err
	}

	fingerprint := hex.EncodeToString(hash.Sum(nil))

	// Add span attributes
	span.SetAttributes(
		attribute.String("db.schema", m.databaseName),
		attribute.Int("schema.table_count", tableCount),
	)

	return fingerprint, nil
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

func (m *Manager) recordRefresh(duration time.Duration, success bool, trigger string) {
	if m.metrics == nil {
		return
	}
	m.metrics.RecordRefresh(context.Background(), duration, success, trigger)
}

func forbiddenRoleHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "role schema not available", http.StatusForbidden)
	})
}
