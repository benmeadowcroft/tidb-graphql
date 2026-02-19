package serverapp

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"strings"
	"sync"

	"tidb-graphql/internal/config"
	"tidb-graphql/internal/dbexec"
	"tidb-graphql/internal/logging"
	"tidb-graphql/internal/observability"
	"tidb-graphql/internal/planner"
	"tidb-graphql/internal/schemarefresh"
	"tidb-graphql/internal/tlscert"
)

// App owns runtime resources for the tidb-graphql server lifecycle.
type App struct {
	cfg    *config.Config
	logger *logging.Logger

	loggerProvider *observability.LoggerProvider

	effectiveDatabase string
	databaseSource    string
	dsnPresent        bool

	meterProvider        *observability.MeterProvider
	graphqlMetrics       *observability.GraphQLMetrics
	schemaRefreshMetrics *observability.SchemaRefreshMetrics
	securityMetrics      *observability.SecurityMetrics
	tracerProvider       *observability.TracerProvider

	db         *sql.DB
	dbStatsReg interface{ Unregister() error }

	limits         *planner.PlanLimits
	availableRoles []string
	queryExecutor  dbexec.QueryExecutor

	manager      *schemarefresh.Manager
	schemaCancel context.CancelFunc

	graphqlHandler http.Handler
	adminHandler   http.Handler
	mux            *http.ServeMux
	handler        http.Handler

	serverAddr string
	srv        *http.Server
	tlsManager tlscert.Manager

	cleanup cleanupStack

	stateMu      sync.Mutex
	initialized  bool
	started      bool
	serverErrors chan error

	shutdownOnce sync.Once
}

// New creates an App lifecycle wrapper.
func New(cfg *config.Config, logger *logging.Logger) (*App, error) {
	if cfg == nil {
		return nil, fmt.Errorf("config is required")
	}
	if logger == nil {
		return nil, fmt.Errorf("logger is required")
	}

	effectiveDatabase, databaseSource, err := cfg.Database.EffectiveDatabaseName()
	if err != nil {
		return nil, fmt.Errorf("failed to resolve effective database configuration: %w", err)
	}

	return &App{
		cfg:               cfg,
		logger:            logger,
		effectiveDatabase: effectiveDatabase,
		databaseSource:    databaseSource,
		dsnPresent:        strings.TrimSpace(cfg.Database.ConnectionString) != "",
	}, nil
}

// AttachLoggerProvider registers an optional logger provider for shutdown cleanup.
func (a *App) AttachLoggerProvider(provider *observability.LoggerProvider) {
	a.stateMu.Lock()
	defer a.stateMu.Unlock()
	a.loggerProvider = provider
}
