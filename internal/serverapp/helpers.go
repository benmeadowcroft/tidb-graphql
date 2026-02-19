package serverapp

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"net/http"
	"path"
	"sort"
	"strings"
	"time"

	"tidb-graphql/internal/config"
	"tidb-graphql/internal/dbexec"
	"tidb-graphql/internal/introspection"
	"tidb-graphql/internal/logging"
	"tidb-graphql/internal/middleware"
	"tidb-graphql/internal/observability"
	"tidb-graphql/internal/planner"
	"tidb-graphql/internal/resolver"
	"tidb-graphql/internal/schemarefresh"
	"tidb-graphql/internal/sqlutil"
	"tidb-graphql/internal/tlscert"

	"github.com/XSAM/otelsql"
	_ "github.com/go-sql-driver/mysql"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
)

func InitLogger(cfg *config.Config) (*logging.Logger, *observability.LoggerProvider, error) {
	loggerCfg := logging.Config{
		Level:  cfg.Observability.Logging.Level,
		Format: cfg.Observability.Logging.Format,
	}
	logger := logging.NewLogger(loggerCfg)
	slog.SetDefault(logger.Logger)

	if !cfg.Observability.Logging.ExportsEnabled {
		return logger, nil, nil
	}

	logsConfig := cfg.Observability.GetLogsConfig()
	logger.Info("initializing OpenTelemetry logging",
		slog.String("service_name", cfg.Observability.ServiceName),
		slog.String("service_version", cfg.Observability.ServiceVersion),
		slog.String("environment", cfg.Observability.Environment),
		slog.String("otlp_endpoint", logsConfig.Endpoint),
		slog.String("otlp_protocol", logsConfig.Protocol),
		slog.Bool("insecure", logsConfig.Insecure),
	)

	loggerProvider, err := observability.InitLoggerProvider(observability.Config{
		ServiceName:    cfg.Observability.ServiceName,
		ServiceVersion: cfg.Observability.ServiceVersion,
		Environment:    cfg.Observability.Environment,
		OTLPConfig: observability.OTLPExporterConfig{
			Endpoint:          logsConfig.Endpoint,
			Protocol:          logsConfig.Protocol,
			Insecure:          logsConfig.Insecure,
			TLSCertFile:       logsConfig.TLSCertFile,
			TLSClientCertFile: logsConfig.TLSClientCertFile,
			TLSClientKeyFile:  logsConfig.TLSClientKeyFile,
			Headers:           logsConfig.Headers,
			Timeout:           logsConfig.Timeout,
			Compression:       logsConfig.Compression,
			RetryEnabled:      logsConfig.RetryEnabled,
			RetryMaxAttempts:  logsConfig.RetryMaxAttempts,
		},
	})
	if err != nil {
		return nil, nil, err
	}

	logger.Info("OpenTelemetry logging initialized successfully")

	loggerCfg.LoggerProvider = loggerProvider.Provider()
	logger = logging.NewLogger(loggerCfg)
	slog.SetDefault(logger.Logger)

	return logger, loggerProvider, nil
}

func initMetrics(cfg *config.Config, logger *logging.Logger) (*observability.MeterProvider, *observability.GraphQLMetrics, *observability.SchemaRefreshMetrics, *observability.SecurityMetrics, error) {
	if !cfg.Observability.MetricsEnabled {
		return nil, nil, nil, nil, nil
	}

	logger.Info("initializing OpenTelemetry metrics",
		slog.String("service_name", cfg.Observability.ServiceName),
		slog.String("service_version", cfg.Observability.ServiceVersion),
		slog.String("environment", cfg.Observability.Environment),
	)

	meterProvider, err := observability.InitMeterProvider(observability.Config{
		ServiceName:    cfg.Observability.ServiceName,
		ServiceVersion: cfg.Observability.ServiceVersion,
		Environment:    cfg.Observability.Environment,
		OTLPConfig:     observability.OTLPExporterConfig{},
	})
	if err != nil {
		return nil, nil, nil, nil, err
	}

	logger.Info("OpenTelemetry metrics initialized successfully")

	graphqlMetrics, err := observability.InitMetrics(logger.Logger)
	if err != nil {
		return nil, nil, nil, nil, err
	}

	schemaRefreshMetrics, err := observability.InitSchemaRefreshMetrics(logger.Logger)
	if err != nil {
		return nil, nil, nil, nil, err
	}

	securityMetrics, err := observability.InitSecurityMetrics()
	if err != nil {
		return nil, nil, nil, nil, err
	}
	logger.Info("security metrics initialized")

	return meterProvider, graphqlMetrics, schemaRefreshMetrics, securityMetrics, nil
}

func initTracing(cfg *config.Config, logger *logging.Logger) (*observability.TracerProvider, error) {
	if !cfg.Observability.TracingEnabled {
		return nil, nil
	}

	tracesConfig := cfg.Observability.GetTracesConfig()
	logger.Info("initializing OpenTelemetry tracing",
		slog.String("service_name", cfg.Observability.ServiceName),
		slog.String("service_version", cfg.Observability.ServiceVersion),
		slog.String("environment", cfg.Observability.Environment),
		slog.String("otlp_endpoint", tracesConfig.Endpoint),
		slog.String("otlp_protocol", tracesConfig.Protocol),
		slog.Bool("insecure", tracesConfig.Insecure),
	)

	tracerProvider, err := observability.InitTracerProvider(observability.Config{
		ServiceName:      cfg.Observability.ServiceName,
		ServiceVersion:   cfg.Observability.ServiceVersion,
		Environment:      cfg.Observability.Environment,
		TraceSampleRatio: cfg.Observability.TraceSampleRatio,
		OTLPConfig: observability.OTLPExporterConfig{
			Endpoint:          tracesConfig.Endpoint,
			Protocol:          tracesConfig.Protocol,
			Insecure:          tracesConfig.Insecure,
			TLSCertFile:       tracesConfig.TLSCertFile,
			TLSClientCertFile: tracesConfig.TLSClientCertFile,
			TLSClientKeyFile:  tracesConfig.TLSClientKeyFile,
			Headers:           tracesConfig.Headers,
			Timeout:           tracesConfig.Timeout,
			Compression:       tracesConfig.Compression,
			RetryEnabled:      tracesConfig.RetryEnabled,
			RetryMaxAttempts:  tracesConfig.RetryMaxAttempts,
		},
	})
	if err != nil {
		return nil, err
	}

	logger.Info("OpenTelemetry tracing initialized successfully")

	return tracerProvider, nil
}

func connectDB(cfg *config.Config, logger *logging.Logger) (*sql.DB, interface{ Unregister() error }, error) {
	var db *sql.DB
	var dbStatsReg interface{ Unregister() error }

	// Register custom TLS configuration if needed (for verify-ca/verify-full modes)
	if err := cfg.Database.RegisterTLS(); err != nil {
		return nil, nil, fmt.Errorf("failed to register database TLS config: %w", err)
	}

	dsn := cfg.Database.DSN()
	if cfg.Server.Auth.DBRoleEnabled {
		dsn = cfg.Database.DSNWithoutDatabase()
	}

	if cfg.Observability.MetricsEnabled || cfg.Observability.TracingEnabled {
		opts := []otelsql.Option{
			otelsql.WithAttributes(semconv.DBSystemMySQL),
		}

		if cfg.Observability.TracingEnabled {
			opts = append(opts, otelsql.WithSpanOptions(otelsql.SpanOptions{
				DisableErrSkip: true,
			}))
		}

		if cfg.Observability.SQLCommenterEnabled && cfg.Observability.TracingEnabled {
			opts = append(opts, otelsql.WithSQLCommenter(true))
			logger.Info("SQLCommenter enabled - trace context will be injected into SQL queries")
		} else if cfg.Observability.SQLCommenterEnabled && !cfg.Observability.TracingEnabled {
			logger.Warn("SQLCommenter requires tracing to be enabled - skipping SQLCommenter")
		}

		var err error
		db, err = otelsql.Open("mysql", dsn, opts...)
		if err != nil {
			return nil, nil, err
		}

		if cfg.Observability.MetricsEnabled {
			dbStatsReg, err = otelsql.RegisterDBStatsMetrics(db, otelsql.WithAttributes(semconv.DBSystemMySQL))
			if err != nil {
				logger.Warn("failed to register DB stats metrics", slog.String("error", err.Error()))
			}
		}

		logger.Info("database instrumentation enabled",
			slog.Bool("metrics", cfg.Observability.MetricsEnabled),
			slog.Bool("tracing", cfg.Observability.TracingEnabled),
			slog.Bool("sqlcommenter", cfg.Observability.SQLCommenterEnabled && cfg.Observability.TracingEnabled),
		)
		return db, dbStatsReg, nil
	}

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, nil, err
	}
	return db, nil, nil
}

func configureDatabase(ctx context.Context, cfg *config.Config, logger *logging.Logger, db *sql.DB, effectiveDatabase string, databaseSource string, dsnPresent bool) error {
	if ctx == nil {
		ctx = context.Background()
	}
	db.SetMaxOpenConns(cfg.Database.Pool.MaxOpen)
	db.SetMaxIdleConns(cfg.Database.Pool.MaxIdle)
	db.SetConnMaxLifetime(cfg.Database.Pool.MaxLifetime)

	if err := waitForDatabase(ctx, cfg, logger, db, effectiveDatabase); err != nil {
		return err
	}

	logger.Info("connected to database",
		slog.String("database_effective", effectiveDatabase),
		slog.String("database_source", databaseSource),
		slog.Bool("dsn_present", dsnPresent),
		slog.Int("pool_max_open", cfg.Database.Pool.MaxOpen),
		slog.Int("pool_max_idle", cfg.Database.Pool.MaxIdle),
		slog.Duration("pool_max_lifetime", cfg.Database.Pool.MaxLifetime),
	)
	return nil
}

func waitForDatabase(ctx context.Context, cfg *config.Config, logger *logging.Logger, db *sql.DB, effectiveDatabase string) error {
	if ctx == nil {
		ctx = context.Background()
	}
	timeout := cfg.Database.ConnectionTimeout
	interval := cfg.Database.ConnectionRetryInterval

	// Helper to attempt connection
	tryConnect := func() error {
		if cfg.Server.Auth.DBRoleEnabled {
			return verifyRoleDatabaseAccess(ctx, cfg, db, effectiveDatabase)
		}
		return db.PingContext(ctx)
	}

	// If timeout is 0, try once and fail immediately (backward-compatible)
	if timeout == 0 {
		return tryConnect()
	}

	deadline := time.Now().Add(timeout)
	attempt := 0

	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		attempt++
		err := tryConnect()

		if err == nil {
			if attempt > 1 {
				logger.Info("database connection established", slog.Int("attempts", attempt))
			}
			return nil
		}

		if time.Now().After(deadline) {
			return fmt.Errorf("database not available after %v: %w", timeout, err)
		}

		logger.Warn("database not ready, retrying...",
			slog.Int("attempt", attempt),
			slog.Duration("retry_in", interval),
			slog.String("error", err.Error()),
		)
		time.Sleep(interval)

		// Exponential backoff, capped at 30s
		interval = min(interval*2, 30*time.Second)
	}
}

func verifyRoleDatabaseAccess(ctx context.Context, cfg *config.Config, db *sql.DB, effectiveDatabase string) error {
	if ctx == nil {
		ctx = context.Background()
	}
	conn, err := db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer func() {
		_, _ = conn.ExecContext(context.WithoutCancel(ctx), "SET ROLE DEFAULT")
		_ = conn.Close()
	}()

	if cfg.Server.Auth.DBRoleIntrospectionRole != "" {
		if _, err := conn.ExecContext(ctx, "SET ROLE NONE"); err != nil {
			return fmt.Errorf("failed to clear roles before introspection: %w", err)
		}
		setRoleSQL := fmt.Sprintf("SET ROLE %s", sqlutil.QuoteIdentifier(cfg.Server.Auth.DBRoleIntrospectionRole))
		if _, err := conn.ExecContext(ctx, setRoleSQL); err != nil {
			return fmt.Errorf("failed to set introspection role %s: %w", cfg.Server.Auth.DBRoleIntrospectionRole, err)
		}
	}

	if effectiveDatabase != "" {
		useSQL := fmt.Sprintf("USE %s", sqlutil.QuoteIdentifier(effectiveDatabase))
		if _, err := conn.ExecContext(ctx, useSQL); err != nil {
			return fmt.Errorf("failed to select database %s: %w", effectiveDatabase, err)
		}
	}

	if _, err := conn.ExecContext(ctx, "SELECT 1"); err != nil {
		return fmt.Errorf("failed to validate database access: %w", err)
	}

	return nil
}

func buildPlanLimits(cfg *config.Config) *planner.PlanLimits {
	if cfg.Server.GraphQLMaxDepth > 0 || cfg.Server.GraphQLMaxComplexity > 0 || cfg.Server.GraphQLMaxRows > 0 {
		return &planner.PlanLimits{
			MaxDepth:      cfg.Server.GraphQLMaxDepth,
			MaxComplexity: cfg.Server.GraphQLMaxComplexity,
			MaxRows:       cfg.Server.GraphQLMaxRows,
		}
	}
	return nil
}

func discoverRoles(ctx context.Context, db *sql.DB, logger *logging.Logger) ([]string, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	availableRoles, err := introspection.DiscoverRoles(ctx, db)
	if err != nil {
		logger.Error("failed to discover database roles", slog.String("error", err.Error()))
		return nil, err
	}
	logger.Info("discovered database roles", slog.Any("roles", availableRoles))
	return availableRoles, nil
}

func resolveRoleSchemaTargets(discoveredRoles []string, includePatterns []string, excludePatterns []string, maxRoles int) ([]string, error) {
	if maxRoles <= 0 {
		return nil, fmt.Errorf("role_schema_max_roles must be greater than 0")
	}
	if len(includePatterns) == 0 {
		includePatterns = []string{"*"}
	}

	selected := make([]string, 0, len(discoveredRoles))
	seen := make(map[string]struct{}, len(discoveredRoles))
	for _, role := range discoveredRoles {
		roleName := strings.TrimSpace(role)
		if roleName == "" {
			continue
		}
		if _, exists := seen[roleName]; exists {
			continue
		}
		seen[roleName] = struct{}{}
		if !matchesAnyRolePattern(roleName, includePatterns) {
			continue
		}
		if matchesAnyRolePattern(roleName, excludePatterns) {
			continue
		}
		selected = append(selected, roleName)
	}
	sort.Strings(selected)

	if len(selected) == 0 {
		return nil, fmt.Errorf("no roles selected for schema generation after include/exclude filtering")
	}
	if len(selected) > maxRoles {
		return nil, fmt.Errorf("selected role count %d exceeds role_schema_max_roles %d", len(selected), maxRoles)
	}

	return selected, nil
}

func matchesAnyRolePattern(role string, patterns []string) bool {
	role = strings.ToLower(role)
	for _, pattern := range patterns {
		p := strings.TrimSpace(pattern)
		if p == "" {
			continue
		}
		ok, err := path.Match(strings.ToLower(p), role)
		if err != nil {
			continue
		}
		if ok {
			return true
		}
	}
	return false
}

func validateDBRolePrivileges(ctx context.Context, db *sql.DB, targetDatabase string, logger *logging.Logger) error {
	if ctx == nil {
		ctx = context.Background()
	}
	result, err := introspection.ValidateRoleBasedAuthPrivileges(ctx, db, targetDatabase)
	if err != nil {
		logger.Error("failed to validate database user privileges", slog.String("error", err.Error()))
		return err
	}

	if !result.Valid {
		logger.Error("database user has privileges incompatible with role-based authorization",
			slog.String("reason", "user has direct SELECT privileges that override SET ROLE restrictions"),
			slog.Any("problematic_grants", result.BroadPrivileges),
			slog.String("hint", "create a restricted database user with only role-assumption privileges, not direct table access"),
		)
		return fmt.Errorf("database user has overly broad privileges for role-based authorization")
	}

	logger.Info("database user privileges validated for role-based authorization")
	return nil
}

func buildQueryExecutor(cfg *config.Config, db *sql.DB, availableRoles []string, effectiveDatabase string) dbexec.QueryExecutor {
	queryExecutor := dbexec.QueryExecutor(dbexec.NewStandardExecutor(db))
	if cfg.Server.Auth.DBRoleEnabled {
		queryExecutor = dbexec.NewRoleExecutor(dbexec.RoleExecutorConfig{
			DB:           db,
			DatabaseName: effectiveDatabase,
			RoleFromCtx: func(ctx context.Context) (string, bool) {
				role, ok := middleware.DBRoleFromContext(ctx)
				return role.Role, ok && role.Validated
			},
			AllowedRoles: availableRoles,
			ValidateRole: cfg.Server.Auth.DBRoleValidationEnabled,
		})
	}
	return queryExecutor
}

func startSchemaManager(ctx context.Context, cfg *config.Config, logger *logging.Logger, db *sql.DB, limits *planner.PlanLimits, metrics *observability.SchemaRefreshMetrics, executor dbexec.QueryExecutor, effectiveDatabase string, availableRoles []string) (*schemarefresh.Manager, context.CancelFunc, error) {
	var roleFromCtx func(context.Context) (string, bool)
	if cfg.Server.Auth.DBRoleEnabled {
		roleFromCtx = func(ctx context.Context) (string, bool) {
			role, ok := middleware.DBRoleFromContext(ctx)
			return role.Role, ok && role.Validated
		}
	}
	manager, err := schemarefresh.NewManager(ctx, schemarefresh.Config{
		DB:                     db,
		DatabaseName:           effectiveDatabase,
		Limits:                 limits,
		DefaultLimit:           cfg.Server.GraphQLDefaultLimit,
		Logger:                 logger,
		Metrics:                metrics,
		MinInterval:            cfg.Server.SchemaRefreshMinInterval,
		MaxInterval:            cfg.Server.SchemaRefreshMaxInterval,
		GraphiQL:               cfg.Server.GraphiQLEnabled,
		Filters:                cfg.SchemaFilters,
		UUIDColumns:            cfg.TypeMappings.UUIDColumns,
		TinyInt1BooleanColumns: cfg.TypeMappings.TinyInt1BooleanColumns,
		TinyInt1IntColumns:     cfg.TypeMappings.TinyInt1IntColumns,
		Naming:                 cfg.Naming,
		VectorRequireIndex:     cfg.Server.Search.VectorRequireIndex,
		VectorMaxTopK:          cfg.Server.Search.VectorMaxTopK,
		Executor:               executor,
		IntrospectionRole:      cfg.Server.Auth.DBRoleIntrospectionRole,
		RoleSchemas:            availableRoles,
		RoleFromCtx:            roleFromCtx,
	})
	if err != nil {
		return nil, nil, err
	}

	schemaCtx, schemaCancel := context.WithCancel(context.Background())
	manager.Start(schemaCtx)

	return manager, schemaCancel, nil
}

func oidcAuthConfig(cfg *config.Config) middleware.OIDCAuthConfig {
	return middleware.OIDCAuthConfig{
		Enabled:       cfg.Server.Auth.OIDCEnabled,
		IssuerURL:     cfg.Server.Auth.OIDCIssuerURL,
		Audience:      cfg.Server.Auth.OIDCAudience,
		ClockSkew:     cfg.Server.Auth.OIDCClockSkew,
		SkipTLSVerify: cfg.Server.Auth.OIDCSkipTLSVerify,
	}
}

func buildGraphQLHandler(cfg *config.Config, logger *logging.Logger, manager *schemarefresh.Manager, graphqlMetrics *observability.GraphQLMetrics, securityMetrics *observability.SecurityMetrics, executor dbexec.QueryExecutor, availableRoles []string) (http.Handler, error) {
	graphqlHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		manager.HandlerForContext(r.Context()).ServeHTTP(w, r)
	})

	batchingHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := resolver.NewBatchingContext(r.Context())
		graphqlHandler.ServeHTTP(w, r.WithContext(ctx))
	})

	tracingHandler := middleware.GraphQLTracingMiddleware()(batchingHandler)

	metricsHandler := tracingHandler
	if cfg.Observability.MetricsEnabled && graphqlMetrics != nil {
		metricsHandler = middleware.GraphQLMetricsMiddleware(graphqlMetrics)(tracingHandler)
		logger.Info("GraphQL metrics middleware enabled")
	}

	// Middleware order: OIDC auth runs outermost, then DB role extraction.
	// DB role middleware must run after OIDC because it reads claims from the
	// validated JWT token that OIDC places in context. The chain is:
	//   request -> logging -> OIDC auth -> DB role -> mutation tx -> metrics -> tracing -> batching -> graphql
	baseHandler := metricsHandler
	if executor != nil {
		baseHandler = middleware.MutationTransactionMiddleware(executor)(baseHandler)
		logger.Info("mutation transaction middleware enabled")
	}
	dbRoleHandler := baseHandler
	if cfg.Server.Auth.DBRoleEnabled {
		dbRoleHandler = middleware.DBRoleMiddleware(cfg.Server.Auth.DBRoleClaimName, cfg.Server.Auth.DBRoleValidationEnabled, availableRoles)(baseHandler)
		logger.Info("database role middleware enabled")
	}

	authHandler := dbRoleHandler
	if cfg.Server.Auth.OIDCEnabled {
		authMiddleware, err := middleware.OIDCAuthMiddleware(oidcAuthConfig(cfg), logger, securityMetrics)
		if err != nil {
			return nil, err
		}
		authHandler = authMiddleware(dbRoleHandler)
		logger.Info("OIDC auth middleware enabled")
	}

	return middleware.LoggingMiddleware(logger)(authHandler), nil
}

func buildAdminHandler(cfg *config.Config, logger *logging.Logger, manager *schemarefresh.Manager, securityMetrics *observability.SecurityMetrics) (http.Handler, error) {
	var adminHandler http.Handler = http.HandlerFunc(schemaReloadHandler(manager, securityMetrics))
	if cfg.Server.Auth.OIDCEnabled {
		adminAuthMiddleware, err := middleware.OIDCAuthMiddleware(oidcAuthConfig(cfg), logger, securityMetrics)
		if err != nil {
			return nil, err
		}
		adminHandler = adminAuthMiddleware(adminHandler)
		logger.Info("admin endpoints require authentication")
	} else {
		logger.Warn("admin endpoints are not authenticated - consider enabling OIDC authentication")
	}
	return adminHandler, nil
}

func buildRouter(cfg *config.Config, logger *logging.Logger, db *sql.DB, graphqlHandler http.Handler, adminHandler http.Handler, meterProvider *observability.MeterProvider) *http.ServeMux {
	mux := http.NewServeMux()
	mux.Handle("/graphql", graphqlHandler)
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/" {
			http.Redirect(w, r, "/graphql", http.StatusFound)
			return
		}
		http.NotFound(w, r)
	})

	mux.HandleFunc("/health", healthHandler(db, cfg.Server.HealthCheckTimeout))
	mux.Handle("/admin/reload-schema", adminHandler)

	if cfg.Observability.MetricsEnabled && meterProvider != nil {
		mux.Handle("/metrics", promhttp.Handler())
		logger.Info("metrics endpoint enabled", slog.String("path", "/metrics"))
	}

	return mux
}

func wrapHTTPHandler(cfg *config.Config, logger *logging.Logger, handler http.Handler) http.Handler {
	if cfg.Observability.MetricsEnabled || cfg.Observability.TracingEnabled {
		handler = otelhttp.NewHandler(handler, "http.server",
			otelhttp.WithSpanNameFormatter(func(_ string, r *http.Request) string {
				return httpRootSpanName(r)
			}),
			otelhttp.WithMessageEvents(otelhttp.ReadEvents, otelhttp.WriteEvents),
		)
		logger.Info("HTTP instrumentation enabled")
	}

	if cfg.Server.CORSEnabled {
		handler = middleware.CORSMiddleware(middleware.CORSConfig{
			Enabled:          cfg.Server.CORSEnabled,
			AllowedOrigins:   cfg.Server.CORSAllowedOrigins,
			AllowedMethods:   cfg.Server.CORSAllowedMethods,
			AllowedHeaders:   cfg.Server.CORSAllowedHeaders,
			ExposeHeaders:    cfg.Server.CORSExposeHeaders,
			AllowCredentials: cfg.Server.CORSAllowCredentials,
			MaxAge:           cfg.Server.CORSMaxAge,
		})(handler)
	}

	if cfg.Server.RateLimitEnabled {
		handler = middleware.RateLimitMiddleware(middleware.RateLimitConfig{
			Enabled: cfg.Server.RateLimitEnabled,
			RPS:     cfg.Server.RateLimitRPS,
			Burst:   cfg.Server.RateLimitBurst,
		})(handler)
	}

	return handler
}

func httpRootSpanName(r *http.Request) string {
	if r == nil {
		return "HTTP /*"
	}

	method := strings.TrimSpace(r.Method)
	if method == "" {
		method = "HTTP"
	}

	return method + " " + normalizeHTTPSpanRoute(r.URL.Path)
}

func normalizeHTTPSpanRoute(rawPath string) string {
	switch rawPath {
	case "/", "/graphql", "/health", "/metrics", "/admin/reload-schema":
		return rawPath
	default:
		return "/*"
	}
}

func buildServer(cfg *config.Config, logger *logging.Logger, handler http.Handler, serverAddr string) (*http.Server, tlscert.Manager, error) {
	srv := &http.Server{
		Addr:         serverAddr,
		Handler:      handler,
		ReadTimeout:  cfg.Server.ReadTimeout,
		WriteTimeout: cfg.Server.WriteTimeout,
		IdleTimeout:  cfg.Server.IdleTimeout,
	}

	var tlsManager tlscert.Manager
	tlsEnabled := cfg.Server.TLSMode != "" && cfg.Server.TLSMode != "off"
	if tlsEnabled {
		// Map tls_mode to tlscert.CertMode
		var certMode tlscert.CertMode
		switch cfg.Server.TLSMode {
		case "auto":
			certMode = tlscert.CertModeSelfSigned
		case "file":
			certMode = tlscert.CertModeFile
		default:
			certMode = tlscert.CertMode(cfg.Server.TLSMode)
		}

		tlsConfig := tlscert.Config{
			Mode:              certMode,
			CertFile:          cfg.Server.TLSCertFile,
			KeyFile:           cfg.Server.TLSKeyFile,
			SelfSignedCertDir: cfg.Server.TLSAutoCertDir,
			SelfSignedHosts:   []string{"localhost", "127.0.0.1", "::1"},
		}

		var err error
		tlsManager, err = tlscert.NewManager(tlsConfig, logger.Logger)
		if err != nil {
			return nil, nil, err
		}

		srv.TLSConfig, err = tlsManager.GetTLSConfig()
		if err != nil {
			return nil, nil, err
		}

		logger.Info("TLS enabled",
			slog.String("mode", cfg.Server.TLSMode),
			slog.String("cert_source", tlsManager.Description()))
	}

	return srv, tlsManager, nil
}

func startServer(cfg *config.Config, logger *logging.Logger, srv *http.Server, serverAddr string) chan error {
	serverErrors := make(chan error, 1)
	tlsEnabled := cfg.Server.TLSMode != "" && cfg.Server.TLSMode != "off"
	go func() {
		protocol := "http"
		if tlsEnabled {
			protocol = "https"
		}

		logAttrs := []any{
			slog.String("protocol", protocol),
			slog.String("address", serverAddr),
			slog.String("graphql_endpoint", "/graphql"),
			slog.String("health_endpoint", "/health"),
			slog.Int("graphql_max_depth", cfg.Server.GraphQLMaxDepth),
			slog.String("log_level", cfg.Observability.Logging.Level),
			slog.String("log_format", cfg.Observability.Logging.Format),
		}

		if cfg.Observability.MetricsEnabled {
			logAttrs = append(logAttrs, slog.String("metrics_endpoint", "/metrics"))
		}

		if cfg.Server.RateLimitEnabled {
			logAttrs = append(logAttrs,
				slog.Float64("rate_limit_rps", cfg.Server.RateLimitRPS),
				slog.Int("rate_limit_burst", cfg.Server.RateLimitBurst),
			)
		}

		if tlsEnabled {
			logAttrs = append(logAttrs,
				slog.Bool("tls_enabled", true),
				slog.String("tls_mode", cfg.Server.TLSMode))
		} else {
			logAttrs = append(logAttrs, slog.Bool("tls_enabled", false))
		}

		logger.Info("server starting", logAttrs...)

		var err error
		if tlsEnabled {
			err = srv.ListenAndServeTLS("", "")
		} else {
			err = srv.ListenAndServe()
		}

		if err != nil && err != http.ErrServerClosed {
			serverErrors <- fmt.Errorf("server failed: %w", err)
		}
	}()
	return serverErrors
}

// healthHandler returns an HTTP handler for health checks
func healthHandler(db *sql.DB, timeout time.Duration) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Get logger from context (with request ID if available)
		reqLogger := logging.FromContext(r.Context())

		// Set JSON content type
		w.Header().Set("Content-Type", "application/json")

		// Check database connectivity with a short timeout
		ctx, cancel := context.WithTimeout(r.Context(), timeout)
		defer cancel()

		if err := db.PingContext(ctx); err != nil {
			reqLogger.Error("health check failed",
				slog.String("error", err.Error()),
				slog.String("check", "database"),
			)
			w.WriteHeader(http.StatusServiceUnavailable)
			// Return generic error message to avoid leaking internal details
			_, _ = fmt.Fprint(w, `{"status":"unhealthy","database":"failed"}`)
			return
		}

		reqLogger.Debug("health check passed")
		w.WriteHeader(http.StatusOK)
		_, _ = fmt.Fprint(w, `{"status":"healthy","database":"ok"}`)
	}
}

func schemaReloadHandler(manager *schemarefresh.Manager, securityMetrics *observability.SecurityMetrics) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		reqLogger := logging.FromContext(r.Context())
		w.Header().Set("Content-Type", "application/json")

		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			_, _ = fmt.Fprint(w, `{"error":"method not allowed"}`)
			return
		}

		// Check if user is authenticated
		authCtx, authenticated := middleware.AuthFromContext(r.Context())

		// Log admin operation with authentication context if available
		logAttrs := []any{
			slog.String("operation", "schema_reload"),
			slog.String("remote_addr", r.RemoteAddr),
			slog.Bool("authenticated", authenticated),
		}
		if authenticated {
			logAttrs = append(logAttrs,
				slog.String("authenticated_user", authCtx.Subject),
				slog.String("issuer", authCtx.Issuer),
			)
		}
		reqLogger.Info("admin endpoint accessed", logAttrs...)

		refreshCtx, refreshCancel := context.WithTimeout(r.Context(), 15*time.Second)
		defer refreshCancel()

		if err := manager.RefreshNowContext(refreshCtx); err != nil {
			// Record failed admin operation
			if securityMetrics != nil {
				securityMetrics.RecordAdminEndpointAccess(r.Context(), "schema_reload", authenticated, false)
			}
			reqLogger.Error("schema reload failed", slog.String("error", err.Error()))
			w.WriteHeader(http.StatusInternalServerError)
			// Return generic error message to avoid leaking internal details
			_, _ = fmt.Fprint(w, `{"status":"error","message":"schema reload failed"}`)
			return
		}

		// Record successful admin operation
		if securityMetrics != nil {
			securityMetrics.RecordAdminEndpointAccess(r.Context(), "schema_reload", authenticated, true)
		}

		reqLogger.Info("schema reloaded successfully", logAttrs...)
		w.WriteHeader(http.StatusOK)
		_, _ = fmt.Fprint(w, `{"status":"ok"}`)
	}
}
