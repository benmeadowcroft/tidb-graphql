package serverapp

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

const (
	startupSpanInit         = "startup.init"
	startupSpanDBConnect    = "startup.db_connect"
	startupSpanDBVerify     = "startup.db_verify"
	startupSpanRoleDiscover = "startup.role_discover"
	startupSpanRoleValidate = "startup.role_validate"
)

// Init initializes all runtime resources. It is idempotent.
func (a *App) Init(ctx context.Context) error {
	a.stateMu.Lock()
	if a.initialized {
		a.stateMu.Unlock()
		return nil
	}
	a.stateMu.Unlock()

	if ctx == nil {
		ctx = context.Background()
	}

	cleanup := cleanupStack{}
	success := false
	defer func() {
		if !success {
			cleanup.run(context.Background(), a.logger)
		}
	}()

	if a.loggerProvider != nil {
		cleanup.push("logger provider", func(shutdownCtx context.Context) error {
			return a.loggerProvider.Shutdown(shutdownCtx, a.logger.Logger)
		})
	}

	meterProvider, graphqlMetrics, schemaRefreshMetrics, securityMetrics, err := initMetrics(a.cfg, a.logger)
	if err != nil {
		return fmt.Errorf("failed to initialize OpenTelemetry metrics: %w", err)
	}
	if meterProvider != nil {
		cleanup.push("meter provider", func(shutdownCtx context.Context) error {
			return meterProvider.Shutdown(shutdownCtx, a.logger.Logger)
		})
	}

	tracerProvider, err := initTracing(a.cfg, a.logger)
	if err != nil {
		return fmt.Errorf("failed to initialize OpenTelemetry tracing: %w", err)
	}
	if tracerProvider != nil {
		cleanup.push("tracer provider", func(shutdownCtx context.Context) error {
			return tracerProvider.Shutdown(shutdownCtx, a.logger.Logger)
		})
	}
	startupCtx, startupSpan := startStartupSpan(ctx, startupSpanInit)
	defer func() {
		if !success {
			startupSpan.SetAttributes(attribute.Bool("startup.success", false))
		}
		startupSpan.End()
	}()

	a.logger.Info("connecting to TiDB",
		slog.String("host", a.cfg.Database.Host),
		slog.Int("port", a.cfg.Database.Port),
		slog.String("database_effective", a.effectiveDatabase),
		slog.String("database_source", a.databaseSource),
		slog.Bool("dsn_present", a.dsnPresent),
	)

	var db *sql.DB
	var dbStatsReg interface{ Unregister() error }
	if err := runStartupPhase(startupCtx, startupSpanDBConnect, func(context.Context) error {
		var connectErr error
		db, dbStatsReg, connectErr = connectDB(a.cfg, a.logger)
		if connectErr != nil {
			return fmt.Errorf("failed to connect to database: %w", connectErr)
		}
		return nil
	}); err != nil {
		return err
	}
	cleanup.push("database", func(_ context.Context) error {
		if dbStatsReg != nil {
			if err := dbStatsReg.Unregister(); err != nil {
				a.logger.Warn("failed to unregister DB stats metrics", slog.String("error", err.Error()))
			}
		}
		return db.Close()
	})

	if err := runStartupPhase(startupCtx, startupSpanDBVerify, func(phaseCtx context.Context) error {
		if verifyErr := configureDatabase(phaseCtx, a.cfg, a.logger, db, a.effectiveDatabase, a.databaseSource, a.dsnPresent); verifyErr != nil {
			return fmt.Errorf("failed to verify database connection: %w", verifyErr)
		}
		return nil
	}); err != nil {
		return err
	}

	limits := buildPlanLimits(a.cfg)

	var availableRoles []string
	if a.cfg.Server.Auth.DBRoleEnabled {
		if err := runStartupPhase(startupCtx, startupSpanRoleDiscover, func(phaseCtx context.Context) error {
			var discoverErr error
			availableRoles, discoverErr = discoverRoles(phaseCtx, db, a.logger)
			if discoverErr != nil {
				return fmt.Errorf("failed to discover database roles: %w", discoverErr)
			}
			return nil
		}); err != nil {
			return err
		}
		availableRoles, err = resolveRoleSchemaTargets(
			availableRoles,
			a.cfg.Server.Auth.RoleSchemaInclude,
			a.cfg.Server.Auth.RoleSchemaExclude,
			a.cfg.Server.Auth.RoleSchemaMaxRoles,
		)
		if err != nil {
			return fmt.Errorf("failed to resolve role schema targets: %w", err)
		}
		a.logger.Info("selected role schema targets",
			slog.Int("count", len(availableRoles)),
			slog.Any("roles", availableRoles),
		)

		if err := runStartupPhase(startupCtx, startupSpanRoleValidate, func(phaseCtx context.Context) error {
			if validateErr := validateDBRolePrivileges(phaseCtx, db, a.effectiveDatabase, a.logger); validateErr != nil {
				return fmt.Errorf("failed to validate database role privileges: %w", validateErr)
			}
			return nil
		}); err != nil {
			return err
		}
	}

	queryExecutor := buildQueryExecutor(a.cfg, db, availableRoles, a.effectiveDatabase)
	manager, schemaCancel, err := startSchemaManager(startupCtx, a.cfg, a.logger, db, limits, schemaRefreshMetrics, queryExecutor, a.effectiveDatabase, availableRoles)
	if err != nil {
		return fmt.Errorf("failed to initialize schema refresh manager: %w", err)
	}
	cleanup.push("schema manager", func(shutdownCtx context.Context) error {
		schemaCancel()
		return manager.Wait(shutdownCtx)
	})

	graphqlHandler, err := buildGraphQLHandler(a.cfg, a.logger, manager, graphqlMetrics, securityMetrics, queryExecutor, availableRoles)
	if err != nil {
		return fmt.Errorf("failed to initialize GraphQL handler: %w", err)
	}

	adminHandler, err := buildAdminHandler(a.cfg, a.logger, manager, securityMetrics)
	if err != nil {
		return fmt.Errorf("failed to initialize admin handler: %w", err)
	}

	mux := buildRouter(a.cfg, a.logger, db, graphqlHandler, adminHandler, meterProvider)
	handler := wrapHTTPHandler(a.cfg, a.logger, mux)

	serverAddr := fmt.Sprintf(":%d", a.cfg.Server.Port)
	srv, tlsManager, err := buildServer(a.cfg, a.logger, handler, serverAddr)
	if err != nil {
		return fmt.Errorf("failed to initialize server: %w", err)
	}
	cleanup.push("HTTP server", func(shutdownCtx context.Context) error {
		return srv.Shutdown(shutdownCtx)
	})
	if tlsManager != nil {
		cleanup.push("TLS manager", func(_ context.Context) error {
			return tlsManager.Shutdown()
		})
	}

	a.stateMu.Lock()
	a.meterProvider = meterProvider
	a.graphqlMetrics = graphqlMetrics
	a.schemaRefreshMetrics = schemaRefreshMetrics
	a.securityMetrics = securityMetrics
	a.tracerProvider = tracerProvider
	a.db = db
	a.dbStatsReg = dbStatsReg
	a.limits = limits
	a.availableRoles = availableRoles
	a.queryExecutor = queryExecutor
	a.manager = manager
	a.schemaCancel = schemaCancel
	a.graphqlHandler = graphqlHandler
	a.adminHandler = adminHandler
	a.mux = mux
	a.handler = handler
	a.serverAddr = serverAddr
	a.srv = srv
	a.tlsManager = tlsManager
	a.cleanup = cleanup
	a.initialized = true
	a.stateMu.Unlock()

	startupSpan.SetAttributes(attribute.Bool("startup.success", true))
	success = true
	return nil
}

func startStartupSpan(ctx context.Context, name string) (context.Context, trace.Span) {
	if ctx == nil {
		ctx = context.Background()
	}
	tracer := otel.Tracer("tidb-graphql/startup")
	ctx, span := tracer.Start(ctx, name)
	return ctx, span
}

func runStartupPhase(ctx context.Context, name string, run func(context.Context) error) error {
	phaseCtx, span := startStartupSpan(ctx, name)
	defer span.End()

	if err := run(phaseCtx); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		span.SetAttributes(attribute.Bool("startup.success", false))
		return err
	}
	span.SetAttributes(attribute.Bool("startup.success", true))
	return nil
}
