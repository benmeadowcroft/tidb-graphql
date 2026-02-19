package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"tidb-graphql/internal/config"
	"tidb-graphql/internal/serverapp"

	"github.com/spf13/pflag"
)

var (
	// Version is set at build time via -ldflags "-X main.Version=...".
	Version = "dev"
	Commit  = "none"
)

func main() {
	if err := run(); err != nil {
		slog.Error("server error", slog.String("error", err.Error()))
		os.Exit(1)
	}
}

func run() error {
	pflag.Bool("version", false, "Print version and exit")

	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("failed to load configuration: %w", err)
	}

	if showVersion, _ := pflag.CommandLine.GetBool("version"); showVersion {
		fmt.Printf("tidb-graphql %s (%s)\n", Version, Commit)
		return nil
	}

	if cfg.Observability.ServiceVersion == "" {
		cfg.Observability.ServiceVersion = Version
	}

	validationResult := cfg.Validate()
	for _, warn := range validationResult.Warnings {
		slog.Warn("configuration warning",
			slog.String("field", warn.Field),
			slog.String("message", warn.Message),
			slog.String("hint", warn.Hint),
		)
	}
	if validationResult.HasErrors() {
		for _, err := range validationResult.Errors {
			slog.Error("configuration error",
				slog.String("field", err.Field),
				slog.String("message", err.Message),
				slog.String("hint", err.Hint),
			)
		}
		return fmt.Errorf("configuration validation failed")
	}

	logger, loggerProvider, err := serverapp.InitLogger(cfg)
	if err != nil {
		return fmt.Errorf("failed to initialize logging: %w", err)
	}

	app, err := serverapp.New(cfg, logger)
	if err != nil {
		if loggerProvider != nil {
			_ = loggerProvider.Shutdown(context.Background(), logger.Logger)
		}
		return err
	}
	app.AttachLoggerProvider(loggerProvider)

	if err := app.Init(context.Background()); err != nil {
		return err
	}

	serverErrors, err := app.Start()
	if err != nil {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.Server.ShutdownTimeout)
		defer cancel()
		_ = app.Shutdown(shutdownCtx)
		return err
	}

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(stop)

	_, waitErr := app.WaitForStop(stop, serverErrors)

	logger.Info("shutting down server gracefully")
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), cfg.Server.ShutdownTimeout)
	shutdownErr := app.Shutdown(shutdownCtx)
	shutdownCancel()

	if waitErr != nil {
		return waitErr
	}
	if shutdownErr != nil {
		return shutdownErr
	}

	logger.Info("server stopped gracefully")
	return nil
}
