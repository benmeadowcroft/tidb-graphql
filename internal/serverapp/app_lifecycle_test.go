package serverapp

import (
	"context"
	"errors"
	"net/http"
	"os"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"tidb-graphql/internal/config"
	"tidb-graphql/internal/logging"
	"tidb-graphql/internal/naming"
)

func testLogger() *logging.Logger {
	return logging.NewLogger(logging.Config{Level: "info", Format: "text"})
}

func TestWaitForStop_SignalWins(t *testing.T) {
	app := &App{logger: testLogger()}
	stop := make(chan os.Signal, 1)
	serverErrors := make(chan error, 1)

	stop <- syscall.SIGTERM

	reason, err := app.WaitForStop(stop, serverErrors)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if reason != "signal" {
		t.Fatalf("expected reason=signal, got %q", reason)
	}
}

func TestWaitForStop_ServerErrorWins(t *testing.T) {
	app := &App{logger: testLogger()}
	stop := make(chan os.Signal, 1)
	serverErrors := make(chan error, 1)
	serverErrors <- errors.New("boom")

	reason, err := app.WaitForStop(stop, serverErrors)
	if err == nil {
		t.Fatalf("expected error, got nil")
	}
	if reason != "server_error" {
		t.Fatalf("expected reason=server_error, got %q", reason)
	}
}

func TestShutdown_Idempotent(t *testing.T) {
	app := &App{logger: testLogger()}
	var calls int32
	app.cleanup.push("test", func(context.Context) error {
		atomic.AddInt32(&calls, 1)
		return nil
	})

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	if err := app.Shutdown(ctx); err != nil {
		t.Fatalf("first shutdown failed: %v", err)
	}
	if err := app.Shutdown(ctx); err != nil {
		t.Fatalf("second shutdown failed: %v", err)
	}

	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Fatalf("expected cleanup to run once, ran %d times", got)
	}
}

func TestStart_BeforeInit_Fails(t *testing.T) {
	app := &App{logger: testLogger()}
	if _, err := app.Start(); err == nil {
		t.Fatalf("expected start to fail before init")
	}
}

func TestStartAndShutdown_HappyPath(t *testing.T) {
	app := &App{
		cfg: &config.Config{
			Server: config.ServerConfig{TLSMode: "off"},
		},
		logger:     testLogger(),
		serverAddr: "127.0.0.1:0",
		srv: &http.Server{
			Addr:    "127.0.0.1:0",
			Handler: http.NewServeMux(),
		},
		initialized: true,
	}
	app.cleanup.push("HTTP server", func(ctx context.Context) error {
		return app.srv.Shutdown(ctx)
	})

	if _, err := app.Start(); err != nil {
		t.Fatalf("start failed: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := app.Shutdown(ctx); err != nil {
		t.Fatalf("shutdown failed: %v", err)
	}
}

func TestInitFailure_DoesNotMarkInitialized(t *testing.T) {
	appCfg := &config.Config{
		Database: config.DatabaseConfig{
			Host:     "127.0.0.1",
			Port:     1,
			User:     "root",
			Password: "invalid",
			Database: "test",
			TLS: config.DatabaseTLSConfig{
				Mode: "off",
			},
			Pool: config.PoolConfig{
				MaxOpen:     1,
				MaxIdle:     1,
				MaxLifetime: time.Second,
			},
			ConnectionTimeout:       0,
			ConnectionRetryInterval: 10 * time.Millisecond,
		},
		Server: config.ServerConfig{
			Port:                     18089,
			GraphQLDefaultLimit:      10,
			SchemaRefreshMinInterval: time.Second,
			SchemaRefreshMaxInterval: 2 * time.Second,
			Search: config.SearchConfig{
				VectorRequireIndex: true,
				VectorMaxTopK:      10,
			},
			ReadTimeout:        time.Second,
			WriteTimeout:       time.Second,
			IdleTimeout:        time.Second,
			ShutdownTimeout:    time.Second,
			HealthCheckTimeout: time.Second,
			TLSMode:            "off",
		},
		Observability: config.ObservabilityConfig{
			ServiceName:    "tidb-graphql",
			ServiceVersion: "test",
			Environment:    "test",
			Logging: config.LoggingConfig{
				Level:          "info",
				Format:         "text",
				ExportsEnabled: false,
			},
		},
		Naming: naming.DefaultConfig(),
		TypeMappings: config.TypeMappingsConfig{
			UUIDColumns:            map[string][]string{},
			TinyInt1BooleanColumns: map[string][]string{},
			TinyInt1IntColumns:     map[string][]string{},
		},
	}

	app, err := New(appCfg, testLogger())
	if err != nil {
		t.Fatalf("new failed: %v", err)
	}

	if err := app.Init(context.Background()); err == nil {
		t.Fatalf("expected init to fail with unreachable database")
	}

	app.stateMu.Lock()
	initialized := app.initialized
	app.stateMu.Unlock()
	if initialized {
		t.Fatalf("app should not be marked initialized after failed Init")
	}
}
