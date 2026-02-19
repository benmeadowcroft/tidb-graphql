package serverapp

import (
	"fmt"
	"log/slog"
	"os"
)

// Start launches the HTTP server goroutine. It requires Init to have completed.
func (a *App) Start() (<-chan error, error) {
	a.stateMu.Lock()
	defer a.stateMu.Unlock()

	if !a.initialized {
		return nil, fmt.Errorf("app is not initialized")
	}
	if a.started {
		return a.serverErrors, nil
	}

	a.serverErrors = startServer(a.cfg, a.logger, a.srv, a.serverAddr)
	a.started = true
	return a.serverErrors, nil
}

// WaitForStop waits for either an OS signal or a server error.
func (a *App) WaitForStop(stop <-chan os.Signal, serverErrors <-chan error) (reason string, err error) {
	if serverErrors == nil {
		a.stateMu.Lock()
		serverErrors = a.serverErrors
		a.stateMu.Unlock()
	}

	if stop == nil && serverErrors == nil {
		return "", fmt.Errorf("both stop and serverErrors channels are nil")
	}
	if stop == nil {
		err := <-serverErrors
		if err == nil {
			return "server_error", fmt.Errorf("server stopped unexpectedly")
		}
		return "server_error", fmt.Errorf("server failed: %w", err)
	}
	if serverErrors == nil {
		sig := <-stop
		if a.logger != nil {
			a.logger.Info("received shutdown signal", slog.String("signal", sig.String()))
		}
		return "signal", nil
	}

	select {
	case err := <-serverErrors:
		if err == nil {
			return "server_error", fmt.Errorf("server stopped unexpectedly")
		}
		return "server_error", fmt.Errorf("server failed: %w", err)
	case sig := <-stop:
		if a.logger != nil {
			a.logger.Info("received shutdown signal", slog.String("signal", sig.String()))
		}
		return "signal", nil
	}
}
