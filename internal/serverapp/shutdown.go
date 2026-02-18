package serverapp

import (
	"context"
	"log/slog"

	"tidb-graphql/internal/logging"
)

// cleanupStack manages shutdown functions in LIFO order.
// Resources are released in reverse order of acquisition.
type cleanupStack struct {
	items []cleanupItem
}

type cleanupItem struct {
	name string
	fn   func(context.Context) error
}

func (s *cleanupStack) push(name string, fn func(context.Context) error) {
	s.items = append(s.items, cleanupItem{name: name, fn: fn})
}

func (s *cleanupStack) run(ctx context.Context, logger *logging.Logger) {
	for i := len(s.items) - 1; i >= 0; i-- {
		item := s.items[i]
		if logger != nil {
			logger.Info("shutting down " + item.name)
		}
		if err := item.fn(ctx); err != nil {
			if logger != nil {
				logger.Warn("cleanup error",
					slog.String("component", item.name),
					slog.String("error", err.Error()),
				)
			}
		}
	}
}

// Shutdown gracefully releases all acquired resources. It is safe to call multiple times.
func (a *App) Shutdown(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}

	a.shutdownOnce.Do(func() {
		a.stateMu.Lock()
		cleanup := a.cleanup
		a.started = false
		a.stateMu.Unlock()

		cleanup.run(ctx, a.logger)
	})

	return nil
}
