package resolver

import (
	"context"
	"sync"

	"tidb-graphql/internal/dbexec"
)

type mutationContextKey struct{}

// MutationContext holds a shared transaction for a mutation operation.
type MutationContext struct {
	tx       dbexec.TxExecutor
	hasError bool
	mu       sync.Mutex
}

func NewMutationContext(tx dbexec.TxExecutor) *MutationContext {
	return &MutationContext{tx: tx}
}

func (mc *MutationContext) Tx() dbexec.TxExecutor {
	return mc.tx
}

func (mc *MutationContext) MarkError() {
	mc.mu.Lock()
	mc.hasError = true
	mc.mu.Unlock()
}

func (mc *MutationContext) Finalize() error {
	mc.mu.Lock()
	hasError := mc.hasError
	mc.mu.Unlock()

	if hasError {
		return mc.tx.Rollback()
	}
	return mc.tx.Commit()
}

func WithMutationContext(ctx context.Context, mc *MutationContext) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, mutationContextKey{}, mc)
}

func MutationContextFromContext(ctx context.Context) *MutationContext {
	if ctx == nil {
		return nil
	}
	mc, _ := ctx.Value(mutationContextKey{}).(*MutationContext)
	return mc
}
