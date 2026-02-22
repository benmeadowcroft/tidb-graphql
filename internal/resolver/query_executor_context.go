package resolver

import (
	"context"

	"tidb-graphql/internal/dbexec"
)

// queryContextExecutor is the minimal query surface used by read paths.
// Both dbexec.QueryExecutor and dbexec.TxExecutor satisfy this contract.
type queryContextExecutor interface {
	QueryContext(ctx context.Context, query string, args ...any) (dbexec.Rows, error)
}

// queryExecutorForContext returns the active mutation transaction when present,
// otherwise it falls back to the resolver's base executor.
func (r *Resolver) queryExecutorForContext(ctx context.Context) queryContextExecutor {
	if mc := MutationContextFromContext(ctx); mc != nil && mc.Tx() != nil {
		return mc.Tx()
	}
	return r.executor
}
