package resolver

import (
	"context"
	"errors"

	"tidb-graphql/internal/introspection"
	"tidb-graphql/internal/observability"
	"tidb-graphql/internal/planner"
)

// batchConnectionPlan carries the per-relationship metadata that is constant
// across all chunks and parents in a batch connection execution.
type batchConnectionPlan struct {
	table         introspection.Table
	selection     []introspection.Column
	orderBy       *planner.OrderBy
	orderByKey    string
	cursorCols    []introspection.Column
	first         int
	parentAliases []string
	relation      string
}

// errBatchSkip is a sentinel error returned by runBatchConnectionChunks when
// ErrNoPrimaryKey is encountered; the caller should return (nil, false, nil).
var errBatchSkip = errors.New("batch skip")

// runBatchConnectionChunks executes the shared chunk→scan→group→build loop
// used by tryBatchOneToManyConnection, tryBatchManyToManyConnection, and
// tryBatchEdgeListConnection.
//
// chunkFn plans a single chunk into a SQLQuery.
// groupFn groups scanned rows by their parent-alias key string.
// perParentFn builds the Count and AggregateBase SQLQuery for one parent;
//
//	its argument is whatever value the caller stored in parentValueByKey.
//
// Returns errBatchSkip if ErrNoPrimaryKey is encountered (caller returns
// (nil, false, nil)); returns a non-nil map otherwise.
func runBatchConnectionChunks(
	ctx context.Context,
	r *Resolver,
	bp batchConnectionPlan,
	chunkCount int,
	metrics *observability.GraphQLMetrics,
	chunkFn func() (planner.SQLQuery, error),
	groupFn func(results []map[string]interface{}) map[string][]map[string]interface{},
	perParentFn func(parentID string) (planner.SQLQuery, planner.SQLQuery, error),
) (map[string]map[string]interface{}, error) {
	groupedConnections := make(map[string]map[string]interface{})

	if metrics != nil {
		metrics.RecordBatchParentCount(ctx, int64(chunkCount), bp.relation)
	}
	planned, err := chunkFn()
	if err != nil {
		if errors.Is(err, planner.ErrNoPrimaryKey) {
			if metrics != nil {
				metrics.RecordBatchSkipped(ctx, bp.relation, "no_primary_key")
			}
			return nil, errBatchSkip
		}
		return nil, err
	}
	if planned.SQL == "" {
		return groupedConnections, nil
	}

	rows, err := r.executor.QueryContext(ctx, planned.SQL, planned.Args...)
	if err != nil {
		return nil, normalizeQueryError(err)
	}
	results, err := scanRowsWithExtras(rows, bp.selection, bp.parentAliases)
	rows.Close()
	if err != nil {
		return nil, err
	}
	if metrics != nil {
		metrics.RecordBatchResultRows(ctx, int64(len(results)), bp.relation)
	}

	grouped := groupFn(results)
	for parentID, parentRows := range grouped {
		hasNext := len(parentRows) > bp.first
		if hasNext {
			parentRows = parentRows[:bp.first]
		}

		countQuery, aggregateBase, err := perParentFn(parentID)
		if err != nil {
			return nil, err
		}

		plan := &planner.ConnectionPlan{
			Count:         countQuery,
			AggregateBase: aggregateBase,
			Table:         bp.table,
			Columns:       bp.selection,
			OrderBy:       bp.orderBy,
			OrderByKey:    bp.orderByKey,
			CursorColumns: bp.cursorCols,
			First:         bp.first,
			Mode:          planner.PaginationModeForward,
		}
		groupedConnections[parentID] = r.buildConnectionResult(ctx, parentRows, plan, hasNext, false)
	}

	return groupedConnections, nil
}
