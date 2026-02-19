package resolver

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"

	"tidb-graphql/internal/dbexec"
	"tidb-graphql/internal/introspection"
	"tidb-graphql/internal/planner"
)

// connectionResult holds the data needed to resolve connection fields.
type connectionResult struct {
	rows     []map[string]interface{}
	plan     *planner.ConnectionPlan
	hasNext  bool
	hasPrev  bool
	executor dbexec.QueryExecutor
	countCtx context.Context
	// totalCount is lazily computed
	totalCountVal *int
	totalCountMu  sync.Mutex
	// aggregate results are cached per selection shape.
	aggregateVals map[string]map[string]interface{}
	aggregateMu   sync.Mutex
}

func (cr *connectionResult) totalCount() (int, error) {
	cr.totalCountMu.Lock()
	defer cr.totalCountMu.Unlock()

	if cr.totalCountVal != nil {
		return *cr.totalCountVal, nil
	}
	if cr.plan == nil || cr.executor == nil || cr.plan.Count.SQL == "" {
		count := 0
		cr.totalCountVal = &count
		return count, nil
	}

	rows, err := cr.executor.QueryContext(cr.countCtx, cr.plan.Count.SQL, cr.plan.Count.Args...)
	if err != nil {
		return 0, err
	}
	defer func() { _ = rows.Close() }()

	var count int
	if rows.Next() {
		if err := rows.Scan(&count); err != nil {
			return 0, err
		}
	}
	if err := rows.Err(); err != nil {
		return 0, err
	}

	cr.totalCountVal = &count
	return count, nil
}

func (cr *connectionResult) aggregate(selection planner.AggregateSelection) (map[string]interface{}, error) {
	columns := planner.BuildAggregateColumns(selection)
	cacheKey := aggregateColumnsKey(columns)

	cr.aggregateMu.Lock()
	if cached, ok := cr.aggregateVals[cacheKey]; ok {
		cr.aggregateMu.Unlock()
		return cached, nil
	}
	cr.aggregateMu.Unlock()

	// COUNT-only aggregate can be served directly from totalCount.
	if len(columns) == 1 {
		count, err := cr.totalCount()
		if err != nil {
			return nil, err
		}
		result := map[string]interface{}{"count": count}
		cr.aggregateMu.Lock()
		if existing, ok := cr.aggregateVals[cacheKey]; ok {
			cr.aggregateMu.Unlock()
			return existing, nil
		}
		cr.aggregateVals[cacheKey] = result
		cr.aggregateMu.Unlock()
		return result, nil
	}

	if cr.plan == nil || cr.plan.AggregateBase.SQL == "" || cr.executor == nil {
		count, err := cr.totalCount()
		if err != nil {
			return nil, err
		}
		result := map[string]interface{}{"count": count}
		cr.aggregateMu.Lock()
		if existing, ok := cr.aggregateVals[cacheKey]; ok {
			cr.aggregateMu.Unlock()
			return existing, nil
		}
		cr.aggregateVals[cacheKey] = result
		cr.aggregateMu.Unlock()
		return result, nil
	}

	aggregateQuery := planner.BuildConnectionAggregateSQL(cr.plan.AggregateBase, selection)
	rows, err := cr.executor.QueryContext(cr.countCtx, aggregateQuery.SQL, aggregateQuery.Args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	result, err := scanAggregateRow(rows, columns, cr.plan.Table)
	if err != nil {
		return nil, err
	}

	if count, ok := aggregateCountAsInt(result["count"]); ok {
		cr.totalCountMu.Lock()
		if cr.totalCountVal == nil {
			c := count
			cr.totalCountVal = &c
		}
		cr.totalCountMu.Unlock()
		result["count"] = count
	} else {
		count, err := cr.totalCount()
		if err != nil {
			return nil, err
		}
		result["count"] = count
	}

	cr.aggregateMu.Lock()
	if existing, ok := cr.aggregateVals[cacheKey]; ok {
		cr.aggregateMu.Unlock()
		return existing, nil
	}
	cr.aggregateVals[cacheKey] = result
	cr.aggregateMu.Unlock()

	return result, nil
}

func aggregateCountAsInt(value interface{}) (int, bool) {
	switch v := value.(type) {
	case int:
		return v, true
	case int64:
		return int(v), true
	case float64:
		return int(v), true
	default:
		return 0, false
	}
}

// buildConnectionResult constructs the map that connection field resolvers read from.
func (r *Resolver) buildConnectionResult(ctx context.Context, rows []map[string]interface{}, plan *planner.ConnectionPlan, hasNext bool, hasPrev bool) map[string]interface{} {
	if rows == nil {
		rows = []map[string]interface{}{}
	}
	countCtx := ctx
	if countCtx == nil {
		countCtx = context.Background()
	}
	// totalCount is lazy, so it should not fail just because the caller's
	// request context is canceled after rows have already been materialized.
	countCtx = context.WithoutCancel(countCtx)

	result := &connectionResult{
		rows:          rows,
		plan:          plan,
		hasNext:       hasNext,
		hasPrev:       hasPrev,
		executor:      r.executor,
		countCtx:      countCtx,
		aggregateVals: make(map[string]map[string]interface{}),
	}

	// Build edges
	edges := make([]map[string]interface{}, len(rows))
	for i, row := range rows {
		var c string
		if plan != nil {
			c = encodeCursorFromRow(row, plan)
		}
		edges[i] = map[string]interface{}{
			"cursor": c,
			"node":   row,
		}
	}

	// Build pageInfo
	var startCursor, endCursor interface{}
	if len(edges) > 0 {
		startCursor = edges[0]["cursor"]
		endCursor = edges[len(edges)-1]["cursor"]
	}

	connMap := map[string]interface{}{
		"edges": edges,
		"nodes": rows,
		"pageInfo": map[string]interface{}{
			"hasNextPage":     hasNext,
			"hasPreviousPage": hasPrev,
			"startCursor":     startCursor,
			"endCursor":       endCursor,
		},
		"__connectionResult": result,
	}

	return connMap
}

// scanAggregateRow scans a single row of aggregate results into a map.
// It uses the columns list from BuildAggregateColumns to ensure scan order
// matches SQL SELECT clause order exactly.
func scanAggregateRow(rows dbexec.Rows, columns []planner.AggregateColumn, table introspection.Table) (map[string]interface{}, error) {
	if !rows.Next() {
		// No rows means count is 0
		return map[string]interface{}{"count": int64(0)}, rows.Err()
	}

	// Build scan destinations in the SAME order as columns (which matches SQL SELECT order).
	// This ordering is guaranteed by using BuildAggregateColumns as the single source of truth.
	scanDests := make([]interface{}, len(columns))
	intValues := make([]sql.NullInt64, len(columns))
	floatValues := make([]sql.NullFloat64, len(columns))
	anyValues := make([]interface{}, len(columns))

	for i, col := range columns {
		switch col.ValueType {
		case planner.AggregateInt:
			scanDests[i] = &intValues[i]
		case planner.AggregateFloat:
			scanDests[i] = &floatValues[i]
		case planner.AggregateAny:
			scanDests[i] = &anyValues[i]
		}
	}

	if err := rows.Scan(scanDests...); err != nil {
		return nil, err
	}

	result := buildAggregateResult(columns, table, intValues, floatValues, anyValues)
	return result, rows.Err()
}

func scanAggregateRows(rows dbexec.Rows, columns []planner.AggregateColumn, table introspection.Table) (map[string]map[string]interface{}, error) {
	grouped := make(map[string]map[string]interface{})

	for rows.Next() {
		var groupKey interface{}

		scanDests := make([]interface{}, len(columns)+1)
		intValues := make([]sql.NullInt64, len(columns))
		floatValues := make([]sql.NullFloat64, len(columns))
		anyValues := make([]interface{}, len(columns))

		scanDests[0] = &groupKey
		for i, col := range columns {
			switch col.ValueType {
			case planner.AggregateInt:
				scanDests[i+1] = &intValues[i]
			case planner.AggregateFloat:
				scanDests[i+1] = &floatValues[i]
			case planner.AggregateAny:
				scanDests[i+1] = &anyValues[i]
			}
		}

		if err := rows.Scan(scanDests...); err != nil {
			return nil, err
		}

		grouped[fmt.Sprint(groupKey)] = buildAggregateResult(columns, table, intValues, floatValues, anyValues)
	}

	return grouped, rows.Err()
}

func buildAggregateResult(columns []planner.AggregateColumn, table introspection.Table, intValues []sql.NullInt64, floatValues []sql.NullFloat64, anyValues []interface{}) map[string]interface{} {
	result := map[string]interface{}{}
	groupedResults := map[string]map[string]interface{}{}

	for i, col := range columns {
		var value interface{}
		var hasValue bool

		switch col.ValueType {
		case planner.AggregateInt:
			if intValues[i].Valid {
				value = intValues[i].Int64
				hasValue = true
			}
		case planner.AggregateFloat:
			if floatValues[i].Valid {
				value = floatValues[i].Float64
				hasValue = true
			}
		case planner.AggregateAny:
			value = convertValue(anyValues[i])
			hasValue = true
		}

		// Handle plain count specially (no column name, goes directly in result)
		if col.ResultKey == "count" && col.ColumnName == "" {
			if hasValue {
				result["count"] = value
			} else {
				result["count"] = int64(0)
			}
			continue
		}

		if hasValue {
			if groupedResults[col.ResultKey] == nil {
				groupedResults[col.ResultKey] = map[string]interface{}{}
			}
			fieldName := graphQLFieldNameForColumn(table, col.ColumnName)
			groupedResults[col.ResultKey][fieldName] = value
		}
	}

	for key, values := range groupedResults {
		if len(values) > 0 {
			result[key] = values
		}
	}

	if _, ok := result["count"]; !ok {
		result["count"] = int64(0)
	}

	return result
}

func aggregateColumnsKey(columns []planner.AggregateColumn) string {
	if len(columns) == 0 {
		return ""
	}
	parts := make([]string, len(columns))
	for i, col := range columns {
		parts[i] = col.SQLClause
	}
	return strings.Join(parts, "|")
}
