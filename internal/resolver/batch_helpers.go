package resolver

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"tidb-graphql/internal/introspection"
	"tidb-graphql/internal/observability"
	"tidb-graphql/internal/planner"

	"github.com/graphql-go/graphql"
)

func seedBatchRows(p graphql.ResolveParams, rows []map[string]interface{}) {
	if len(rows) == 0 {
		return
	}
	state, ok := getBatchState(p.Context)
	if !ok {
		return
	}
	parentKey := parentKeyFromResolve(p)
	for _, row := range rows {
		row[batchParentKeyField] = parentKey
	}
	state.setParentRows(parentKey, rows)
}

func shouldBatchForwardConnection(args map[string]interface{}) bool {
	if hasConnectionCursorArg(args, "after") || hasConnectionCursorArg(args, "before") {
		return false
	}
	if args == nil {
		return true
	}
	if last, ok := args["last"]; ok && last != nil {
		return false
	}
	return true
}

func hasConnectionCursorArg(args map[string]interface{}, key string) bool {
	if args == nil {
		return false
	}
	raw, ok := args[key]
	if !ok || raw == nil {
		return false
	}
	if str, ok := raw.(string); ok {
		return str != ""
	}
	return true
}

func uniqueParentValues(rows []map[string]interface{}, key string) []interface{} {
	seen := make(map[string]struct{})
	values := make([]interface{}, 0, len(rows))

	for _, row := range rows {
		raw := row[key]
		if raw == nil {
			continue
		}
		normalized := fmt.Sprint(raw)
		if _, ok := seen[normalized]; ok {
			continue
		}
		seen[normalized] = struct{}{}
		values = append(values, raw)
	}

	return values
}

func uniqueParentTuples(rows []map[string]interface{}, keys []string) []planner.ParentTuple {
	if len(keys) == 0 {
		return nil
	}
	seen := make(map[string]struct{})
	tuples := make([]planner.ParentTuple, 0, len(rows))
	for _, row := range rows {
		values := make([]interface{}, len(keys))
		missing := false
		for i, key := range keys {
			value := row[key]
			if value == nil {
				missing = true
				break
			}
			values[i] = value
		}
		if missing {
			continue
		}
		tupleKey := tupleKeyFromValues(values)
		if _, ok := seen[tupleKey]; ok {
			continue
		}
		seen[tupleKey] = struct{}{}
		tuples = append(tuples, planner.ParentTuple{Values: values})
	}
	return tuples
}

func groupByField(rows []map[string]interface{}, fieldName string) map[string][]map[string]interface{} {
	grouped := make(map[string][]map[string]interface{})
	for _, row := range rows {
		key := fmt.Sprint(row[fieldName])
		grouped[key] = append(grouped[key], row)
	}
	return grouped
}

func groupByAlias(rows []map[string]interface{}, alias string) map[string][]map[string]interface{} {
	grouped := make(map[string][]map[string]interface{})
	for _, row := range rows {
		key := fmt.Sprint(row[alias])
		delete(row, alias)
		grouped[key] = append(grouped[key], row)
	}
	return grouped
}

func groupByAliases(rows []map[string]interface{}, aliases []string) map[string][]map[string]interface{} {
	grouped := make(map[string][]map[string]interface{})
	for _, row := range rows {
		values := make([]interface{}, len(aliases))
		for i, alias := range aliases {
			values[i] = row[alias]
			delete(row, alias)
		}
		key := tupleKeyFromValues(values)
		grouped[key] = append(grouped[key], row)
	}
	return grouped
}

func mergeGrouped(target, src map[string][]map[string]interface{}) {
	for key, rows := range src {
		target[key] = append(target[key], rows...)
	}
}

func chunkValues(values []interface{}, max int) [][]interface{} {
	if len(values) == 0 {
		return nil
	}
	if max <= 0 || len(values) <= max {
		return [][]interface{}{values}
	}
	chunks := make([][]interface{}, 0, (len(values)+max-1)/max)
	for start := 0; start < len(values); start += max {
		end := start + max
		if end > len(values) {
			end = len(values)
		}
		chunks = append(chunks, values[start:end])
	}
	return chunks
}

func chunkParentTuples(values []planner.ParentTuple, max int) [][]planner.ParentTuple {
	if len(values) == 0 {
		return nil
	}
	if max <= 0 || len(values) <= max {
		return [][]planner.ParentTuple{values}
	}
	chunks := make([][]planner.ParentTuple, 0, (len(values)+max-1)/max)
	for start := 0; start < len(values); start += max {
		end := start + max
		if end > len(values) {
			end = len(values)
		}
		chunks = append(chunks, values[start:end])
	}
	return chunks
}

func listBatchQueriesSaved(parentCount, chunkCount int) int64 {
	// For list batches, compare per-parent queries to chunked queries (1 per chunk).
	if parentCount <= 0 || chunkCount <= 0 {
		return 0
	}
	if saved := parentCount - chunkCount; saved > 0 {
		return int64(saved)
	}
	return 0
}

func columnsKey(columns []introspection.Column) string {
	if len(columns) == 0 {
		return ""
	}
	names := make([]string, len(columns))
	for i, col := range columns {
		names[i] = col.Name
	}
	sort.Strings(names)
	return strings.Join(names, ",")
}

func orderByKey(orderBy *planner.OrderBy) string {
	if orderBy == nil {
		return ""
	}
	return strings.Join(orderBy.Columns, ",") + ":" + strings.Join(orderBy.Directions, ",")
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

func firstGroupedRecord(grouped map[string][]map[string]interface{}, key interface{}) map[string]interface{} {
	return firstGroupedRecordByTuple(grouped, []interface{}{key})
}

func firstGroupedRecordByTuple(grouped map[string][]map[string]interface{}, values []interface{}) map[string]interface{} {
	if grouped == nil {
		return nil
	}
	rows := grouped[tupleKeyFromValues(values)]
	if len(rows) == 0 {
		return nil
	}
	return rows[0]
}

func tupleKeyFromValues(values []interface{}) string {
	return encodeCanonicalValue(values)
}

const batchParentKeyField = "__batch_parent_key"

var batchMaxInClause = 1000

const (
	relationOneToMany  = "one_to_many"
	relationManyToOne  = "many_to_one"
	relationManyToMany = "many_to_many"
	relationEdgeList   = "edge_list"
)

func parentKeyFromResolve(p graphql.ResolveParams) string {
	return fmt.Sprintf("%s|%s|%s", responsePathString(p.Info.Path), fieldNameWithAlias(p.Info.FieldASTs), stableArgsKey(p.Args))
}

func parentKeyFromSource(source interface{}) (string, bool) {
	row, ok := source.(map[string]interface{})
	if !ok {
		return "", false
	}
	key, ok := row[batchParentKeyField].(string)
	return key, ok
}

func graphQLMetricsFromContext(ctx context.Context) *observability.GraphQLMetrics {
	return observability.GraphQLMetricsFromContext(ctx)
}
