package resolver

import (
	"errors"
	"fmt"
	"strings"

	"tidb-graphql/internal/introspection"
	"tidb-graphql/internal/planner"

	"github.com/graphql-go/graphql"
	"go.opentelemetry.io/otel/attribute"
)

func oneToManyMappingColumns(rel introspection.Relationship) (localColumn string, remoteColumn string, err error) {
	localColumns := rel.EffectiveLocalColumns()
	remoteColumns := rel.EffectiveRemoteColumns()
	if len(localColumns) != 1 || len(remoteColumns) != 1 {
		return "", "", fmt.Errorf("invalid one-to-many mapping for %s", rel.GraphQLFieldName)
	}
	return localColumns[0], remoteColumns[0], nil
}

func (r *Resolver) tryBatchOneToManyConnection(p graphql.ResolveParams, table introspection.Table, rel introspection.Relationship, pkValue interface{}) (map[string]interface{}, bool, error) {
	metrics := graphQLMetricsFromContext(p.Context)

	state, ok := getBatchState(p.Context)
	if !ok {
		if metrics != nil {
			metrics.RecordBatchSkipped(p.Context, relationOneToMany, "no_batch_state")
		}
		return nil, false, nil
	}

	parentKey, ok := parentKeyFromSource(p.Source)
	if !ok {
		if metrics != nil {
			metrics.RecordBatchSkipped(p.Context, relationOneToMany, "missing_parent_key")
		}
		return nil, false, nil
	}

	parentRows := state.getParentRows(parentKey)
	if len(parentRows) == 0 {
		if metrics != nil {
			metrics.RecordBatchSkipped(p.Context, relationOneToMany, "missing_parent_rows")
		}
		return nil, false, nil
	}

	relatedTable, err := r.findTable(rel.RemoteTable)
	if err != nil {
		return nil, true, fmt.Errorf("failed to find related table %s: %w", rel.RemoteTable, err)
	}

	pkCols := introspection.PrimaryKeyColumns(relatedTable)
	if len(pkCols) == 0 {
		if metrics != nil {
			metrics.RecordBatchSkipped(p.Context, relationOneToMany, "no_primary_key")
		}
		return nil, false, nil
	}

	field := firstFieldAST(p.Info.FieldASTs)
	if field == nil {
		return nil, true, fmt.Errorf("missing field AST")
	}
	localColumn, remoteColumn, err := oneToManyMappingColumns(rel)
	if err != nil {
		return nil, true, err
	}

	first, err := planner.ParseFirstWithDefault(p.Args, r.defaultLimit)
	if err != nil {
		return nil, true, err
	}

	orderBy, err := planner.ParseOrderBy(relatedTable, p.Args)
	if err != nil {
		return nil, true, err
	}
	if orderBy == nil {
		pkColNames := make([]string, len(pkCols))
		pkDirections := make([]string, len(pkCols))
		for i, col := range pkCols {
			pkColNames[i] = col.Name
			pkDirections[i] = "ASC"
		}
		orderBy = &planner.OrderBy{
			Columns:    pkColNames,
			Directions: pkDirections,
		}
	}

	var whereClause *planner.WhereClause
	if whereArg, ok := p.Args["where"]; ok {
		if whereMap, ok := whereArg.(map[string]interface{}); ok {
			whereClause, err = planner.BuildWhereClauseWithSchema(r.dbSchema, relatedTable, whereMap)
			if err != nil {
				return nil, true, err
			}
			if whereClause != nil {
				if err := planner.ValidateWhereClauseIndexes(r.dbSchema, relatedTable, whereClause); err != nil {
					return nil, true, err
				}
			}
		}
	}

	selection := planner.SelectedColumnsForConnection(relatedTable, field, p.Info.Fragments, orderBy)
	orderByKey := planner.OrderByKey(relatedTable, orderBy.Columns)
	cursorCols := planner.CursorColumns(relatedTable, orderBy)

	relKey := fmt.Sprintf(
		"%s|%s|%s|%s|%s|%s",
		table.Name,
		rel.RemoteTable,
		remoteColumn,
		orderByKey,
		columnsKey(selection),
		stableArgsKey(p.Args),
	)

	if cached := state.getConnectionRows(relKey); cached != nil {
		state.IncrementCacheHit()
		if metrics != nil {
			metrics.RecordBatchCacheHit(p.Context, relationOneToMany)
		}
		if result, ok := cached[fmt.Sprint(pkValue)]; ok {
			if nodes, ok := result["nodes"].([]map[string]interface{}); ok {
				seedBatchRows(p, nodes)
			}
			return result, true, nil
		}
		return r.buildConnectionResult(p.Context, nil, nil, false, false), true, nil
	}
	state.IncrementCacheMiss()
	if metrics != nil {
		metrics.RecordBatchCacheMiss(p.Context, relationOneToMany)
	}

	parentField := graphQLFieldNameForColumn(table, localColumn)
	parentValues := uniqueParentValues(parentRows, parentField)
	if len(parentValues) == 0 {
		state.setConnectionRows(relKey, map[string]map[string]interface{}{})
		return r.buildConnectionResult(p.Context, nil, nil, false, false), true, nil
	}
	// Keep original typed parent values so count queries can bind FK args correctly.
	parentValueByKey := make(map[string]interface{}, len(parentValues))
	for _, value := range parentValues {
		parentValueByKey[fmt.Sprint(value)] = value
	}

	chunks := chunkValues(parentValues, batchMaxInClause)
	if metrics != nil {
		metrics.RecordBatchQueriesSaved(p.Context, listBatchQueriesSaved(len(parentValues), len(chunks)), relationOneToMany)
	}

	bp := batchConnectionPlan{
		table:         relatedTable,
		selection:     selection,
		orderBy:       orderBy,
		orderByKey:    orderByKey,
		cursorCols:    cursorCols,
		first:         first,
		parentAliases: []string{planner.BatchParentAlias},
		relation:      relationOneToMany,
	}
	groupedConnections := make(map[string]map[string]interface{})
	for _, chunk := range chunks {
		partial, err := runBatchConnectionChunks(
			p.Context, r, bp, len(chunk), metrics,
			func() (planner.SQLQuery, error) {
				return planner.PlanOneToManyConnectionBatch(relatedTable, remoteColumn, selection, chunk, first, orderBy, whereClause)
			},
			func(results []map[string]interface{}) map[string][]map[string]interface{} {
				return groupByAlias(results, planner.BatchParentAlias)
			},
			func(parentID string) (planner.SQLQuery, planner.SQLQuery, error) {
				parentValue := parentValueByKey[parentID]
				count, err := planner.BuildOneToManyCountSQL(relatedTable, remoteColumn, parentValue, whereClause)
				if err != nil {
					return planner.SQLQuery{}, planner.SQLQuery{}, err
				}
				agg, err := planner.BuildOneToManyAggregateBaseSQL(relatedTable, remoteColumn, parentValue, whereClause)
				return count, agg, err
			},
		)
		if errors.Is(err, errBatchSkip) {
			return nil, false, nil
		}
		if err != nil {
			return nil, true, err
		}
		for k, v := range partial {
			groupedConnections[k] = v
		}
	}

	if len(groupedConnections) == 0 {
		state.setConnectionRows(relKey, map[string]map[string]interface{}{})
		return r.buildConnectionResult(p.Context, nil, nil, false, false), true, nil
	}
	state.setConnectionRows(relKey, groupedConnections)

	if result, ok := groupedConnections[fmt.Sprint(pkValue)]; ok {
		if nodes, ok := result["nodes"].([]map[string]interface{}); ok {
			seedBatchRows(p, nodes)
		}
		return result, true, nil
	}
	return r.buildConnectionResult(p.Context, nil, nil, false, false), true, nil
}

func (r *Resolver) tryBatchManyToManyConnection(p graphql.ResolveParams, table introspection.Table, rel introspection.Relationship, pkValues []interface{}) (map[string]interface{}, bool, error) {
	metrics := graphQLMetricsFromContext(p.Context)

	state, ok := getBatchState(p.Context)
	if !ok {
		if metrics != nil {
			metrics.RecordBatchSkipped(p.Context, relationManyToMany, "no_batch_state")
		}
		return nil, false, nil
	}

	parentKey, ok := parentKeyFromSource(p.Source)
	if !ok {
		if metrics != nil {
			metrics.RecordBatchSkipped(p.Context, relationManyToMany, "missing_parent_key")
		}
		return nil, false, nil
	}

	parentRows := state.getParentRows(parentKey)
	if len(parentRows) == 0 {
		if metrics != nil {
			metrics.RecordBatchSkipped(p.Context, relationManyToMany, "missing_parent_rows")
		}
		return nil, false, nil
	}

	relatedTable, err := r.findTable(rel.RemoteTable)
	if err != nil {
		return nil, true, fmt.Errorf("failed to find related table %s: %w", rel.RemoteTable, err)
	}

	pkCols := introspection.PrimaryKeyColumns(relatedTable)
	if len(pkCols) == 0 {
		if metrics != nil {
			metrics.RecordBatchSkipped(p.Context, relationManyToMany, "no_primary_key")
		}
		return nil, false, nil
	}

	field := firstFieldAST(p.Info.FieldASTs)
	if field == nil {
		return nil, true, fmt.Errorf("missing field AST")
	}

	first, err := planner.ParseFirstWithDefault(p.Args, r.defaultLimit)
	if err != nil {
		return nil, true, err
	}

	orderBy, err := planner.ParseOrderBy(relatedTable, p.Args)
	if err != nil {
		return nil, true, err
	}
	if orderBy == nil {
		pkColNames := make([]string, len(pkCols))
		pkDirections := make([]string, len(pkCols))
		for i, col := range pkCols {
			pkColNames[i] = col.Name
			pkDirections[i] = "ASC"
		}
		orderBy = &planner.OrderBy{
			Columns:    pkColNames,
			Directions: pkDirections,
		}
	}

	var whereClause *planner.WhereClause
	if whereArg, ok := p.Args["where"]; ok {
		if whereMap, ok := whereArg.(map[string]interface{}); ok {
			whereClause, err = planner.BuildWhereClauseQualifiedWithSchema(r.dbSchema, relatedTable, relatedTable.Name, whereMap)
			if err != nil {
				return nil, true, err
			}
			if whereClause != nil {
				if err := planner.ValidateWhereClauseIndexes(r.dbSchema, relatedTable, whereClause); err != nil {
					return nil, true, err
				}
			}
		}
	}

	selection := planner.SelectedColumnsForConnection(relatedTable, field, p.Info.Fragments, orderBy)
	orderByKey := planner.OrderByKey(relatedTable, orderBy.Columns)
	cursorCols := planner.CursorColumns(relatedTable, orderBy)
	localColumns := rel.EffectiveLocalColumns()
	junctionLocalColumns := rel.EffectiveJunctionLocalFKColumns()
	junctionRemoteColumns := rel.EffectiveJunctionRemoteFKColumns()
	remoteColumns := rel.EffectiveRemoteColumns()
	if len(localColumns) == 0 || len(localColumns) != len(pkValues) {
		return nil, true, fmt.Errorf("invalid many-to-many local key mapping")
	}
	if len(junctionLocalColumns) != len(localColumns) {
		return nil, true, fmt.Errorf("invalid many-to-many junction local key mapping")
	}
	if len(junctionRemoteColumns) != len(remoteColumns) {
		return nil, true, fmt.Errorf("invalid many-to-many junction remote key mapping")
	}
	currentParentTupleKey := tupleKeyFromValues(pkValues)

	relKey := fmt.Sprintf(
		"%s|%s|%s|%s|%s|%s|%s",
		table.Name,
		rel.RemoteTable,
		rel.JunctionTable,
		strings.Join(remoteColumns, ","),
		orderByKey,
		columnsKey(selection),
		stableArgsKey(p.Args),
	)

	if cached := state.getConnectionRows(relKey); cached != nil {
		state.IncrementCacheHit()
		if metrics != nil {
			metrics.RecordBatchCacheHit(p.Context, relationManyToMany)
		}
		if result, ok := cached[currentParentTupleKey]; ok {
			if nodes, ok := result["nodes"].([]map[string]interface{}); ok {
				seedBatchRows(p, nodes)
			}
			return result, true, nil
		}
		return r.buildConnectionResult(p.Context, nil, nil, false, false), true, nil
	}
	state.IncrementCacheMiss()
	if metrics != nil {
		metrics.RecordBatchCacheMiss(p.Context, relationManyToMany)
	}

	parentFields := make([]string, len(localColumns))
	for i, colName := range localColumns {
		parentFields[i] = graphQLFieldNameForColumn(table, colName)
	}
	parentTuples := uniqueParentTuples(parentRows, parentFields)
	if len(parentTuples) == 0 {
		state.setConnectionRows(relKey, map[string]map[string]interface{}{})
		return r.buildConnectionResult(p.Context, nil, nil, false, false), true, nil
	}
	parentValueByKey := make(map[string]planner.ParentTuple, len(parentTuples))
	for _, tuple := range parentTuples {
		parentValueByKey[tupleKeyFromValues(tuple.Values)] = tuple
	}

	chunks := chunkParentTuples(parentTuples, batchMaxInClause)
	if metrics != nil {
		metrics.RecordBatchQueriesSaved(p.Context, listBatchQueriesSaved(len(parentTuples), len(chunks)), relationManyToMany)
	}

	parentAliases := planner.BatchParentAliases(len(junctionLocalColumns))
	bp := batchConnectionPlan{
		table:         relatedTable,
		selection:     selection,
		orderBy:       orderBy,
		orderByKey:    orderByKey,
		cursorCols:    cursorCols,
		first:         first,
		parentAliases: parentAliases,
		relation:      relationManyToMany,
	}
	groupedConnections := make(map[string]map[string]interface{})
	for _, chunk := range chunks {
		partial, err := runBatchConnectionChunks(
			p.Context, r, bp, len(chunk), metrics,
			func() (planner.SQLQuery, error) {
				return planner.PlanManyToManyConnectionBatch(relatedTable, rel.JunctionTable, junctionLocalColumns, junctionRemoteColumns, remoteColumns, selection, chunk, first, orderBy, whereClause)
			},
			func(results []map[string]interface{}) map[string][]map[string]interface{} {
				return groupByAliases(results, parentAliases)
			},
			func(parentID string) (planner.SQLQuery, planner.SQLQuery, error) {
				tuple := parentValueByKey[parentID]
				count, err := planner.BuildManyToManyCountSQL(relatedTable, rel.JunctionTable, junctionLocalColumns, junctionRemoteColumns, remoteColumns, tuple.Values, whereClause)
				if err != nil {
					return planner.SQLQuery{}, planner.SQLQuery{}, err
				}
				agg, err := planner.BuildManyToManyAggregateBaseSQL(relatedTable, rel.JunctionTable, junctionLocalColumns, junctionRemoteColumns, remoteColumns, tuple.Values, whereClause)
				return count, agg, err
			},
		)
		if errors.Is(err, errBatchSkip) {
			return nil, false, nil
		}
		if err != nil {
			return nil, true, err
		}
		for k, v := range partial {
			groupedConnections[k] = v
		}
	}

	if len(groupedConnections) == 0 {
		state.setConnectionRows(relKey, map[string]map[string]interface{}{})
		return r.buildConnectionResult(p.Context, nil, nil, false, false), true, nil
	}
	state.setConnectionRows(relKey, groupedConnections)

	if result, ok := groupedConnections[currentParentTupleKey]; ok {
		if nodes, ok := result["nodes"].([]map[string]interface{}); ok {
			seedBatchRows(p, nodes)
		}
		return result, true, nil
	}
	return r.buildConnectionResult(p.Context, nil, nil, false, false), true, nil
}

func (r *Resolver) tryBatchEdgeListConnection(p graphql.ResolveParams, table introspection.Table, rel introspection.Relationship, pkValues []interface{}) (map[string]interface{}, bool, error) {
	metrics := graphQLMetricsFromContext(p.Context)

	state, ok := getBatchState(p.Context)
	if !ok {
		if metrics != nil {
			metrics.RecordBatchSkipped(p.Context, relationEdgeList, "no_batch_state")
		}
		return nil, false, nil
	}

	parentKey, ok := parentKeyFromSource(p.Source)
	if !ok {
		if metrics != nil {
			metrics.RecordBatchSkipped(p.Context, relationEdgeList, "missing_parent_key")
		}
		return nil, false, nil
	}

	parentRows := state.getParentRows(parentKey)
	if len(parentRows) == 0 {
		if metrics != nil {
			metrics.RecordBatchSkipped(p.Context, relationEdgeList, "missing_parent_rows")
		}
		return nil, false, nil
	}

	junctionTable, err := r.findTable(rel.JunctionTable)
	if err != nil {
		return nil, true, fmt.Errorf("failed to find junction table %s: %w", rel.JunctionTable, err)
	}

	pkCols := introspection.PrimaryKeyColumns(junctionTable)
	if len(pkCols) == 0 {
		if metrics != nil {
			metrics.RecordBatchSkipped(p.Context, relationEdgeList, "no_primary_key")
		}
		return nil, false, nil
	}

	field := firstFieldAST(p.Info.FieldASTs)
	if field == nil {
		return nil, true, fmt.Errorf("missing field AST")
	}

	first, err := planner.ParseFirstWithDefault(p.Args, r.defaultLimit)
	if err != nil {
		return nil, true, err
	}

	orderBy, err := planner.ParseOrderBy(junctionTable, p.Args)
	if err != nil {
		return nil, true, err
	}
	if orderBy == nil {
		pkColNames := make([]string, len(pkCols))
		pkDirections := make([]string, len(pkCols))
		for i, col := range pkCols {
			pkColNames[i] = col.Name
			pkDirections[i] = "ASC"
		}
		orderBy = &planner.OrderBy{
			Columns:    pkColNames,
			Directions: pkDirections,
		}
	}

	var whereClause *planner.WhereClause
	if whereArg, ok := p.Args["where"]; ok {
		if whereMap, ok := whereArg.(map[string]interface{}); ok {
			whereClause, err = planner.BuildWhereClauseWithSchema(r.dbSchema, junctionTable, whereMap)
			if err != nil {
				return nil, true, err
			}
			if whereClause != nil {
				if err := planner.ValidateWhereClauseIndexes(r.dbSchema, junctionTable, whereClause); err != nil {
					return nil, true, err
				}
			}
		}
	}

	selection := planner.SelectedColumnsForConnection(junctionTable, field, p.Info.Fragments, orderBy)
	orderByKey := planner.OrderByKey(junctionTable, orderBy.Columns)
	cursorCols := planner.CursorColumns(junctionTable, orderBy)
	localColumns := rel.EffectiveLocalColumns()
	junctionLocalColumns := rel.EffectiveJunctionLocalFKColumns()
	if len(localColumns) == 0 || len(localColumns) != len(pkValues) {
		return nil, true, fmt.Errorf("invalid edge-list local key mapping")
	}
	if len(junctionLocalColumns) != len(localColumns) {
		return nil, true, fmt.Errorf("invalid edge-list junction local key mapping")
	}
	currentParentTupleKey := tupleKeyFromValues(pkValues)

	relKey := fmt.Sprintf(
		"%s|%s|%s|%s|%s|%s",
		table.Name,
		rel.JunctionTable,
		strings.Join(junctionLocalColumns, ","),
		orderByKey,
		columnsKey(selection),
		stableArgsKey(p.Args),
	)

	if cached := state.getConnectionRows(relKey); cached != nil {
		state.IncrementCacheHit()
		if metrics != nil {
			metrics.RecordBatchCacheHit(p.Context, relationEdgeList)
		}
		if result, ok := cached[currentParentTupleKey]; ok {
			if nodes, ok := result["nodes"].([]map[string]interface{}); ok {
				seedBatchRows(p, nodes)
			}
			return result, true, nil
		}
		return r.buildConnectionResult(p.Context, nil, nil, false, false), true, nil
	}
	state.IncrementCacheMiss()
	if metrics != nil {
		metrics.RecordBatchCacheMiss(p.Context, relationEdgeList)
	}

	parentFields := make([]string, len(localColumns))
	for i, colName := range localColumns {
		parentFields[i] = graphQLFieldNameForColumn(table, colName)
	}
	parentTuples := uniqueParentTuples(parentRows, parentFields)
	if len(parentTuples) == 0 {
		state.setConnectionRows(relKey, map[string]map[string]interface{}{})
		return r.buildConnectionResult(p.Context, nil, nil, false, false), true, nil
	}
	parentValueByKey := make(map[string]planner.ParentTuple, len(parentTuples))
	for _, tuple := range parentTuples {
		parentValueByKey[tupleKeyFromValues(tuple.Values)] = tuple
	}

	chunks := chunkParentTuples(parentTuples, batchMaxInClause)
	if metrics != nil {
		metrics.RecordBatchQueriesSaved(p.Context, listBatchQueriesSaved(len(parentTuples), len(chunks)), relationEdgeList)
	}

	parentAliases := planner.BatchParentAliases(len(junctionLocalColumns))
	bp := batchConnectionPlan{
		table:         junctionTable,
		selection:     selection,
		orderBy:       orderBy,
		orderByKey:    orderByKey,
		cursorCols:    cursorCols,
		first:         first,
		parentAliases: parentAliases,
		relation:      relationEdgeList,
	}
	groupedConnections := make(map[string]map[string]interface{})
	for _, chunk := range chunks {
		partial, err := runBatchConnectionChunks(
			p.Context, r, bp, len(chunk), metrics,
			func() (planner.SQLQuery, error) {
				return planner.PlanEdgeListConnectionBatch(junctionTable, junctionLocalColumns, selection, chunk, first, orderBy, whereClause)
			},
			func(results []map[string]interface{}) map[string][]map[string]interface{} {
				return groupByAliases(results, parentAliases)
			},
			func(parentID string) (planner.SQLQuery, planner.SQLQuery, error) {
				tuple := parentValueByKey[parentID]
				count, err := planner.BuildEdgeListCountSQL(junctionTable, junctionLocalColumns, tuple.Values, whereClause)
				if err != nil {
					return planner.SQLQuery{}, planner.SQLQuery{}, err
				}
				agg, err := planner.BuildEdgeListAggregateBaseSQL(junctionTable, junctionLocalColumns, tuple.Values, whereClause)
				return count, agg, err
			},
		)
		if errors.Is(err, errBatchSkip) {
			return nil, false, nil
		}
		if err != nil {
			return nil, true, err
		}
		for k, v := range partial {
			groupedConnections[k] = v
		}
	}

	if len(groupedConnections) == 0 {
		state.setConnectionRows(relKey, map[string]map[string]interface{}{})
		return r.buildConnectionResult(p.Context, nil, nil, false, false), true, nil
	}
	state.setConnectionRows(relKey, groupedConnections)

	if result, ok := groupedConnections[currentParentTupleKey]; ok {
		if nodes, ok := result["nodes"].([]map[string]interface{}); ok {
			seedBatchRows(p, nodes)
		}
		return result, true, nil
	}
	return r.buildConnectionResult(p.Context, nil, nil, false, false), true, nil
}

func (r *Resolver) tryBatchManyToOne(p graphql.ResolveParams, table introspection.Table, rel introspection.Relationship, fkValues []interface{}) (result map[string]interface{}, ok bool, err error) {
	outcome := ""
	ctx, span := startResolverSpan(p.Context, "graphql.batch.many_to_one",
		attribute.String("db.table", table.Name),
		attribute.String("relation_type", relationManyToOne),
	)
	defer func() {
		finishResolverSpan(span, err, outcome)
		span.End()
	}()
	p.Context = ctx

	metrics := graphQLMetricsFromContext(p.Context)

	state, ok := getBatchState(p.Context)
	if !ok {
		if metrics != nil {
			metrics.RecordBatchSkipped(p.Context, relationManyToOne, "no_batch_state")
		}
		span.SetAttributes(attribute.String("graphql.batch.skip_reason", "no_batch_state"))
		outcome = "skipped"
		return nil, false, nil
	}

	parentKey, ok := parentKeyFromSource(p.Source)
	if !ok {
		if metrics != nil {
			metrics.RecordBatchSkipped(p.Context, relationManyToOne, "missing_parent_key")
		}
		span.SetAttributes(attribute.String("graphql.batch.skip_reason", "missing_parent_key"))
		outcome = "skipped"
		return nil, false, nil
	}

	parentRows := state.getParentRows(parentKey)
	if len(parentRows) == 0 {
		if metrics != nil {
			metrics.RecordBatchSkipped(p.Context, relationManyToOne, "missing_parent_rows")
		}
		span.SetAttributes(attribute.String("graphql.batch.skip_reason", "missing_parent_rows"))
		outcome = "skipped"
		return nil, false, nil
	}

	relatedTable, err := r.findTable(rel.RemoteTable)
	if err != nil {
		return nil, true, fmt.Errorf("failed to find related table %s: %w", rel.RemoteTable, err)
	}

	field := firstFieldAST(p.Info.FieldASTs)
	if field == nil {
		return nil, true, fmt.Errorf("missing field AST")
	}
	selection := planner.SelectedColumns(relatedTable, field, p.Info.Fragments)
	remoteColumns := rel.EffectiveRemoteColumns()
	localColumns := rel.EffectiveLocalColumns()
	if len(remoteColumns) == 0 || len(remoteColumns) != len(localColumns) || len(remoteColumns) != len(fkValues) {
		return nil, true, fmt.Errorf("invalid many-to-one batch mapping")
	}

	relKey := fmt.Sprintf("%s|%s|%s|%s", relatedTable.Name, strings.Join(remoteColumns, ","), parentKey, columnsKey(selection))
	if cached := state.getChildRows(relKey); cached != nil {
		state.IncrementCacheHit()
		if metrics != nil {
			metrics.RecordBatchCacheHit(p.Context, relationManyToOne)
		}
		return firstGroupedRecordByTuple(cached, fkValues), true, nil
	}
	state.IncrementCacheMiss()
	if metrics != nil {
		metrics.RecordBatchCacheMiss(p.Context, relationManyToOne)
	}

	parentFields := make([]string, len(localColumns))
	for i, colName := range localColumns {
		parentFields[i] = graphQLFieldNameForColumn(table, colName)
	}
	parentTuples := uniqueParentTuples(parentRows, parentFields)
	span.SetAttributes(attribute.Int("graphql.batch.parent_count", len(parentTuples)))
	if len(parentTuples) == 0 {
		state.setChildRows(relKey, map[string][]map[string]interface{}{})
		span.SetAttributes(attribute.String("graphql.batch.skip_reason", "no_parent_tuples"))
		outcome = "skipped"
		return nil, true, nil
	}

	chunks := chunkParentTuples(parentTuples, batchMaxInClause)
	span.SetAttributes(attribute.Int("graphql.batch.chunk_count", len(chunks)))
	if metrics != nil {
		metrics.RecordBatchQueriesSaved(p.Context, listBatchQueriesSaved(len(parentTuples), len(chunks)), relationManyToOne)
	}

	grouped := make(map[string][]map[string]interface{})
	parentAliases := planner.BatchParentAliases(len(remoteColumns))
	for _, chunk := range chunks {
		if metrics != nil {
			metrics.RecordBatchParentCount(p.Context, int64(len(chunk)), relationManyToOne)
		}
		planned, err := planner.PlanManyToOneBatch(relatedTable, selection, remoteColumns, chunk)
		if err != nil {
			return nil, true, err
		}
		if planned.SQL == "" {
			continue
		}

		rows, err := r.queryExecutorForContext(p.Context).QueryContext(p.Context, planned.SQL, planned.Args...)
		if err != nil {
			return nil, true, normalizeQueryError(err)
		}
		results, err := scanRowsWithExtras(rows, selection, parentAliases)
		rows.Close()
		if err != nil {
			return nil, true, err
		}
		if metrics != nil {
			metrics.RecordBatchResultRows(p.Context, int64(len(results)), relationManyToOne)
		}

		mergeGrouped(grouped, groupByAliases(results, parentAliases))
	}
	if len(grouped) == 0 {
		state.setChildRows(relKey, map[string][]map[string]interface{}{})
		return nil, true, nil
	}
	state.setChildRows(relKey, grouped)

	return firstGroupedRecordByTuple(grouped, fkValues), true, nil
}
