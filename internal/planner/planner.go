// Package planner converts GraphQL queries into parameterized SQL statements.
// It handles table lookups, relationship resolution, filtering, ordering, and pagination
// while enforcing query cost limits to prevent expensive operations.
package planner

import (
	"encoding/base64"
	"errors"
	"fmt"
	"sort"
	"strings"

	"tidb-graphql/internal/introspection"
	"tidb-graphql/internal/setutil"
	"tidb-graphql/internal/sqltype"
	"tidb-graphql/internal/sqlutil"
	"tidb-graphql/internal/uuidutil"

	sq "github.com/Masterminds/squirrel"
)

// ErrNoPrimaryKey indicates a required primary key is missing for a batch plan.
var ErrNoPrimaryKey = errors.New("no primary key")

// BatchParentAlias is the column alias used to return parent keys in batch queries.
const BatchParentAlias = "__batch_parent_id"

const batchParentAliasPrefix = "__batch_parent_"

// ParentTuple represents an ordered composite parent key used in batch plans.
type ParentTuple struct {
	Values []interface{}
}

// BatchParentAliases returns the extra scan aliases emitted by batch SQL.
func BatchParentAliases(columnCount int) []string {
	if columnCount <= 1 {
		return []string{BatchParentAlias}
	}
	aliases := make([]string, columnCount)
	for i := 0; i < columnCount; i++ {
		aliases[i] = batchParentAliasPrefix + fmt.Sprint(i)
	}
	return aliases
}

// SQLQuery represents a planned SQL statement with bound args.
type SQLQuery struct {
	SQL  string
	Args []interface{}
}

// PlanTableByPK builds the SQL for a single-column primary key lookup.
func PlanTableByPK(table introspection.Table, columns []introspection.Column, pk *introspection.Column, pkValue interface{}) (SQLQuery, error) {
	query, args, err := sq.Select(columnNames(table, columns)...).
		From(sqlutil.QuoteIdentifier(table.Name)).
		Where(sq.Eq{sqlutil.QuoteIdentifier(pk.Name): pkValue}).
		PlaceholderFormat(sq.Question).
		ToSql()
	if err != nil {
		return SQLQuery{}, err
	}

	return SQLQuery{SQL: query, Args: args}, nil
}

// PlanTableByPKColumns builds the SQL for a composite primary key lookup.
func PlanTableByPKColumns(table introspection.Table, columns []introspection.Column, pkCols []introspection.Column, values map[string]interface{}) (SQLQuery, error) {
	whereClause := sq.Eq{}
	for _, pk := range pkCols {
		value, ok := values[pk.Name]
		if !ok {
			return SQLQuery{}, fmt.Errorf("missing value for primary key column %s", pk.Name)
		}
		whereClause[sqlutil.QuoteIdentifier(pk.Name)] = value
	}

	query, args, err := sq.Select(columnNames(table, columns)...).
		From(sqlutil.QuoteIdentifier(table.Name)).
		Where(whereClause).
		PlaceholderFormat(sq.Question).
		ToSql()
	if err != nil {
		return SQLQuery{}, err
	}

	return SQLQuery{SQL: query, Args: args}, nil
}

// PlanUniqueKeyLookup builds the SQL for a unique index lookup.
func PlanUniqueKeyLookup(table introspection.Table, columns []introspection.Column, idx introspection.Index, values map[string]interface{}) (SQLQuery, error) {
	// Build WHERE clause for all columns in the unique index
	whereClause := sq.Eq{}
	for _, colName := range idx.Columns {
		value, ok := values[colName]
		if !ok {
			return SQLQuery{}, fmt.Errorf("missing value for unique key column %s", colName)
		}
		whereClause[sqlutil.QuoteIdentifier(colName)] = value
	}

	query, args, err := sq.Select(columnNames(table, columns)...).
		From(sqlutil.QuoteIdentifier(table.Name)).
		Where(whereClause).
		PlaceholderFormat(sq.Question).
		ToSql()
	if err != nil {
		return SQLQuery{}, err
	}

	return SQLQuery{SQL: query, Args: args}, nil
}

// PlanManyToOne builds SQL for a many-to-one lookup (FK -> parent table), including composite mappings.
func PlanManyToOne(relatedTable introspection.Table, columns []introspection.Column, remoteColumns []string, fkValues []interface{}) (SQLQuery, error) {
	if len(remoteColumns) == 0 || len(remoteColumns) != len(fkValues) {
		return SQLQuery{}, fmt.Errorf("many-to-one mapping requires equal remote columns and values")
	}

	builder := sq.Select(columnNames(relatedTable, columns)...).
		From(sqlutil.QuoteIdentifier(relatedTable.Name))
	if len(remoteColumns) == 1 {
		builder = builder.Where(sq.Eq{sqlutil.QuoteIdentifier(remoteColumns[0]): fkValues[0]})
	} else {
		whereClause := sq.Eq{}
		for i, col := range remoteColumns {
			whereClause[sqlutil.QuoteIdentifier(col)] = fkValues[i]
		}
		builder = builder.Where(whereClause)
	}

	query, args, err := builder.PlaceholderFormat(sq.Question).ToSql()
	if err != nil {
		return SQLQuery{}, err
	}
	return SQLQuery{SQL: query, Args: args}, nil
}

// PlanManyToOneBatch builds the SQL for a batched many-to-one lookup (FK -> parent table).
func PlanManyToOneBatch(relatedTable introspection.Table, columns []introspection.Column, remoteColumns []string, values []ParentTuple) (SQLQuery, error) {
	if len(values) == 0 {
		return SQLQuery{}, nil
	}
	if len(remoteColumns) == 0 {
		return SQLQuery{}, fmt.Errorf("many-to-one batch requires at least one remote column")
	}
	aliases := BatchParentAliases(len(remoteColumns))

	builder := sq.Select(columnNames(relatedTable, columns)...).
		From(sqlutil.QuoteIdentifier(relatedTable.Name))

	if len(remoteColumns) == 1 {
		flat := make([]interface{}, 0, len(values))
		for _, tuple := range values {
			if len(tuple.Values) != 1 {
				return SQLQuery{}, fmt.Errorf("many-to-one batch tuple width mismatch")
			}
			flat = append(flat, tuple.Values[0])
		}
		builder = builder.
			Where(sq.Eq{sqlutil.QuoteIdentifier(remoteColumns[0]): flat}).
			Column(fmt.Sprintf("%s AS %s", sqlutil.QuoteIdentifier(remoteColumns[0]), aliases[0]))
	} else {
		whereSQL, whereArgs, err := buildTupleInCondition(quotedColumnNames(remoteColumns), values)
		if err != nil {
			return SQLQuery{}, err
		}
		if whereSQL == "" {
			return SQLQuery{}, nil
		}
		builder = builder.Where(sq.Expr(whereSQL, whereArgs...))
		for i, col := range remoteColumns {
			builder = builder.Column(fmt.Sprintf("%s AS %s", sqlutil.QuoteIdentifier(col), aliases[i]))
		}
	}

	query, args, err := builder.PlaceholderFormat(sq.Question).ToSql()
	if err != nil {
		return SQLQuery{}, err
	}
	return SQLQuery{SQL: query, Args: args}, nil
}

// PlanManyToManyBatch builds a batched SQL query for many-to-many relationships with per-parent limits.
func PlanManyToManyBatch(
	junctionTable string,
	targetTable introspection.Table,
	junctionLocalFKColumns []string,
	junctionRemoteFKColumns []string,
	targetPKColumns []string,
	columns []introspection.Column,
	values []ParentTuple,
	limit, offset int,
	orderBy *OrderBy,
	where *WhereClause,
) (SQLQuery, error) {
	if len(junctionLocalFKColumns) == 0 || len(junctionRemoteFKColumns) == 0 || len(targetPKColumns) == 0 {
		return SQLQuery{}, fmt.Errorf("many-to-many batch requires key column mappings")
	}
	if len(junctionRemoteFKColumns) != len(targetPKColumns) {
		return SQLQuery{}, fmt.Errorf("many-to-many batch remote key mapping width mismatch")
	}

	quotedTarget := sqlutil.QuoteIdentifier(targetTable.Name)
	quotedJunction := sqlutil.QuoteIdentifier(junctionTable)
	joinPredicates := make([]string, len(junctionRemoteFKColumns))
	for i := range junctionRemoteFKColumns {
		joinPredicates[i] = fmt.Sprintf(
			"%s.%s = %s.%s",
			quotedJunction, sqlutil.QuoteIdentifier(junctionRemoteFKColumns[i]),
			quotedTarget, sqlutil.QuoteIdentifier(targetPKColumns[i]),
		)
	}
	fromClause := fmt.Sprintf("%s INNER JOIN %s ON %s", quotedTarget, quotedJunction, strings.Join(joinPredicates, " AND "))
	partitionColumns := qualifiedColumnNames(quotedJunction, junctionLocalFKColumns)

	orderClause, err := batchOrderClause(targetTable, orderBy)
	if err != nil {
		return SQLQuery{}, err
	}
	columnList := strings.Join(columnNames(targetTable, columns), ", ")
	return buildBatchWindowQuery(fromClause, columnList, partitionColumns, orderClause, values, limit, offset, where)
}

// PlanEdgeListBatch builds a batched SQL query for edge list relationships with per-parent limits.
func PlanEdgeListBatch(
	junctionTable introspection.Table,
	junctionLocalFKColumns []string,
	columns []introspection.Column,
	values []ParentTuple,
	limit, offset int,
	orderBy *OrderBy,
	where *WhereClause,
) (SQLQuery, error) {
	if len(junctionLocalFKColumns) == 0 {
		return SQLQuery{}, fmt.Errorf("edge-list batch requires at least one local FK column")
	}

	quotedTable := sqlutil.QuoteIdentifier(junctionTable.Name)
	partitionColumns := quotedColumnNames(junctionLocalFKColumns)
	orderClause, err := batchOrderClause(junctionTable, orderBy)
	if err != nil {
		return SQLQuery{}, err
	}
	columnList := strings.Join(columnNames(junctionTable, columns), ", ")
	return buildBatchWindowQuery(quotedTable, columnList, partitionColumns, orderClause, values, limit, offset, where)
}

// buildBatchWindowQuery emits the shared ROW_NUMBER() window pattern used by
// PlanManyToManyBatch and PlanEdgeListBatch.
func buildBatchWindowQuery(
	fromClause string,
	columnList string,
	partitionColumns []string,
	orderClause string,
	values []ParentTuple,
	limit, offset int,
	where *WhereClause,
) (SQLQuery, error) {
	if len(values) == 0 {
		return SQLQuery{}, nil
	}
	if err := validateLimitOffset(limit, offset); err != nil {
		return SQLQuery{}, err
	}

	partitionExpr := strings.Join(partitionColumns, ", ")
	parentAliases := BatchParentAliases(len(partitionColumns))
	innerParentCols := make([]string, len(partitionColumns))
	outerParentCols := make([]string, len(partitionColumns))
	for i := range partitionColumns {
		innerParentCols[i] = fmt.Sprintf("%s AS %s", partitionColumns[i], parentAliases[i])
		outerParentCols[i] = parentAliases[i]
	}

	whereSQL := ""
	var whereArgs []interface{}
	if where != nil && where.Condition != nil {
		condSQL, condArgs, err := where.Condition.ToSql()
		if err != nil {
			return SQLQuery{}, err
		}
		whereSQL = " AND " + condSQL
		whereArgs = condArgs
	}
	parentWhereSQL, parentWhereArgs, err := buildTupleInCondition(partitionColumns, values)
	if err != nil {
		return SQLQuery{}, err
	}
	if parentWhereSQL == "" {
		return SQLQuery{}, nil
	}

	outerSelect := fmt.Sprintf("%s, %s", columnList, strings.Join(outerParentCols, ", "))
	innerSelect := fmt.Sprintf("%s, %s", columnList, strings.Join(innerParentCols, ", "))
	query := fmt.Sprintf(
		"SELECT %s FROM (SELECT %s, ROW_NUMBER() OVER (PARTITION BY %s ORDER BY %s) AS __rn FROM %s WHERE %s%s) AS __batch WHERE __rn > ? AND __rn <= ? ORDER BY %s, __rn",
		outerSelect,
		innerSelect,
		partitionExpr,
		orderClause,
		fromClause,
		parentWhereSQL,
		whereSQL,
		strings.Join(outerParentCols, ", "),
	)

	args := append([]interface{}{}, parentWhereArgs...)
	args = append(args, whereArgs...)
	args = append(args, offset, offset+limit)
	return SQLQuery{SQL: query, Args: args}, nil
}

// PlanOneToManyBatch builds a batched SQL query with per-parent limits.
func PlanOneToManyBatch(
	relatedTable introspection.Table,
	columns []introspection.Column,
	remoteColumn string,
	values []interface{},
	limit, offset int,
	orderBy *OrderBy,
	where *WhereClause,
) (SQLQuery, error) {
	if len(values) == 0 {
		return SQLQuery{}, nil
	}
	if err := validateLimitOffset(limit, offset); err != nil {
		return SQLQuery{}, err
	}

	pkCols := introspection.PrimaryKeyColumns(relatedTable)
	if len(pkCols) == 0 {
		return SQLQuery{}, fmt.Errorf("%w: table %s", ErrNoPrimaryKey, relatedTable.Name)
	}

	cols := columnNames(relatedTable, columns)
	columnList := strings.Join(cols, ", ")
	placeholders := sq.Placeholders(len(values))

	// Build ORDER BY clause from all primary key columns (for composite PKs)
	var pkOrderClauses []string
	for _, pk := range pkCols {
		pkOrderClauses = append(pkOrderClauses, sqlutil.QuoteIdentifier(pk.Name))
	}
	orderClause := strings.Join(pkOrderClauses, ", ")
	if orderBy != nil && len(orderBy.Columns) > 0 {
		orderClause = strings.Join(orderByClauses(orderBy), ", ")
	}

	quotedRemoteColumn := sqlutil.QuoteIdentifier(remoteColumn)
	quotedTable := sqlutil.QuoteIdentifier(relatedTable.Name)

	whereSQL := ""
	var whereArgs []interface{}
	if where != nil && where.Condition != nil {
		condSQL, condArgs, err := where.Condition.ToSql()
		if err != nil {
			return SQLQuery{}, err
		}
		whereSQL = " AND " + condSQL
		whereArgs = condArgs
	}
	// Unfortunately as these are column lists, we can't use Squirrel to build
	// the query so need to create it directly.
	outerSelect := fmt.Sprintf("%s, %s", columnList, BatchParentAlias)
	innerSelect := fmt.Sprintf("%s, %s AS %s", columnList, quotedRemoteColumn, BatchParentAlias)
	query := fmt.Sprintf(
		"SELECT %s FROM (SELECT %s, ROW_NUMBER() OVER (PARTITION BY %s ORDER BY %s) AS __rn FROM %s WHERE %s IN (%s)%s) AS __batch WHERE __rn > ? AND __rn <= ? ORDER BY %s, __rn",
		outerSelect,
		innerSelect,
		quotedRemoteColumn,
		orderClause,
		quotedTable,
		quotedRemoteColumn,
		placeholders,
		whereSQL,
		BatchParentAlias,
	)

	args := append([]interface{}{}, values...)
	args = append(args, whereArgs...)
	args = append(args, offset, offset+limit)
	return SQLQuery{SQL: query, Args: args}, nil
}

// quotedColumnNames returns the backtick-quoted column identifiers with no table prefix.
func quotedColumnNames(columns []string) []string {
	quoted := make([]string, len(columns))
	for i, col := range columns {
		quoted[i] = sqlutil.QuoteIdentifier(col)
	}
	return quoted
}

// qualifiedColumnNames returns column identifiers prefixed with a pre-quoted table alias.
func qualifiedColumnNames(quotedAlias string, columns []string) []string {
	qualified := make([]string, len(columns))
	for i, col := range columns {
		qualified[i] = fmt.Sprintf("%s.%s", quotedAlias, sqlutil.QuoteIdentifier(col))
	}
	return qualified
}

func buildTupleInCondition(quotedColumns []string, tuples []ParentTuple) (string, []interface{}, error) {
	if len(tuples) == 0 {
		return "", nil, nil
	}
	width := len(quotedColumns)
	if width == 0 {
		return "", nil, fmt.Errorf("tuple IN requires at least one column")
	}

	if width == 1 {
		placeholders := sq.Placeholders(len(tuples))
		args := make([]interface{}, 0, len(tuples))
		for _, tuple := range tuples {
			if len(tuple.Values) != 1 {
				return "", nil, fmt.Errorf("tuple width mismatch: expected 1 value")
			}
			args = append(args, tuple.Values[0])
		}
		return fmt.Sprintf("%s IN (%s)", quotedColumns[0], placeholders), args, nil
	}

	args := make([]interface{}, 0, len(tuples)*width)
	rowPlaceholders := make([]string, 0, len(tuples))
	valuePlaceholders := "(" + strings.TrimSuffix(strings.Repeat("?,", width), ",") + ")"
	for _, tuple := range tuples {
		if len(tuple.Values) != width {
			return "", nil, fmt.Errorf("tuple width mismatch: expected %d values", width)
		}
		rowPlaceholders = append(rowPlaceholders, valuePlaceholders)
		args = append(args, tuple.Values...)
	}

	return fmt.Sprintf("(%s) IN (%s)", strings.Join(quotedColumns, ", "), strings.Join(rowPlaceholders, ", ")), args, nil
}

func columnNames(table introspection.Table, columns []introspection.Column) []string {
	cols := columns
	if len(cols) == 0 {
		cols = table.Columns
	}
	names := make([]string, len(cols))
	for i, col := range cols {
		names[i] = sqlutil.QuoteIdentifier(col.Name)
	}
	return names
}

func orderByClauses(orderBy *OrderBy) []string {
	if orderBy == nil {
		return nil
	}
	clauses := make([]string, len(orderBy.Columns))
	for i, col := range orderBy.Columns {
		direction := "ASC"
		if i < len(orderBy.Directions) && strings.EqualFold(orderBy.Directions[i], "DESC") {
			direction = "DESC"
		}
		clauses[i] = fmt.Sprintf("%s %s", sqlutil.QuoteIdentifier(col), direction)
	}
	return clauses
}

func batchOrderClause(table introspection.Table, orderBy *OrderBy) (string, error) {
	if orderBy != nil && len(orderBy.Columns) > 0 {
		return strings.Join(orderByClauses(orderBy), ", "), nil
	}

	pkCols := introspection.PrimaryKeyColumns(table)
	if len(pkCols) == 0 {
		return "", ErrNoPrimaryKey
	}

	var pkOrderClauses []string
	for _, pk := range pkCols {
		pkOrderClauses = append(pkOrderClauses, sqlutil.QuoteIdentifier(pk.Name))
	}
	return strings.Join(pkOrderClauses, ", "), nil
}

// WhereClause represents a parsed WHERE condition
type WhereClause struct {
	Condition          sq.Sqlizer
	UsedColumns        []string
	UsedColumnsByTable map[string][]string
}

// BuildWhereClause parses a GraphQL WHERE input into a SQL WHERE clause.
// Returns the condition and a list of columns used (for indexed validation).
func BuildWhereClause(table introspection.Table, whereInput map[string]interface{}) (*WhereClause, error) {
	return buildWhereClauseWithAliasAndSchema(nil, table, "", whereInput)
}

// BuildWhereClauseWithSchema parses a GraphQL WHERE input into a SQL WHERE clause
// and enables relationship-aware filters using schema metadata.
func BuildWhereClauseWithSchema(schema *introspection.Schema, table introspection.Table, whereInput map[string]interface{}) (*WhereClause, error) {
	return buildWhereClauseWithAliasAndSchema(schema, table, "", whereInput)
}

// BuildWhereClauseQualified parses a GraphQL WHERE input into a SQL WHERE clause
// with qualified column names (alias.column).
func BuildWhereClauseQualified(table introspection.Table, alias string, whereInput map[string]interface{}) (*WhereClause, error) {
	return buildWhereClauseWithAliasAndSchema(nil, table, alias, whereInput)
}

// BuildWhereClauseQualifiedWithSchema parses a GraphQL WHERE input into a SQL WHERE clause
// with qualified column names (alias.column) and enables relationship-aware filters.
func BuildWhereClauseQualifiedWithSchema(schema *introspection.Schema, table introspection.Table, alias string, whereInput map[string]interface{}) (*WhereClause, error) {
	return buildWhereClauseWithAliasAndSchema(schema, table, alias, whereInput)
}

type whereBuildState struct {
	schema       *introspection.Schema
	aliasCounter int
	usedByTable  map[string]map[string]struct{}
}

func newWhereBuildState(schema *introspection.Schema) *whereBuildState {
	return &whereBuildState{
		schema:      schema,
		usedByTable: make(map[string]map[string]struct{}),
	}
}

func (s *whereBuildState) nextAlias(prefix string) string {
	normalized := strings.TrimSpace(prefix)
	if normalized == "" {
		normalized = "rel"
	}
	normalized = strings.ReplaceAll(normalized, "`", "")
	normalized = strings.ReplaceAll(normalized, ".", "_")
	s.aliasCounter++
	return fmt.Sprintf("__%s_%d", normalized, s.aliasCounter)
}

func (s *whereBuildState) addUsedColumn(tableName, columnName string) {
	if tableName == "" || columnName == "" {
		return
	}
	cols, ok := s.usedByTable[tableName]
	if !ok {
		cols = make(map[string]struct{})
		s.usedByTable[tableName] = cols
	}
	cols[columnName] = struct{}{}
}

func (s *whereBuildState) usedColumnsByTable() map[string][]string {
	out := make(map[string][]string, len(s.usedByTable))
	for tableName, colSet := range s.usedByTable {
		cols := make([]string, 0, len(colSet))
		for col := range colSet {
			cols = append(cols, col)
		}
		sort.Strings(cols)
		out[tableName] = cols
	}
	return out
}

func buildWhereClauseWithAliasAndSchema(schema *introspection.Schema, table introspection.Table, alias string, whereInput map[string]interface{}) (*WhereClause, error) {
	if len(whereInput) == 0 {
		return nil, nil
	}

	state := newWhereBuildState(schema)
	condition, err := buildWhereCondition(table, alias, whereInput, state, true, "")
	if err != nil {
		return nil, err
	}
	usedByTable := state.usedColumnsByTable()
	rootUsed := usedByTable[table.Name]

	return &WhereClause{
		Condition:          condition,
		UsedColumns:        rootUsed,
		UsedColumnsByTable: usedByTable,
	}, nil
}

// buildWhereCondition recursively builds WHERE conditions with AND/OR support.
// When alias is non-empty, column names are qualified as alias.column.
func buildWhereCondition(
	table introspection.Table,
	alias string,
	whereInput map[string]interface{},
	state *whereBuildState,
	allowRelations bool,
	path string,
) (sq.Sqlizer, error) {
	conditions := []sq.Sqlizer{}
	keys := make([]string, 0, len(whereInput))
	for key := range whereInput {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	for _, key := range keys {
		value := whereInput[key]
		switch key {
		case "AND":
			andArray, ok := value.([]interface{})
			if !ok {
				return nil, fmt.Errorf("AND must be an array")
			}
			andConditions := []sq.Sqlizer{}
			for _, item := range andArray {
				itemMap, ok := item.(map[string]interface{})
				if !ok {
					return nil, fmt.Errorf("AND array items must be objects")
				}
				cond, err := buildWhereCondition(table, alias, itemMap, state, allowRelations, path)
				if err != nil {
					return nil, err
				}
				if cond != nil {
					andConditions = append(andConditions, cond)
				}
			}
			if len(andConditions) > 0 {
				conditions = append(conditions, sq.And(andConditions))
			}

		case "OR":
			orArray, ok := value.([]interface{})
			if !ok {
				return nil, fmt.Errorf("OR must be an array")
			}
			orConditions := []sq.Sqlizer{}
			for _, item := range orArray {
				itemMap, ok := item.(map[string]interface{})
				if !ok {
					return nil, fmt.Errorf("OR array items must be objects")
				}
				cond, err := buildWhereCondition(table, alias, itemMap, state, allowRelations, path)
				if err != nil {
					return nil, err
				}
				if cond != nil {
					orConditions = append(orConditions, cond)
				}
			}
			if len(orConditions) > 0 {
				conditions = append(conditions, sq.Or(orConditions))
			}

		default:
			col := findColumnByGraphQLName(table, key)
			if col != nil {
				state.addUsedColumn(table.Name, col.Name)

				filterMap, ok := value.(map[string]interface{})
				if !ok {
					return nil, fmt.Errorf("filter for %s must be an object", key)
				}

				colConditions, err := buildColumnFilter(*col, alias, filterMap)
				if err != nil {
					return nil, err
				}
				conditions = append(conditions, colConditions...)
				continue
			}

			rel := findRelationshipByGraphQLName(table, key)
			if rel == nil {
				return nil, fmt.Errorf("unknown column: %s", key)
			}
			if !allowRelations {
				if path != "" {
					return nil, fmt.Errorf("relationship where filters support single hop only (nested relation at %s.%s)", path, key)
				}
				return nil, fmt.Errorf("relationship where filters support single hop only (nested relation %s)", key)
			}

			relCond, err := buildRelationshipFilterCondition(table, alias, *rel, key, value, state, key)
			if err != nil {
				return nil, err
			}
			if relCond != nil {
				conditions = append(conditions, relCond)
			}
		}
	}

	if len(conditions) == 0 {
		return nil, nil
	}
	if len(conditions) == 1 {
		return conditions[0], nil
	}
	return sq.And(conditions), nil
}

func buildRelationshipFilterCondition(
	table introspection.Table,
	alias string,
	rel introspection.Relationship,
	fieldName string,
	value interface{},
	state *whereBuildState,
	path string,
) (sq.Sqlizer, error) {
	filterMap, ok := value.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("filter for relationship %s must be an object", fieldName)
	}

	if rel.IsManyToOne {
		for op := range filterMap {
			if op != "is" && op != "isNull" {
				return nil, fmt.Errorf("unknown relationship filter operator: %s", op)
			}
		}
		_, hasIs := filterMap["is"]
		_, hasIsNull := filterMap["isNull"]
		if hasIs && hasIsNull {
			return nil, fmt.Errorf("relationship filter %s cannot use both is and isNull", fieldName)
		}
		conditions := []sq.Sqlizer{}
		if rawIs, ok := filterMap["is"]; ok {
			isWhere, ok := rawIs.(map[string]interface{})
			if !ok {
				return nil, fmt.Errorf("relationship filter %s.is must be an object", fieldName)
			}
			cond, err := buildRelationshipExistsPredicate(table, alias, rel, isWhere, true, state, path)
			if err != nil {
				return nil, err
			}
			conditions = append(conditions, cond)
		}
		if rawIsNull, ok := filterMap["isNull"]; ok {
			isNull, ok := rawIsNull.(bool)
			if !ok {
				return nil, fmt.Errorf("relationship filter %s.isNull must be a boolean", fieldName)
			}
			cond, err := buildRelationshipExistsPredicate(table, alias, rel, map[string]interface{}{}, !isNull, state, path)
			if err != nil {
				return nil, err
			}
			conditions = append(conditions, cond)
		}
		if len(conditions) == 0 {
			return nil, fmt.Errorf("relationship filter %s must include is or isNull", fieldName)
		}
		if len(conditions) == 1 {
			return conditions[0], nil
		}
		return sq.And(conditions), nil
	}

	if !(rel.IsOneToMany || rel.IsManyToMany || rel.IsEdgeList) {
		return nil, fmt.Errorf("unsupported relationship filter on %s", fieldName)
	}
	for op := range filterMap {
		if op != "some" && op != "none" {
			return nil, fmt.Errorf("unknown relationship filter operator: %s", op)
		}
	}

	conditions := []sq.Sqlizer{}
	if rawSome, ok := filterMap["some"]; ok {
		someWhere, ok := rawSome.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("relationship filter %s.some must be an object", fieldName)
		}
		cond, err := buildRelationshipExistsPredicate(table, alias, rel, someWhere, true, state, path)
		if err != nil {
			return nil, err
		}
		conditions = append(conditions, cond)
	}
	if rawNone, ok := filterMap["none"]; ok {
		noneWhere, ok := rawNone.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("relationship filter %s.none must be an object", fieldName)
		}
		cond, err := buildRelationshipExistsPredicate(table, alias, rel, noneWhere, false, state, path)
		if err != nil {
			return nil, err
		}
		conditions = append(conditions, cond)
	}
	if len(conditions) == 0 {
		return nil, fmt.Errorf("relationship filter %s must include some or none", fieldName)
	}
	if len(conditions) == 1 {
		return conditions[0], nil
	}
	return sq.And(conditions), nil
}

func buildRelationshipExistsPredicate(
	table introspection.Table,
	outerAlias string,
	rel introspection.Relationship,
	nestedWhere map[string]interface{},
	shouldExist bool,
	state *whereBuildState,
	path string,
) (sq.Sqlizer, error) {
	subquery, args, err := buildRelationshipSubquerySQL(table, outerAlias, rel, nestedWhere, state, path)
	if err != nil {
		return nil, err
	}
	prefix := "EXISTS"
	if !shouldExist {
		prefix = "NOT EXISTS"
	}
	return sq.Expr(fmt.Sprintf("%s (%s)", prefix, subquery), args...), nil
}

func buildRelationshipSubquerySQL(
	table introspection.Table,
	outerAlias string,
	rel introspection.Relationship,
	nestedWhere map[string]interface{},
	state *whereBuildState,
	path string,
) (string, []interface{}, error) {
	outerRefAlias := outerAlias
	if outerRefAlias == "" {
		// Root-table relationship filters still need deterministic correlation targets.
		// Using table name avoids ambiguous bare-column references inside subqueries.
		outerRefAlias = table.Name
	}

	resolveTable := func(tableName string) (introspection.Table, error) {
		if state.schema == nil {
			return introspection.Table{}, fmt.Errorf("relationship where filters require schema context")
		}
		for _, candidate := range state.schema.Tables {
			if candidate.Name == tableName {
				return candidate, nil
			}
		}
		return introspection.Table{}, fmt.Errorf("relationship where table not found: %s", tableName)
	}
	qualifiedColumn := func(alias, col string) string {
		if alias == "" {
			return sqlutil.QuoteIdentifier(col)
		}
		return fmt.Sprintf("%s.%s", sqlutil.QuoteIdentifier(alias), sqlutil.QuoteIdentifier(col))
	}
	quotedFrom := func(tableName, alias string) string {
		if alias == "" {
			return sqlutil.QuoteIdentifier(tableName)
		}
		return fmt.Sprintf("%s AS %s", sqlutil.QuoteIdentifier(tableName), sqlutil.QuoteIdentifier(alias))
	}

	joinPairs := func(leftAlias string, leftCols []string, rightAlias string, rightCols []string) ([]string, error) {
		if len(leftCols) == 0 || len(leftCols) != len(rightCols) {
			return nil, fmt.Errorf("relationship mapping width mismatch")
		}
		pairs := make([]string, len(leftCols))
		for i := range leftCols {
			pairs[i] = fmt.Sprintf(
				"%s = %s",
				qualifiedColumn(leftAlias, leftCols[i]),
				qualifiedColumn(rightAlias, rightCols[i]),
			)
		}
		return pairs, nil
	}

	localCols := rel.EffectiveLocalColumns()
	remoteCols := rel.EffectiveRemoteColumns()
	if len(localCols) == 0 {
		return "", nil, fmt.Errorf("relationship %s has no local key mapping", rel.GraphQLFieldName)
	}

	switch {
	case rel.IsManyToOne:
		remoteTable, err := resolveTable(rel.RemoteTable)
		if err != nil {
			return "", nil, err
		}
		remoteAlias := state.nextAlias(remoteTable.Name)
		corrPairs, err := joinPairs(remoteAlias, remoteCols, outerRefAlias, localCols)
		if err != nil {
			return "", nil, err
		}
		for _, col := range remoteCols {
			state.addUsedColumn(remoteTable.Name, col)
		}
		for _, col := range localCols {
			state.addUsedColumn(table.Name, col)
		}

		builder := sq.Select("1").From(quotedFrom(remoteTable.Name, remoteAlias))
		for _, pair := range corrPairs {
			builder = builder.Where(sq.Expr(pair))
		}
		if len(nestedWhere) > 0 {
			nestedCond, err := buildWhereCondition(remoteTable, remoteAlias, nestedWhere, state, false, path)
			if err != nil {
				return "", nil, err
			}
			if nestedCond != nil {
				builder = builder.Where(nestedCond)
			}
		}
		return builder.PlaceholderFormat(sq.Question).ToSql()

	case rel.IsOneToMany:
		remoteTable, err := resolveTable(rel.RemoteTable)
		if err != nil {
			return "", nil, err
		}
		remoteAlias := state.nextAlias(remoteTable.Name)
		corrPairs, err := joinPairs(remoteAlias, remoteCols, outerRefAlias, localCols)
		if err != nil {
			return "", nil, err
		}
		for _, col := range remoteCols {
			state.addUsedColumn(remoteTable.Name, col)
		}
		for _, col := range localCols {
			state.addUsedColumn(table.Name, col)
		}

		builder := sq.Select("1").From(quotedFrom(remoteTable.Name, remoteAlias))
		for _, pair := range corrPairs {
			builder = builder.Where(sq.Expr(pair))
		}
		if len(nestedWhere) > 0 {
			nestedCond, err := buildWhereCondition(remoteTable, remoteAlias, nestedWhere, state, false, path)
			if err != nil {
				return "", nil, err
			}
			if nestedCond != nil {
				builder = builder.Where(nestedCond)
			}
		}
		return builder.PlaceholderFormat(sq.Question).ToSql()

	case rel.IsEdgeList:
		junctionTable, err := resolveTable(rel.JunctionTable)
		if err != nil {
			return "", nil, err
		}
		junctionAlias := state.nextAlias(junctionTable.Name)
		junctionLocalCols := rel.EffectiveJunctionLocalFKColumns()
		corrPairs, err := joinPairs(junctionAlias, junctionLocalCols, outerRefAlias, localCols)
		if err != nil {
			return "", nil, err
		}
		for _, col := range junctionLocalCols {
			state.addUsedColumn(junctionTable.Name, col)
		}
		for _, col := range localCols {
			state.addUsedColumn(table.Name, col)
		}

		builder := sq.Select("1").From(quotedFrom(junctionTable.Name, junctionAlias))
		for _, pair := range corrPairs {
			builder = builder.Where(sq.Expr(pair))
		}
		if len(nestedWhere) > 0 {
			nestedCond, err := buildWhereCondition(junctionTable, junctionAlias, nestedWhere, state, false, path)
			if err != nil {
				return "", nil, err
			}
			if nestedCond != nil {
				builder = builder.Where(nestedCond)
			}
		}
		return builder.PlaceholderFormat(sq.Question).ToSql()

	case rel.IsManyToMany:
		remoteTable, err := resolveTable(rel.RemoteTable)
		if err != nil {
			return "", nil, err
		}
		junctionTable, err := resolveTable(rel.JunctionTable)
		if err != nil {
			return "", nil, err
		}

		junctionLocalCols := rel.EffectiveJunctionLocalFKColumns()
		junctionRemoteCols := rel.EffectiveJunctionRemoteFKColumns()
		if len(junctionLocalCols) != len(localCols) {
			return "", nil, fmt.Errorf("many-to-many local mapping width mismatch")
		}
		if len(junctionRemoteCols) != len(remoteCols) {
			return "", nil, fmt.Errorf("many-to-many remote mapping width mismatch")
		}
		for _, col := range junctionLocalCols {
			state.addUsedColumn(junctionTable.Name, col)
		}
		for _, col := range junctionRemoteCols {
			state.addUsedColumn(junctionTable.Name, col)
		}
		for _, col := range remoteCols {
			state.addUsedColumn(remoteTable.Name, col)
		}
		for _, col := range localCols {
			state.addUsedColumn(table.Name, col)
		}

		junctionAlias := state.nextAlias(junctionTable.Name)
		remoteAlias := state.nextAlias(remoteTable.Name)
		joinConditions, err := joinPairs(junctionAlias, junctionRemoteCols, remoteAlias, remoteCols)
		if err != nil {
			return "", nil, err
		}
		corrPairs, err := joinPairs(junctionAlias, junctionLocalCols, outerRefAlias, localCols)
		if err != nil {
			return "", nil, err
		}

		builder := sq.Select("1").
			From(quotedFrom(junctionTable.Name, junctionAlias)).
			Join(fmt.Sprintf("%s ON %s", quotedFrom(remoteTable.Name, remoteAlias), strings.Join(joinConditions, " AND ")))
		for _, pair := range corrPairs {
			builder = builder.Where(sq.Expr(pair))
		}
		if len(nestedWhere) > 0 {
			nestedCond, err := buildWhereCondition(remoteTable, remoteAlias, nestedWhere, state, false, path)
			if err != nil {
				return "", nil, err
			}
			if nestedCond != nil {
				builder = builder.Where(nestedCond)
			}
		}
		return builder.PlaceholderFormat(sq.Question).ToSql()

	default:
		return "", nil, fmt.Errorf("unsupported relationship filter on %s", rel.GraphQLFieldName)
	}
}

// isNullCondition converts an isNull boolean value into a squirrel condition.
func isNullCondition(quotedColumn string, value interface{}) (sq.Sqlizer, error) {
	boolVal, ok := value.(bool)
	if !ok {
		return nil, fmt.Errorf("isNull must be a boolean")
	}
	if boolVal {
		return sq.Eq{quotedColumn: nil}, nil
	}
	return sq.NotEq{quotedColumn: nil}, nil
}

// buildColumnFilter builds filter conditions for a specific column.
// When alias is non-empty, the column name is qualified as alias.column.
func buildColumnFilter(col introspection.Column, alias string, filterMap map[string]interface{}) ([]sq.Sqlizer, error) {
	conditions := []sq.Sqlizer{}
	quotedColumn := sqlutil.QuoteIdentifier(col.Name)
	if alias != "" {
		quotedColumn = fmt.Sprintf("%s.%s", sqlutil.QuoteIdentifier(alias), quotedColumn)
	}

	effectiveType := introspection.EffectiveGraphQLType(col)
	if effectiveType == sqltype.TypeSet {
		return buildSetColumnFilter(col, quotedColumn, filterMap)
	}
	if effectiveType == sqltype.TypeBytes {
		return buildBytesColumnFilter(quotedColumn, filterMap)
	}
	if effectiveType == sqltype.TypeUUID {
		return buildUUIDColumnFilter(col, quotedColumn, filterMap)
	}

	for op, value := range filterMap {
		switch op {
		case "eq":
			conditions = append(conditions, sq.Eq{quotedColumn: value})
		case "ne":
			conditions = append(conditions, sq.NotEq{quotedColumn: value})
		case "lt":
			conditions = append(conditions, sq.Lt{quotedColumn: value})
		case "lte":
			conditions = append(conditions, sq.LtOrEq{quotedColumn: value})
		case "gt":
			conditions = append(conditions, sq.Gt{quotedColumn: value})
		case "gte":
			conditions = append(conditions, sq.GtOrEq{quotedColumn: value})
		case "in":
			if arr, ok := value.([]interface{}); ok {
				conditions = append(conditions, sq.Eq{quotedColumn: arr})
			} else {
				return nil, fmt.Errorf("in operator requires an array")
			}
		case "notIn":
			if arr, ok := value.([]interface{}); ok {
				conditions = append(conditions, sq.NotEq{quotedColumn: arr})
			} else {
				return nil, fmt.Errorf("notIn operator requires an array")
			}
		case "like":
			conditions = append(conditions, sq.Like{quotedColumn: value})
		case "notLike":
			conditions = append(conditions, sq.NotLike{quotedColumn: value})
		case "isNull":
			cond, err := isNullCondition(quotedColumn, value)
			if err != nil {
				return nil, err
			}
			conditions = append(conditions, cond)
		default:
			return nil, fmt.Errorf("unknown filter operator: %s", op)
		}
	}

	return conditions, nil
}

func buildSetColumnFilter(col introspection.Column, quotedColumn string, filterMap map[string]interface{}) ([]sq.Sqlizer, error) {
	conditions := []sq.Sqlizer{}

	ops := make([]string, 0, len(filterMap))
	for op := range filterMap {
		ops = append(ops, op)
	}
	sort.Strings(ops)

	for _, op := range ops {
		value := filterMap[op]
		switch op {
		case "has":
			item, ok := value.(string)
			if !ok {
				return nil, fmt.Errorf("has operator requires a value")
			}
			csv, err := setutil.Canonicalize([]string{item}, col.EnumValues)
			if err != nil {
				return nil, err
			}
			conditions = append(conditions, sq.Expr(fmt.Sprintf("FIND_IN_SET(?, %s) > 0", quotedColumn), csv))
		case "hasAnyOf":
			items, err := setArrayValues(value)
			if err != nil {
				return nil, fmt.Errorf("hasAnyOf must be an array")
			}
			if len(items) == 0 {
				conditions = append(conditions, sq.Expr("1=0"))
				continue
			}
			anyConds := make([]sq.Sqlizer, 0, len(items))
			for _, item := range items {
				csv, err := setutil.Canonicalize([]string{item}, col.EnumValues)
				if err != nil {
					return nil, err
				}
				anyConds = append(anyConds, sq.Expr(fmt.Sprintf("FIND_IN_SET(?, %s) > 0", quotedColumn), csv))
			}
			conditions = append(conditions, sq.Or(anyConds))
		case "hasAllOf":
			items, err := setArrayValues(value)
			if err != nil {
				return nil, fmt.Errorf("hasAllOf must be an array")
			}
			if len(items) == 0 {
				conditions = append(conditions, sq.Expr("1=1"))
				continue
			}
			allConds := make([]sq.Sqlizer, 0, len(items))
			for _, item := range items {
				csv, err := setutil.Canonicalize([]string{item}, col.EnumValues)
				if err != nil {
					return nil, err
				}
				allConds = append(allConds, sq.Expr(fmt.Sprintf("FIND_IN_SET(?, %s) > 0", quotedColumn), csv))
			}
			conditions = append(conditions, sq.And(allConds))
		case "hasNoneOf":
			items, err := setArrayValues(value)
			if err != nil {
				return nil, fmt.Errorf("hasNoneOf must be an array")
			}
			if len(items) == 0 {
				conditions = append(conditions, sq.Expr("1=1"))
				continue
			}
			noneConds := make([]sq.Sqlizer, 0, len(items))
			for _, item := range items {
				csv, err := setutil.Canonicalize([]string{item}, col.EnumValues)
				if err != nil {
					return nil, err
				}
				noneConds = append(noneConds, sq.Expr(fmt.Sprintf("FIND_IN_SET(?, %s) = 0", quotedColumn), csv))
			}
			conditions = append(conditions, sq.And(noneConds))
		case "eq":
			csv, err := setutil.CanonicalizeAny(value, col.EnumValues)
			if err != nil {
				return nil, err
			}
			conditions = append(conditions, sq.Eq{quotedColumn: csv})
		case "ne":
			csv, err := setutil.CanonicalizeAny(value, col.EnumValues)
			if err != nil {
				return nil, err
			}
			conditions = append(conditions, sq.NotEq{quotedColumn: csv})
		case "isNull":
			cond, err := isNullCondition(quotedColumn, value)
			if err != nil {
				return nil, err
			}
			conditions = append(conditions, cond)
		default:
			return nil, fmt.Errorf("unknown set filter operator: %s", op)
		}
	}

	return conditions, nil
}

func setArrayValues(value interface{}) ([]string, error) {
	switch v := value.(type) {
	case []string:
		return append([]string(nil), v...), nil
	case []interface{}:
		out := make([]string, 0, len(v))
		for _, item := range v {
			str, ok := item.(string)
			if !ok {
				return nil, fmt.Errorf("set list items must be strings")
			}
			out = append(out, str)
		}
		return out, nil
	default:
		return nil, fmt.Errorf("set filter value must be an array")
	}
}

func buildBytesColumnFilter(quotedColumn string, filterMap map[string]interface{}) ([]sq.Sqlizer, error) {
	conditions := []sq.Sqlizer{}

	for op, value := range filterMap {
		switch op {
		case "eq":
			decoded, err := decodeBase64Bytes(value)
			if err != nil {
				return nil, err
			}
			conditions = append(conditions, sq.Eq{quotedColumn: decoded})
		case "ne":
			decoded, err := decodeBase64Bytes(value)
			if err != nil {
				return nil, err
			}
			conditions = append(conditions, sq.NotEq{quotedColumn: decoded})
		case "in":
			decoded, err := decodeBase64BytesList(value)
			if err != nil {
				return nil, err
			}
			conditions = append(conditions, sq.Eq{quotedColumn: decoded})
		case "notIn":
			decoded, err := decodeBase64BytesList(value)
			if err != nil {
				return nil, err
			}
			conditions = append(conditions, sq.NotEq{quotedColumn: decoded})
		case "isNull":
			cond, err := isNullCondition(quotedColumn, value)
			if err != nil {
				return nil, err
			}
			conditions = append(conditions, cond)
		case "lt", "lte", "gt", "gte", "like", "notLike":
			return nil, fmt.Errorf("operator %s is not supported for bytes columns", op)
		default:
			return nil, fmt.Errorf("unknown bytes filter operator: %s", op)
		}
	}

	return conditions, nil
}

func decodeBase64Bytes(value interface{}) ([]byte, error) {
	switch v := value.(type) {
	case []byte:
		// Bytes scalar ParseValue/ParseLiteral already decoded base64 for us.
		return v, nil
	case string:
		decoded, err := base64.StdEncoding.DecodeString(v)
		if err != nil {
			return nil, fmt.Errorf("invalid base64 value")
		}
		return decoded, nil
	default:
		return nil, fmt.Errorf("bytes filter value must be bytes or a base64 string")
	}
}

func decodeBase64BytesList(value interface{}) ([]interface{}, error) {
	arr, ok := value.([]interface{})
	if !ok {
		return nil, fmt.Errorf("bytes filter value must be an array")
	}
	out := make([]interface{}, 0, len(arr))
	for _, item := range arr {
		decoded, err := decodeBase64Bytes(item)
		if err != nil {
			return nil, err
		}
		out = append(out, decoded)
	}
	return out, nil
}

func buildUUIDColumnFilter(col introspection.Column, quotedColumn string, filterMap map[string]interface{}) ([]sq.Sqlizer, error) {
	conditions := []sq.Sqlizer{}
	binaryStorage := uuidutil.IsBinaryStorageType(col.DataType)

	for op, value := range filterMap {
		switch op {
		case "eq":
			parsed, err := parseUUIDFilterValue(value, binaryStorage)
			if err != nil {
				return nil, err
			}
			conditions = append(conditions, sq.Eq{quotedColumn: parsed})
		case "ne":
			parsed, err := parseUUIDFilterValue(value, binaryStorage)
			if err != nil {
				return nil, err
			}
			conditions = append(conditions, sq.NotEq{quotedColumn: parsed})
		case "in":
			parsed, err := parseUUIDFilterValueList(value, binaryStorage)
			if err != nil {
				return nil, err
			}
			conditions = append(conditions, sq.Eq{quotedColumn: parsed})
		case "notIn":
			parsed, err := parseUUIDFilterValueList(value, binaryStorage)
			if err != nil {
				return nil, err
			}
			conditions = append(conditions, sq.NotEq{quotedColumn: parsed})
		case "isNull":
			cond, err := isNullCondition(quotedColumn, value)
			if err != nil {
				return nil, err
			}
			conditions = append(conditions, cond)
		case "lt", "lte", "gt", "gte", "like", "notLike":
			return nil, fmt.Errorf("operator %s is not supported for UUID columns", op)
		default:
			return nil, fmt.Errorf("unknown UUID filter operator: %s", op)
		}
	}

	return conditions, nil
}

func parseUUIDFilterValue(value interface{}, binaryStorage bool) (interface{}, error) {
	var raw string
	switch v := value.(type) {
	case string:
		raw = v
	case []byte:
		raw = string(v)
	default:
		return nil, fmt.Errorf("UUID filter value must be a string")
	}
	parsed, canonical, err := uuidutil.ParseString(raw)
	if err != nil {
		return nil, fmt.Errorf("invalid UUID value")
	}
	if binaryStorage {
		return uuidutil.ToBytes(parsed), nil
	}
	return canonical, nil
}

func parseUUIDFilterValueList(value interface{}, binaryStorage bool) ([]interface{}, error) {
	arr, ok := value.([]interface{})
	if !ok {
		return nil, fmt.Errorf("UUID filter value must be an array")
	}
	out := make([]interface{}, 0, len(arr))
	for _, item := range arr {
		parsed, err := parseUUIDFilterValue(item, binaryStorage)
		if err != nil {
			return nil, err
		}
		out = append(out, parsed)
	}
	return out, nil
}

// findColumnByGraphQLName finds a column in the table by its GraphQL field name
func findColumnByGraphQLName(table introspection.Table, graphQLName string) *introspection.Column {
	for i := range table.Columns {
		if introspection.GraphQLFieldName(table.Columns[i]) == graphQLName {
			return &table.Columns[i]
		}
	}
	return nil
}

func findRelationshipByGraphQLName(table introspection.Table, graphQLName string) *introspection.Relationship {
	for i := range table.Relationships {
		if table.Relationships[i].GraphQLFieldName == graphQLName {
			return &table.Relationships[i]
		}
	}
	return nil
}

// indexedColumnSet returns the set of column names that participate in at least
// one index on the table, as a sorted slice. Built once and reused by both
// ValidateIndexedColumns and ValidateWhereClauseIndexes.
func indexedColumnSet(table introspection.Table) []string {
	seen := make(map[string]struct{})
	for _, idx := range table.Indexes {
		for _, col := range idx.Columns {
			seen[col] = struct{}{}
		}
	}
	cols := make([]string, 0, len(seen))
	for col := range seen {
		cols = append(cols, col)
	}
	sort.Strings(cols)
	return cols
}

// ValidateIndexedColumns checks if at least one indexed column is used in the WHERE clause.
func ValidateIndexedColumns(table introspection.Table, usedColumns []string) error {
	if len(usedColumns) == 0 {
		return nil
	}
	indexed := indexedColumnSet(table)
	indexedSet := make(map[string]struct{}, len(indexed))
	for _, col := range indexed {
		indexedSet[col] = struct{}{}
	}
	for _, col := range usedColumns {
		if _, ok := indexedSet[col]; ok {
			return nil
		}
	}
	return fmt.Errorf("where clause must include at least one indexed column for performance")
}

// ValidateWhereClauseIndexes validates indexed-column guardrails for all tables used by a WHERE clause.
func ValidateWhereClauseIndexes(schema *introspection.Schema, rootTable introspection.Table, whereClause *WhereClause) error {
	if whereClause == nil {
		return nil
	}
	if len(whereClause.UsedColumnsByTable) == 0 {
		return ValidateIndexedColumns(rootTable, whereClause.UsedColumns)
	}

	findTable := func(tableName string) (introspection.Table, bool) {
		if rootTable.Name == tableName {
			return rootTable, true
		}
		if schema == nil {
			return introspection.Table{}, false
		}
		for _, table := range schema.Tables {
			if table.Name == tableName {
				return table, true
			}
		}
		return introspection.Table{}, false
	}

	for tableName, cols := range whereClause.UsedColumnsByTable {
		table, ok := findTable(tableName)
		if !ok {
			return fmt.Errorf("where clause references unknown table %s for indexed validation", tableName)
		}
		if err := ValidateIndexedColumns(table, cols); err != nil {
			indexed := indexedColumnSet(table)
			if len(indexed) == 0 {
				return fmt.Errorf("where clause for table %s must include at least one indexed column for performance (table has no indexes)", tableName)
			}
			return fmt.Errorf(
				"where clause for table %s must include at least one indexed column for performance (indexed columns: %s)",
				tableName,
				strings.Join(indexed, ", "),
			)
		}
	}
	return nil
}

func validateLimitOffset(limit, offset int) error {
	if limit < 0 {
		return fmt.Errorf("limit must be non-negative")
	}
	if offset < 0 {
		return fmt.Errorf("offset must be non-negative")
	}
	return nil
}
