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
		whereSQL, whereArgs, err := buildTupleInCondition(quotedColumns(remoteColumns, ""), values)
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
	if len(values) == 0 {
		return SQLQuery{}, nil
	}
	if err := validateLimitOffset(limit, offset); err != nil {
		return SQLQuery{}, err
	}

	orderClause, err := batchOrderClause(targetTable, orderBy)
	if err != nil {
		return SQLQuery{}, err
	}

	cols := columnNames(targetTable, columns)
	columnList := strings.Join(cols, ", ")

	quotedTarget := sqlutil.QuoteIdentifier(targetTable.Name)
	quotedJunction := sqlutil.QuoteIdentifier(junctionTable)
	if len(junctionLocalFKColumns) == 0 || len(junctionRemoteFKColumns) == 0 || len(targetPKColumns) == 0 {
		return SQLQuery{}, fmt.Errorf("many-to-many batch requires key column mappings")
	}
	if len(junctionRemoteFKColumns) != len(targetPKColumns) {
		return SQLQuery{}, fmt.Errorf("many-to-many batch remote key mapping width mismatch")
	}

	partitionColumns := quotedColumns(junctionLocalFKColumns, quotedJunction)
	partitionExpr := strings.Join(partitionColumns, ", ")
	parentAliases := BatchParentAliases(len(partitionColumns))
	innerParentCols := make([]string, len(partitionColumns))
	outerParentCols := make([]string, len(partitionColumns))
	for i := range partitionColumns {
		innerParentCols[i] = fmt.Sprintf("%s AS %s", partitionColumns[i], parentAliases[i])
		outerParentCols[i] = parentAliases[i]
	}
	outerSelect := fmt.Sprintf("%s, %s", columnList, strings.Join(outerParentCols, ", "))
	innerSelect := fmt.Sprintf("%s, %s", columnList, strings.Join(innerParentCols, ", "))

	joinPredicates := make([]string, len(junctionRemoteFKColumns))
	for i := range junctionRemoteFKColumns {
		joinPredicates[i] = fmt.Sprintf(
			"%s.%s = %s.%s",
			quotedJunction, sqlutil.QuoteIdentifier(junctionRemoteFKColumns[i]),
			quotedTarget, sqlutil.QuoteIdentifier(targetPKColumns[i]),
		)
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

	query := fmt.Sprintf(
		"SELECT %s FROM (SELECT %s, ROW_NUMBER() OVER (PARTITION BY %s ORDER BY %s) AS __rn FROM %s INNER JOIN %s ON %s WHERE %s%s) AS __batch WHERE __rn > ? AND __rn <= ? ORDER BY %s, __rn",
		outerSelect,
		innerSelect,
		partitionExpr,
		orderClause,
		quotedTarget,
		quotedJunction,
		strings.Join(joinPredicates, " AND "),
		parentWhereSQL,
		whereSQL,
		strings.Join(outerParentCols, ", "),
	)

	args := append([]interface{}{}, parentWhereArgs...)
	args = append(args, whereArgs...)
	args = append(args, offset, offset+limit)
	return SQLQuery{SQL: query, Args: args}, nil
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
	if len(values) == 0 {
		return SQLQuery{}, nil
	}
	if err := validateLimitOffset(limit, offset); err != nil {
		return SQLQuery{}, err
	}

	orderClause, err := batchOrderClause(junctionTable, orderBy)
	if err != nil {
		return SQLQuery{}, err
	}

	cols := columnNames(junctionTable, columns)
	columnList := strings.Join(cols, ", ")
	if len(junctionLocalFKColumns) == 0 {
		return SQLQuery{}, fmt.Errorf("edge-list batch requires at least one local FK column")
	}

	quotedTable := sqlutil.QuoteIdentifier(junctionTable.Name)
	partitionColumns := quotedColumns(junctionLocalFKColumns, "")
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
		quotedTable,
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

func quotedColumns(columns []string, tableAlias string) []string {
	quoted := make([]string, len(columns))
	for i, col := range columns {
		if tableAlias == "" {
			quoted[i] = sqlutil.QuoteIdentifier(col)
			continue
		}
		quoted[i] = fmt.Sprintf("%s.%s", tableAlias, sqlutil.QuoteIdentifier(col))
	}
	return quoted
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
		clauses[i] = fmt.Sprintf("%s %s", sqlutil.QuoteIdentifier(col), orderBy.Direction)
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
	Condition   sq.Sqlizer
	UsedColumns []string
}

// BuildWhereClause parses a GraphQL WHERE input into a SQL WHERE clause.
// Returns the condition and a list of columns used (for indexed validation).
func BuildWhereClause(table introspection.Table, whereInput map[string]interface{}) (*WhereClause, error) {
	return buildWhereClauseWithAlias(table, "", whereInput)
}

// BuildWhereClauseQualified parses a GraphQL WHERE input into a SQL WHERE clause
// with qualified column names (alias.column).
func BuildWhereClauseQualified(table introspection.Table, alias string, whereInput map[string]interface{}) (*WhereClause, error) {
	return buildWhereClauseWithAlias(table, alias, whereInput)
}

func buildWhereClauseWithAlias(table introspection.Table, alias string, whereInput map[string]interface{}) (*WhereClause, error) {
	if len(whereInput) == 0 {
		return nil, nil
	}

	condition, usedCols, err := buildWhereCondition(table, alias, whereInput)
	if err != nil {
		return nil, err
	}

	return &WhereClause{
		Condition:   condition,
		UsedColumns: usedCols,
	}, nil
}

// buildWhereCondition recursively builds WHERE conditions with AND/OR support.
// When alias is non-empty, column names are qualified as alias.column.
func buildWhereCondition(table introspection.Table, alias string, whereInput map[string]interface{}) (sq.Sqlizer, []string, error) {
	usedColumns := []string{}
	conditions := []sq.Sqlizer{}

	for key, value := range whereInput {
		switch key {
		case "AND":
			andArray, ok := value.([]interface{})
			if !ok {
				return nil, nil, fmt.Errorf("AND must be an array")
			}
			andConditions := []sq.Sqlizer{}
			for _, item := range andArray {
				itemMap, ok := item.(map[string]interface{})
				if !ok {
					return nil, nil, fmt.Errorf("AND array items must be objects")
				}
				cond, cols, err := buildWhereCondition(table, alias, itemMap)
				if err != nil {
					return nil, nil, err
				}
				if cond != nil {
					andConditions = append(andConditions, cond)
					usedColumns = append(usedColumns, cols...)
				}
			}
			if len(andConditions) > 0 {
				conditions = append(conditions, sq.And(andConditions))
			}

		case "OR":
			orArray, ok := value.([]interface{})
			if !ok {
				return nil, nil, fmt.Errorf("OR must be an array")
			}
			orConditions := []sq.Sqlizer{}
			for _, item := range orArray {
				itemMap, ok := item.(map[string]interface{})
				if !ok {
					return nil, nil, fmt.Errorf("OR array items must be objects")
				}
				cond, cols, err := buildWhereCondition(table, alias, itemMap)
				if err != nil {
					return nil, nil, err
				}
				if cond != nil {
					orConditions = append(orConditions, cond)
					usedColumns = append(usedColumns, cols...)
				}
			}
			if len(orConditions) > 0 {
				conditions = append(conditions, sq.Or(orConditions))
			}

		default:
			col := findColumnByGraphQLName(table, key)
			if col == nil {
				return nil, nil, fmt.Errorf("unknown column: %s", key)
			}
			usedColumns = append(usedColumns, col.Name)

			filterMap, ok := value.(map[string]interface{})
			if !ok {
				return nil, nil, fmt.Errorf("filter for %s must be an object", key)
			}

			colConditions, err := buildColumnFilter(*col, alias, filterMap)
			if err != nil {
				return nil, nil, err
			}
			conditions = append(conditions, colConditions...)
		}
	}

	if len(conditions) == 0 {
		return nil, usedColumns, nil
	}
	if len(conditions) == 1 {
		return conditions[0], usedColumns, nil
	}
	return sq.And(conditions), usedColumns, nil
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
			boolVal, ok := value.(bool)
			if !ok {
				return nil, fmt.Errorf("isNull must be a boolean")
			}
			if boolVal {
				conditions = append(conditions, sq.Eq{quotedColumn: nil})
			} else {
				conditions = append(conditions, sq.NotEq{quotedColumn: nil})
			}
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
			boolVal, ok := value.(bool)
			if !ok {
				return nil, fmt.Errorf("isNull must be a boolean")
			}
			if boolVal {
				conditions = append(conditions, sq.Eq{quotedColumn: nil})
			} else {
				conditions = append(conditions, sq.NotEq{quotedColumn: nil})
			}
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
			boolVal, ok := value.(bool)
			if !ok {
				return nil, fmt.Errorf("isNull must be a boolean")
			}
			if boolVal {
				conditions = append(conditions, sq.Eq{quotedColumn: nil})
			} else {
				conditions = append(conditions, sq.NotEq{quotedColumn: nil})
			}
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
			boolVal, ok := value.(bool)
			if !ok {
				return nil, fmt.Errorf("isNull must be a boolean")
			}
			if boolVal {
				conditions = append(conditions, sq.Eq{quotedColumn: nil})
			} else {
				conditions = append(conditions, sq.NotEq{quotedColumn: nil})
			}
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

// ValidateIndexedColumns checks if at least one indexed column is used in the WHERE clause
func ValidateIndexedColumns(table introspection.Table, usedColumns []string) error {
	if len(usedColumns) == 0 {
		return nil // No WHERE clause, no validation needed
	}

	// Collect all indexed columns
	indexedColumns := make(map[string]bool)
	for _, idx := range table.Indexes {
		for _, col := range idx.Columns {
			indexedColumns[col] = true
		}
	}

	// Check if any used column is indexed
	for _, col := range usedColumns {
		if indexedColumns[col] {
			return nil // At least one indexed column found
		}
	}

	return fmt.Errorf(
		"where clause must include at least one indexed column for performance",
	)
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
