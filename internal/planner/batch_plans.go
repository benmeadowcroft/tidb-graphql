package planner

import (
	"fmt"
	"strings"

	"tidb-graphql/internal/introspection"
	"tidb-graphql/internal/sqlutil"

	sq "github.com/Masterminds/squirrel"
)

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
func validateLimitOffset(limit, offset int) error {
	if limit < 0 {
		return fmt.Errorf("limit must be non-negative")
	}
	if offset < 0 {
		return fmt.Errorf("offset must be non-negative")
	}
	return nil
}
