package planner

import (
	"fmt"
	"strings"

	"tidb-graphql/internal/cursor"
	"tidb-graphql/internal/introspection"
	"tidb-graphql/internal/sqlutil"

	sq "github.com/Masterminds/squirrel"
	"github.com/graphql-go/graphql/language/ast"
)

const (
	// DefaultConnectionLimit is the default page size for connection queries.
	DefaultConnectionLimit = 25
	// MaxConnectionLimit is the maximum allowed page size.
	MaxConnectionLimit = 100
)

type PaginationMode string

const (
	PaginationModeForward  PaginationMode = "forward"
	PaginationModeBackward PaginationMode = "backward"
)

// ConnectionPlan holds the planned SQL for a connection query.
type ConnectionPlan struct {
	Root          SQLQuery // main data query (first+1 rows)
	Count         SQLQuery // totalCount query (filter only, no cursor)
	AggregateBase SQLQuery // base dataset query used for aggregate fields
	Table         introspection.Table
	Columns       []introspection.Column // selected + orderBy + PK columns
	OrderBy       *OrderBy
	OrderByKey    string                 // GraphQL orderBy field name
	CursorColumns []introspection.Column // columns encoded in cursor
	First         int
	Mode          PaginationMode
	HasCursor     bool // whether any cursor was provided
	HasAfter      bool
	HasBefore     bool
}

// connectionArgs holds the parsed common arguments for connection queries.
type connectionArgs struct {
	limit         int
	orderBy       *OrderBy
	sqlOrderBy    *OrderBy
	orderByKey    string
	typeName      string
	cursorCols    []introspection.Column
	seekCondition sq.Sqlizer
	mode          PaginationMode
	hasAfter      bool
	hasBefore     bool
	whereClause   *WhereClause
	selected      []introspection.Column
}

type connectionWindow struct {
	mode      PaginationMode
	limit     int
	hasAfter  bool
	hasBefore bool
	after     string
	before    string
}

// seekBuilder builds a cursor seek condition from column names, values, and direction.
type seekBuilder func(colNames []string, values []interface{}, direction string) sq.Sqlizer

// whereBuilder builds a WHERE clause for a table from a filter input map.
type whereBuilder func(table introspection.Table, whereMap map[string]interface{}) (*WhereClause, error)

// parseConnectionArgs parses the common arguments shared by all connection planning functions.
func parseConnectionArgs(
	table introspection.Table,
	field *ast.Field,
	args map[string]interface{},
	buildSeek seekBuilder,
	buildWhere whereBuilder,
	options *planOptions,
) (*connectionArgs, error) {
	defaultLimit := DefaultConnectionLimit
	if options.defaultLimit > 0 {
		defaultLimit = options.defaultLimit
	}

	if options.limits != nil {
		cost := EstimateCost(field, args, defaultLimit, options.fragments)
		if err := validateLimits(cost, *options.limits); err != nil {
			return nil, err
		}
	}

	pkCols := introspection.PrimaryKeyColumns(table)
	if len(pkCols) == 0 {
		return nil, fmt.Errorf("connections require a primary key on table %s", table.Name)
	}

	window, err := parseConnectionWindowWithDefault(args, defaultLimit)
	if err != nil {
		return nil, err
	}

	orderBy, err := parseConnectionOrderBy(table, args, pkCols)
	if err != nil {
		return nil, err
	}

	orderByKey := OrderByKey(table, orderBy.Columns)
	typeName := introspection.GraphQLTypeName(table)
	cursorCols := CursorColumns(table, orderBy)
	sqlOrderBy := orderBy
	if window.mode == PaginationModeBackward {
		sqlOrderBy = reverseOrderBy(orderBy)
	}

	// Parse cursor
	var seekCondition sq.Sqlizer
	if window.hasAfter || window.hasBefore {
		cursorArgName := "after"
		rawCursor := window.after
		seekDirection := orderBy.Direction
		if window.hasBefore {
			cursorArgName = "before"
			rawCursor = window.before
			seekDirection = reverseDirection(orderBy.Direction)
		}

		cType, cKey, cDir, cVals, err := cursor.DecodeCursor(rawCursor)
		if err != nil {
			return nil, fmt.Errorf("invalid %s cursor: %w", cursorArgName, err)
		}
		if err := cursor.ValidateCursor(typeName, orderByKey, orderBy.Direction, cType, cKey, cDir); err != nil {
			return nil, fmt.Errorf("invalid %s cursor: %w", cursorArgName, err)
		}
		parsedValues, err := cursor.ParseCursorValues(cVals, cursorCols)
		if err != nil {
			return nil, fmt.Errorf("invalid %s cursor: %w", cursorArgName, err)
		}
		colNames := make([]string, len(cursorCols))
		for i, c := range cursorCols {
			colNames[i] = c.Name
		}
		seekCondition = buildSeek(colNames, parsedValues, seekDirection)
	}

	// Parse WHERE clause
	var whereClause *WhereClause
	if whereArg, ok := args["where"]; ok {
		if whereMap, ok := whereArg.(map[string]interface{}); ok {
			whereClause, err = buildWhere(table, whereMap)
			if err != nil {
				return nil, fmt.Errorf("invalid WHERE clause: %w", err)
			}
			if whereClause != nil {
				if err := ValidateIndexedColumns(table, whereClause.UsedColumns); err != nil {
					return nil, err
				}
			}
		}
	}

	// Resolve selected columns from connection selection set
	selected := SelectedColumnsForConnection(table, field, options.fragments, orderBy)

	return &connectionArgs{
		limit:         window.limit,
		orderBy:       orderBy,
		sqlOrderBy:    sqlOrderBy,
		orderByKey:    orderByKey,
		typeName:      typeName,
		cursorCols:    cursorCols,
		seekCondition: seekCondition,
		mode:          window.mode,
		hasAfter:      window.hasAfter,
		hasBefore:     window.hasBefore,
		whereClause:   whereClause,
		selected:      selected,
	}, nil
}

// PlanConnection plans a root connection query.
func PlanConnection(
	schema *introspection.Schema,
	table introspection.Table,
	field *ast.Field,
	args map[string]interface{},
	opts ...PlanOption,
) (*ConnectionPlan, error) {
	options := &planOptions{}
	for _, opt := range opts {
		opt(options)
	}

	ca, err := parseConnectionArgs(table, field, args, BuildSeekCondition, BuildWhereClause, options)
	if err != nil {
		return nil, err
	}

	// Build root SQL: SELECT columns WHERE (filter AND seek) ORDER BY LIMIT pageSize+1
	rootSQL, err := buildConnectionSQL(table, ca.selected, ca.whereClause, ca.seekCondition, ca.sqlOrderBy, ca.limit+1)
	if err != nil {
		return nil, err
	}

	// Build count SQL: SELECT COUNT(*) WHERE (filter only)
	countSQL, err := buildCountSQL(table, ca.whereClause)
	if err != nil {
		return nil, err
	}
	aggregateBase, err := buildRootAggregateBaseSQL(table, ca.whereClause)
	if err != nil {
		return nil, err
	}

	return &ConnectionPlan{
		Root:          rootSQL,
		Count:         countSQL,
		AggregateBase: aggregateBase,
		Table:         table,
		Columns:       ca.selected,
		OrderBy:       ca.orderBy,
		OrderByKey:    ca.orderByKey,
		CursorColumns: ca.cursorCols,
		First:         ca.limit,
		Mode:          ca.mode,
		HasCursor:     ca.hasAfter || ca.hasBefore,
		HasAfter:      ca.hasAfter,
		HasBefore:     ca.hasBefore,
	}, nil
}

// PlanOneToManyConnection plans a connection for a one-to-many relationship.
func PlanOneToManyConnection(
	table introspection.Table,
	remoteColumn string,
	fkValue interface{},
	field *ast.Field,
	args map[string]interface{},
	opts ...PlanOption,
) (*ConnectionPlan, error) {
	options := &planOptions{}
	for _, opt := range opts {
		opt(options)
	}

	ca, err := parseConnectionArgs(table, field, args, BuildSeekCondition, BuildWhereClause, options)
	if err != nil {
		return nil, err
	}

	// FK filter: WHERE remoteColumn = fkValue
	fkCondition := sq.Eq{sqlutil.QuoteIdentifier(remoteColumn): fkValue}

	// Build root SQL with FK + seek
	builder := sq.Select(columnNames(table, ca.selected)...).
		From(sqlutil.QuoteIdentifier(table.Name)).
		Where(fkCondition)

	if ca.whereClause != nil && ca.whereClause.Condition != nil {
		builder = builder.Where(ca.whereClause.Condition)
	}
	if ca.seekCondition != nil {
		builder = builder.Where(ca.seekCondition)
	}

	builder = builder.OrderBy(orderByClauses(ca.sqlOrderBy)...).
		Limit(uint64(ca.limit + 1)).
		PlaceholderFormat(sq.Question)

	query, sqlArgs, err := builder.ToSql()
	if err != nil {
		return nil, err
	}

	countQuery, err := BuildOneToManyCountSQL(table, remoteColumn, fkValue, ca.whereClause)
	if err != nil {
		return nil, err
	}
	aggregateBase, err := BuildOneToManyAggregateBaseSQL(table, remoteColumn, fkValue, ca.whereClause)
	if err != nil {
		return nil, err
	}

	return &ConnectionPlan{
		Root:          SQLQuery{SQL: query, Args: sqlArgs},
		Count:         countQuery,
		AggregateBase: aggregateBase,
		Table:         table,
		Columns:       ca.selected,
		OrderBy:       ca.orderBy,
		OrderByKey:    ca.orderByKey,
		CursorColumns: ca.cursorCols,
		First:         ca.limit,
		Mode:          ca.mode,
		HasCursor:     ca.hasAfter || ca.hasBefore,
		HasAfter:      ca.hasAfter,
		HasBefore:     ca.hasBefore,
	}, nil
}

// PlanManyToManyConnection plans a connection for a many-to-many relationship.
func PlanManyToManyConnection(
	targetTable introspection.Table,
	junctionTable string,
	junctionLocalFKColumns []string,
	junctionRemoteFKColumns []string,
	targetPKColumns []string,
	fkValues []interface{},
	field *ast.Field,
	args map[string]interface{},
	opts ...PlanOption,
) (*ConnectionPlan, error) {
	options := &planOptions{}
	for _, opt := range opts {
		opt(options)
	}

	buildSeek := func(colNames []string, values []interface{}, direction string) sq.Sqlizer {
		return BuildSeekConditionQualified(targetTable.Name, colNames, values, direction)
	}
	buildWhere := func(table introspection.Table, whereMap map[string]interface{}) (*WhereClause, error) {
		return BuildWhereClauseQualified(table, table.Name, whereMap)
	}

	ca, err := parseConnectionArgs(targetTable, field, args, buildSeek, buildWhere, options)
	if err != nil {
		return nil, err
	}

	quotedTarget := sqlutil.QuoteIdentifier(targetTable.Name)
	quotedJunction := sqlutil.QuoteIdentifier(junctionTable)
	if len(junctionLocalFKColumns) == 0 || len(junctionLocalFKColumns) != len(fkValues) {
		return nil, fmt.Errorf("many-to-many local FK mapping width mismatch")
	}
	if len(junctionRemoteFKColumns) == 0 || len(junctionRemoteFKColumns) != len(targetPKColumns) {
		return nil, fmt.Errorf("many-to-many remote FK mapping width mismatch")
	}
	joinPredicates := make([]string, len(junctionRemoteFKColumns))
	for i := range junctionRemoteFKColumns {
		joinPredicates[i] = fmt.Sprintf(
			"%s.%s = %s.%s",
			quotedJunction, sqlutil.QuoteIdentifier(junctionRemoteFKColumns[i]),
			quotedTarget, sqlutil.QuoteIdentifier(targetPKColumns[i]),
		)
	}
	localFKQuoted := quotedColumns(junctionLocalFKColumns, quotedJunction)
	localWhereSQL, localWhereArgs, err := buildTupleInCondition(localFKQuoted, []ParentTuple{{Values: fkValues}})
	if err != nil {
		return nil, err
	}
	if localWhereSQL == "" {
		return nil, fmt.Errorf("many-to-many local key filter is empty")
	}

	builder := sq.Select(columnNamesQualified(targetTable.Name, ca.selected)...).
		From(quotedTarget).
		Join(fmt.Sprintf("%s ON %s", quotedJunction, strings.Join(joinPredicates, " AND "))).
		Where(sq.Expr(localWhereSQL, localWhereArgs...))

	if ca.whereClause != nil && ca.whereClause.Condition != nil {
		builder = builder.Where(ca.whereClause.Condition)
	}
	if ca.seekCondition != nil {
		builder = builder.Where(ca.seekCondition)
	}

	builder = builder.OrderBy(orderByClausesQualified(targetTable.Name, ca.sqlOrderBy)...).
		Limit(uint64(ca.limit + 1)).
		PlaceholderFormat(sq.Question)

	query, sqlArgs, err := builder.ToSql()
	if err != nil {
		return nil, err
	}

	countQuery, err := BuildManyToManyCountSQL(targetTable, junctionTable, junctionLocalFKColumns, junctionRemoteFKColumns, targetPKColumns, fkValues, ca.whereClause)
	if err != nil {
		return nil, err
	}
	aggregateBase, err := BuildManyToManyAggregateBaseSQL(targetTable, junctionTable, junctionLocalFKColumns, junctionRemoteFKColumns, targetPKColumns, fkValues, ca.whereClause)
	if err != nil {
		return nil, err
	}

	return &ConnectionPlan{
		Root:          SQLQuery{SQL: query, Args: sqlArgs},
		Count:         countQuery,
		AggregateBase: aggregateBase,
		Table:         targetTable,
		Columns:       ca.selected,
		OrderBy:       ca.orderBy,
		OrderByKey:    ca.orderByKey,
		CursorColumns: ca.cursorCols,
		First:         ca.limit,
		Mode:          ca.mode,
		HasCursor:     ca.hasAfter || ca.hasBefore,
		HasAfter:      ca.hasAfter,
		HasBefore:     ca.hasBefore,
	}, nil
}

// PlanEdgeListConnection plans a connection for an edge-list relationship.
func PlanEdgeListConnection(
	junctionTable introspection.Table,
	junctionLocalFKColumns []string,
	fkValues []interface{},
	field *ast.Field,
	args map[string]interface{},
	opts ...PlanOption,
) (*ConnectionPlan, error) {
	options := &planOptions{}
	for _, opt := range opts {
		opt(options)
	}

	ca, err := parseConnectionArgs(junctionTable, field, args, BuildSeekCondition, BuildWhereClause, options)
	if err != nil {
		return nil, err
	}

	if len(junctionLocalFKColumns) == 0 || len(junctionLocalFKColumns) != len(fkValues) {
		return nil, fmt.Errorf("edge-list local FK mapping width mismatch")
	}
	whereSQL, whereArgs, err := buildTupleInCondition(quotedColumns(junctionLocalFKColumns, ""), []ParentTuple{{Values: fkValues}})
	if err != nil {
		return nil, err
	}
	if whereSQL == "" {
		return nil, fmt.Errorf("edge-list local key filter is empty")
	}
	builder := sq.Select(columnNames(junctionTable, ca.selected)...).
		From(sqlutil.QuoteIdentifier(junctionTable.Name)).
		Where(sq.Expr(whereSQL, whereArgs...))

	if ca.whereClause != nil && ca.whereClause.Condition != nil {
		builder = builder.Where(ca.whereClause.Condition)
	}
	if ca.seekCondition != nil {
		builder = builder.Where(ca.seekCondition)
	}

	builder = builder.OrderBy(orderByClauses(ca.sqlOrderBy)...).
		Limit(uint64(ca.limit + 1)).
		PlaceholderFormat(sq.Question)

	query, sqlArgs, err := builder.ToSql()
	if err != nil {
		return nil, err
	}

	countQuery, err := BuildEdgeListCountSQL(junctionTable, junctionLocalFKColumns, fkValues, ca.whereClause)
	if err != nil {
		return nil, err
	}
	aggregateBase, err := BuildEdgeListAggregateBaseSQL(junctionTable, junctionLocalFKColumns, fkValues, ca.whereClause)
	if err != nil {
		return nil, err
	}

	return &ConnectionPlan{
		Root:          SQLQuery{SQL: query, Args: sqlArgs},
		Count:         countQuery,
		AggregateBase: aggregateBase,
		Table:         junctionTable,
		Columns:       ca.selected,
		OrderBy:       ca.orderBy,
		OrderByKey:    ca.orderByKey,
		CursorColumns: ca.cursorCols,
		First:         ca.limit,
		Mode:          ca.mode,
		HasCursor:     ca.hasAfter || ca.hasBefore,
		HasAfter:      ca.hasAfter,
		HasBefore:     ca.hasBefore,
	}, nil
}

// BuildOneToManyCountSQL builds the count query for a one-to-many connection.
func BuildOneToManyCountSQL(
	table introspection.Table,
	remoteColumn string,
	fkValue interface{},
	whereClause *WhereClause,
) (SQLQuery, error) {
	base, err := BuildOneToManyAggregateBaseSQL(table, remoteColumn, fkValue, whereClause)
	if err != nil {
		return SQLQuery{}, err
	}
	return buildCountFromBaseSQL(base), nil
}

// BuildManyToManyCountSQL builds the count query for a many-to-many connection.
func BuildManyToManyCountSQL(
	targetTable introspection.Table,
	junctionTable string,
	junctionLocalFKColumns []string,
	junctionRemoteFKColumns []string,
	targetPKColumns []string,
	fkValues []interface{},
	whereClause *WhereClause,
) (SQLQuery, error) {
	base, err := BuildManyToManyAggregateBaseSQL(
		targetTable,
		junctionTable,
		junctionLocalFKColumns,
		junctionRemoteFKColumns,
		targetPKColumns,
		fkValues,
		whereClause,
	)
	if err != nil {
		return SQLQuery{}, err
	}
	return buildCountFromBaseSQL(base), nil
}

// BuildManyToManyAggregateBaseSQL builds the base rowset query for many-to-many aggregates.
func BuildManyToManyAggregateBaseSQL(
	targetTable introspection.Table,
	junctionTable string,
	junctionLocalFKColumns []string,
	junctionRemoteFKColumns []string,
	targetPKColumns []string,
	fkValues []interface{},
	whereClause *WhereClause,
) (SQLQuery, error) {
	quotedTarget := sqlutil.QuoteIdentifier(targetTable.Name)
	quotedJunction := sqlutil.QuoteIdentifier(junctionTable)
	if len(junctionLocalFKColumns) == 0 || len(junctionLocalFKColumns) != len(fkValues) {
		return SQLQuery{}, fmt.Errorf("many-to-many aggregate local key mapping width mismatch")
	}
	if len(junctionRemoteFKColumns) == 0 || len(junctionRemoteFKColumns) != len(targetPKColumns) {
		return SQLQuery{}, fmt.Errorf("many-to-many aggregate remote key mapping width mismatch")
	}
	joinPredicates := make([]string, len(junctionRemoteFKColumns))
	for i := range junctionRemoteFKColumns {
		joinPredicates[i] = fmt.Sprintf(
			"%s.%s = %s.%s",
			quotedJunction, sqlutil.QuoteIdentifier(junctionRemoteFKColumns[i]),
			quotedTarget, sqlutil.QuoteIdentifier(targetPKColumns[i]),
		)
	}
	whereSQL, whereArgs, err := buildTupleInCondition(
		quotedColumns(junctionLocalFKColumns, quotedJunction),
		[]ParentTuple{{Values: fkValues}},
	)
	if err != nil {
		return SQLQuery{}, err
	}
	if whereSQL == "" {
		return SQLQuery{}, fmt.Errorf("many-to-many aggregate local key filter is empty")
	}

	builder := sq.Select("*").
		From(quotedTarget).
		Join(fmt.Sprintf("%s ON %s", quotedJunction, strings.Join(joinPredicates, " AND "))).
		Where(sq.Expr(whereSQL, whereArgs...))

	if whereClause != nil && whereClause.Condition != nil {
		builder = builder.Where(whereClause.Condition)
	}

	query, args, err := builder.PlaceholderFormat(sq.Question).ToSql()
	if err != nil {
		return SQLQuery{}, err
	}
	return SQLQuery{SQL: query, Args: args}, nil
}

// BuildEdgeListCountSQL builds the count query for an edge-list connection.
func BuildEdgeListCountSQL(
	junctionTable introspection.Table,
	junctionLocalFKColumns []string,
	fkValues []interface{},
	whereClause *WhereClause,
) (SQLQuery, error) {
	base, err := BuildEdgeListAggregateBaseSQL(junctionTable, junctionLocalFKColumns, fkValues, whereClause)
	if err != nil {
		return SQLQuery{}, err
	}
	return buildCountFromBaseSQL(base), nil
}

// BuildOneToManyAggregateBaseSQL builds the base rowset query for one-to-many aggregates.
func BuildOneToManyAggregateBaseSQL(
	table introspection.Table,
	remoteColumn string,
	fkValue interface{},
	whereClause *WhereClause,
) (SQLQuery, error) {
	builder := sq.Select("*").
		From(sqlutil.QuoteIdentifier(table.Name)).
		Where(sq.Eq{sqlutil.QuoteIdentifier(remoteColumn): fkValue})

	if whereClause != nil && whereClause.Condition != nil {
		builder = builder.Where(whereClause.Condition)
	}

	query, args, err := builder.PlaceholderFormat(sq.Question).ToSql()
	if err != nil {
		return SQLQuery{}, err
	}
	return SQLQuery{SQL: query, Args: args}, nil
}

// BuildEdgeListAggregateBaseSQL builds the base rowset query for edge-list aggregates.
func BuildEdgeListAggregateBaseSQL(
	junctionTable introspection.Table,
	junctionLocalFKColumns []string,
	fkValues []interface{},
	whereClause *WhereClause,
) (SQLQuery, error) {
	if len(junctionLocalFKColumns) == 0 || len(junctionLocalFKColumns) != len(fkValues) {
		return SQLQuery{}, fmt.Errorf("edge-list aggregate local key mapping width mismatch")
	}
	whereSQL, whereArgs, err := buildTupleInCondition(
		quotedColumns(junctionLocalFKColumns, ""),
		[]ParentTuple{{Values: fkValues}},
	)
	if err != nil {
		return SQLQuery{}, err
	}
	if whereSQL == "" {
		return SQLQuery{}, fmt.Errorf("edge-list aggregate local key filter is empty")
	}
	builder := sq.Select("*").
		From(sqlutil.QuoteIdentifier(junctionTable.Name)).
		Where(sq.Expr(whereSQL, whereArgs...))

	if whereClause != nil && whereClause.Condition != nil {
		builder = builder.Where(whereClause.Condition)
	}

	query, args, err := builder.PlaceholderFormat(sq.Question).ToSql()
	if err != nil {
		return SQLQuery{}, err
	}
	return SQLQuery{SQL: query, Args: args}, nil
}

// BuildSeekCondition creates a SQL row comparison for cursor-based seek.
// For ASC: (col1, col2) > (?, ?)
// For DESC: (col1, col2) < (?, ?)
func BuildSeekCondition(columns []string, values []interface{}, direction string) sq.Sqlizer {
	quoted := make([]string, len(columns))
	for i, col := range columns {
		quoted[i] = sqlutil.QuoteIdentifier(col)
	}

	lhs := "(" + strings.Join(quoted, ", ") + ")"
	placeholders := make([]string, len(values))
	for i := range values {
		placeholders[i] = "?"
	}
	rhs := "(" + strings.Join(placeholders, ", ") + ")"

	op := ">"
	if strings.ToUpper(direction) == "DESC" {
		op = "<"
	}

	return sq.Expr(lhs+" "+op+" "+rhs, values...)
}

// BuildSeekConditionQualified creates a SQL row comparison for cursor-based seek
// using qualified column names (tableAlias.column).
func BuildSeekConditionQualified(tableAlias string, columns []string, values []interface{}, direction string) sq.Sqlizer {
	qualified := make([]string, len(columns))
	for i, col := range columns {
		qualified[i] = fmt.Sprintf("%s.%s", sqlutil.QuoteIdentifier(tableAlias), sqlutil.QuoteIdentifier(col))
	}

	lhs := "(" + strings.Join(qualified, ", ") + ")"
	placeholders := make([]string, len(values))
	for i := range values {
		placeholders[i] = "?"
	}
	rhs := "(" + strings.Join(placeholders, ", ") + ")"

	op := ">"
	if strings.ToUpper(direction) == "DESC" {
		op = "<"
	}

	return sq.Expr(lhs+" "+op+" "+rhs, values...)
}

func columnNamesQualified(tableName string, columns []introspection.Column) []string {
	names := make([]string, len(columns))
	for i, col := range columns {
		names[i] = fmt.Sprintf("%s.%s", sqlutil.QuoteIdentifier(tableName), sqlutil.QuoteIdentifier(col.Name))
	}
	return names
}

func orderByClausesQualified(tableName string, orderBy *OrderBy) []string {
	if orderBy == nil {
		return nil
	}
	clauses := make([]string, len(orderBy.Columns))
	for i, col := range orderBy.Columns {
		clauses[i] = fmt.Sprintf("%s.%s %s", sqlutil.QuoteIdentifier(tableName), sqlutil.QuoteIdentifier(col), orderBy.Direction)
	}
	return clauses
}

// PlanOneToManyConnectionBatch builds a batched SQL query for one-to-many connections.
// It uses offset=0 and limit=first+1 to allow per-parent hasNextPage detection.
func PlanOneToManyConnectionBatch(
	relatedTable introspection.Table,
	remoteColumn string,
	columns []introspection.Column,
	parentValues []interface{},
	first int,
	orderBy *OrderBy,
	whereClause *WhereClause,
) (SQLQuery, error) {
	return PlanOneToManyBatch(
		relatedTable,
		columns,
		remoteColumn,
		parentValues,
		first+1,
		0,
		orderBy,
		whereClause,
	)
}

// PlanManyToManyConnectionBatch builds a batched SQL query for many-to-many connections.
// It uses offset=0 and limit=first+1 to allow per-parent hasNextPage detection.
func PlanManyToManyConnectionBatch(
	targetTable introspection.Table,
	junctionTable string,
	junctionLocalFKColumns []string,
	junctionRemoteFKColumns []string,
	targetPKColumns []string,
	columns []introspection.Column,
	parentValues []ParentTuple,
	first int,
	orderBy *OrderBy,
	whereClause *WhereClause,
) (SQLQuery, error) {
	return PlanManyToManyBatch(
		junctionTable,
		targetTable,
		junctionLocalFKColumns,
		junctionRemoteFKColumns,
		targetPKColumns,
		columns,
		parentValues,
		first+1,
		0,
		orderBy,
		whereClause,
	)
}

// PlanEdgeListConnectionBatch builds a batched SQL query for edge-list connections.
// It uses offset=0 and limit=first+1 to allow per-parent hasNextPage detection.
func PlanEdgeListConnectionBatch(
	junctionTable introspection.Table,
	junctionLocalFKColumns []string,
	columns []introspection.Column,
	parentValues []ParentTuple,
	first int,
	orderBy *OrderBy,
	whereClause *WhereClause,
) (SQLQuery, error) {
	return PlanEdgeListBatch(
		junctionTable,
		junctionLocalFKColumns,
		columns,
		parentValues,
		first+1,
		0,
		orderBy,
		whereClause,
	)
}

// parseConnectionWindowWithDefault parses connection pagination arguments and
// resolves the pagination mode and requested page size.
func parseConnectionWindowWithDefault(args map[string]interface{}, fallback int) (connectionWindow, error) {
	if fallback <= 0 {
		fallback = DefaultConnectionLimit
	}
	window := connectionWindow{
		mode:  PaginationModeForward,
		limit: normalizeFirstLimit(fallback),
	}
	if args == nil {
		return window, nil
	}

	first, hasFirst, err := parseConnectionLimitArg(args, "first")
	if err != nil {
		return connectionWindow{}, err
	}
	last, hasLast, err := parseConnectionLimitArg(args, "last")
	if err != nil {
		return connectionWindow{}, err
	}

	after, hasAfter, err := parseOptionalStringArg(args, "after")
	if err != nil {
		return connectionWindow{}, err
	}
	before, hasBefore, err := parseOptionalStringArg(args, "before")
	if err != nil {
		return connectionWindow{}, err
	}

	if hasFirst && hasLast {
		return connectionWindow{}, fmt.Errorf("cannot use both first and last")
	}
	if hasAfter && hasBefore {
		return connectionWindow{}, fmt.Errorf("cannot use both after and before")
	}
	if hasBefore && !hasLast {
		return connectionWindow{}, fmt.Errorf("before requires last")
	}
	if hasLast && hasAfter {
		return connectionWindow{}, fmt.Errorf("last cannot be used with after")
	}
	if hasFirst && hasBefore {
		return connectionWindow{}, fmt.Errorf("before cannot be used with first")
	}

	if hasLast {
		window.mode = PaginationModeBackward
		window.limit = last
	} else if hasFirst {
		window.mode = PaginationModeForward
		window.limit = first
	}

	window.hasAfter = hasAfter
	window.after = after
	window.hasBefore = hasBefore
	window.before = before

	return window, nil
}

// ParseFirst extracts the "first" argument for connection queries.
func ParseFirst(args map[string]interface{}) (int, error) {
	return ParseFirstWithDefault(args, DefaultConnectionLimit)
}

// ParseFirstWithDefault extracts the "first" argument for connection queries
// using the supplied fallback when the argument is omitted.
func ParseFirstWithDefault(args map[string]interface{}, fallback int) (int, error) {
	if fallback <= 0 {
		fallback = DefaultConnectionLimit
	}
	if args == nil {
		return normalizeFirstLimit(fallback), nil
	}
	value, hasFirst, err := parseConnectionLimitArg(args, "first")
	if err != nil {
		return 0, err
	}
	if !hasFirst {
		return normalizeFirstLimit(fallback), nil
	}
	return value, nil
}

func normalizeFirstLimit(limit int) int {
	if limit < 0 {
		return 0
	}
	if limit > MaxConnectionLimit {
		return MaxConnectionLimit
	}
	return limit
}

func parseConnectionLimitArg(args map[string]interface{}, name string) (int, bool, error) {
	if args == nil {
		return 0, false, nil
	}
	raw, ok := args[name]
	if !ok || raw == nil {
		return 0, false, nil
	}
	switch v := raw.(type) {
	case int:
		if v < 0 {
			return 0, false, fmt.Errorf("%s must be non-negative", name)
		}
		if v > MaxConnectionLimit {
			return MaxConnectionLimit, true, nil
		}
		return v, true, nil
	case float64:
		iv := int(v)
		if iv < 0 {
			return 0, false, fmt.Errorf("%s must be non-negative", name)
		}
		if iv > MaxConnectionLimit {
			return MaxConnectionLimit, true, nil
		}
		return iv, true, nil
	default:
		return 0, false, fmt.Errorf("%s must be an integer", name)
	}
}

func parseOptionalStringArg(args map[string]interface{}, name string) (string, bool, error) {
	if args == nil {
		return "", false, nil
	}
	raw, ok := args[name]
	if !ok || raw == nil {
		return "", false, nil
	}
	value, ok := raw.(string)
	if !ok {
		return "", false, fmt.Errorf("%s must be a string", name)
	}
	return value, true, nil
}

func reverseDirection(direction string) string {
	if strings.EqualFold(direction, "DESC") {
		return "ASC"
	}
	return "DESC"
}

func reverseOrderBy(orderBy *OrderBy) *OrderBy {
	if orderBy == nil {
		return nil
	}
	columns := make([]string, len(orderBy.Columns))
	copy(columns, orderBy.Columns)
	return &OrderBy{
		Columns:   columns,
		Direction: reverseDirection(orderBy.Direction),
	}
}

// parseConnectionOrderBy resolves the orderBy for a connection query.
// If no orderBy is provided, defaults to PK ASC.
func parseConnectionOrderBy(table introspection.Table, args map[string]interface{}, pkCols []introspection.Column) (*OrderBy, error) {
	orderBy, err := ParseOrderBy(table, args)
	if err != nil {
		return nil, err
	}

	if orderBy != nil {
		return orderBy, nil
	}

	// Default to PK ASC (exempt from index validation since PKs are always indexed)
	pkColNames := make([]string, len(pkCols))
	for i, col := range pkCols {
		pkColNames[i] = col.Name
	}
	return &OrderBy{
		Columns:   pkColNames,
		Direction: "ASC",
	}, nil
}

// CursorColumns returns the columns that make up the cursor value.
// This is the orderBy columns (which already include PK tie-breaker from ParseOrderBy).
func CursorColumns(table introspection.Table, orderBy *OrderBy) []introspection.Column {
	cols := make([]introspection.Column, 0, len(orderBy.Columns))
	for _, colName := range orderBy.Columns {
		for _, tc := range table.Columns {
			if tc.Name == colName {
				cols = append(cols, tc)
				break
			}
		}
	}
	return cols
}

func buildConnectionSQL(table introspection.Table, columns []introspection.Column, where *WhereClause, seek sq.Sqlizer, orderBy *OrderBy, limit int) (SQLQuery, error) {
	builder := sq.Select(columnNames(table, columns)...).
		From(sqlutil.QuoteIdentifier(table.Name))

	if where != nil && where.Condition != nil {
		builder = builder.Where(where.Condition)
	}
	if seek != nil {
		builder = builder.Where(seek)
	}

	builder = builder.OrderBy(orderByClauses(orderBy)...).
		Limit(uint64(limit)).
		PlaceholderFormat(sq.Question)

	query, args, err := builder.ToSql()
	if err != nil {
		return SQLQuery{}, err
	}
	return SQLQuery{SQL: query, Args: args}, nil
}

func buildCountSQL(table introspection.Table, where *WhereClause) (SQLQuery, error) {
	base, err := buildRootAggregateBaseSQL(table, where)
	if err != nil {
		return SQLQuery{}, err
	}
	return buildCountFromBaseSQL(base), nil
}

func buildRootAggregateBaseSQL(table introspection.Table, where *WhereClause) (SQLQuery, error) {
	builder := sq.Select("*").
		From(sqlutil.QuoteIdentifier(table.Name))

	if where != nil && where.Condition != nil {
		builder = builder.Where(where.Condition)
	}

	builder = builder.PlaceholderFormat(sq.Question)

	query, args, err := builder.ToSql()
	if err != nil {
		return SQLQuery{}, err
	}
	return SQLQuery{SQL: query, Args: args}, nil
}

func buildCountFromBaseSQL(base SQLQuery) SQLQuery {
	return SQLQuery{
		SQL:  fmt.Sprintf("SELECT COUNT(*) FROM (%s) AS __count", base.SQL),
		Args: append([]interface{}(nil), base.Args...),
	}
}

func BuildConnectionAggregateSQL(base SQLQuery, selection AggregateSelection) SQLQuery {
	return PlanAggregateFromBaseSQL(base, selection)
}
