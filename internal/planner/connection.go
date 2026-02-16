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
	HasCursor     bool // whether an after cursor was provided
}

// connectionArgs holds the parsed common arguments for connection queries.
type connectionArgs struct {
	first         int
	orderBy       *OrderBy
	orderByKey    string
	typeName      string
	cursorCols    []introspection.Column
	seekCondition sq.Sqlizer
	hasCursor     bool
	whereClause   *WhereClause
	selected      []introspection.Column
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

	first, err := ParseFirstWithDefault(args, defaultLimit)
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

	// Parse after cursor
	var seekCondition sq.Sqlizer
	hasCursor := false
	if afterRaw, ok := args["after"]; ok && afterRaw != nil {
		afterStr, ok := afterRaw.(string)
		if !ok {
			return nil, fmt.Errorf("after must be a string")
		}
		cType, cKey, cDir, cVals, err := cursor.DecodeCursor(afterStr)
		if err != nil {
			return nil, fmt.Errorf("invalid after cursor: %w", err)
		}
		if err := cursor.ValidateCursor(typeName, orderByKey, orderBy.Direction, cType, cKey, cDir); err != nil {
			return nil, fmt.Errorf("invalid after cursor: %w", err)
		}
		parsedValues, err := cursor.ParseCursorValues(cVals, cursorCols)
		if err != nil {
			return nil, fmt.Errorf("invalid after cursor: %w", err)
		}
		colNames := make([]string, len(cursorCols))
		for i, c := range cursorCols {
			colNames[i] = c.Name
		}
		seekCondition = buildSeek(colNames, parsedValues, orderBy.Direction)
		hasCursor = true
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
		first:         first,
		orderBy:       orderBy,
		orderByKey:    orderByKey,
		typeName:      typeName,
		cursorCols:    cursorCols,
		seekCondition: seekCondition,
		hasCursor:     hasCursor,
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

	// Build root SQL: SELECT columns WHERE (filter AND seek) ORDER BY LIMIT first+1
	rootSQL, err := buildConnectionSQL(table, ca.selected, ca.whereClause, ca.seekCondition, ca.orderBy, ca.first+1)
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
		First:         ca.first,
		HasCursor:     ca.hasCursor,
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

	builder = builder.OrderBy(orderByClauses(ca.orderBy)...).
		Limit(uint64(ca.first + 1)).
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
		First:         ca.first,
		HasCursor:     ca.hasCursor,
	}, nil
}

// PlanManyToManyConnection plans a connection for a many-to-many relationship.
func PlanManyToManyConnection(
	targetTable introspection.Table,
	junctionTable string,
	junctionLocalFK string,
	junctionRemoteFK string,
	targetPK string,
	fkValue interface{},
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
	quotedLocalFK := sqlutil.QuoteIdentifier(junctionLocalFK)
	quotedRemoteFK := sqlutil.QuoteIdentifier(junctionRemoteFK)
	quotedTargetPK := sqlutil.QuoteIdentifier(targetPK)

	builder := sq.Select(columnNamesQualified(targetTable.Name, ca.selected)...).
		From(quotedTarget).
		Join(fmt.Sprintf("%s ON %s.%s = %s.%s", quotedJunction, quotedJunction, quotedRemoteFK, quotedTarget, quotedTargetPK)).
		Where(sq.Eq{fmt.Sprintf("%s.%s", quotedJunction, quotedLocalFK): fkValue})

	if ca.whereClause != nil && ca.whereClause.Condition != nil {
		builder = builder.Where(ca.whereClause.Condition)
	}
	if ca.seekCondition != nil {
		builder = builder.Where(ca.seekCondition)
	}

	builder = builder.OrderBy(orderByClausesQualified(targetTable.Name, ca.orderBy)...).
		Limit(uint64(ca.first + 1)).
		PlaceholderFormat(sq.Question)

	query, sqlArgs, err := builder.ToSql()
	if err != nil {
		return nil, err
	}

	countQuery, err := BuildManyToManyCountSQL(targetTable, junctionTable, junctionLocalFK, junctionRemoteFK, targetPK, fkValue, ca.whereClause)
	if err != nil {
		return nil, err
	}
	aggregateBase, err := BuildManyToManyAggregateBaseSQL(targetTable, junctionTable, junctionLocalFK, junctionRemoteFK, targetPK, fkValue, ca.whereClause)
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
		First:         ca.first,
		HasCursor:     ca.hasCursor,
	}, nil
}

// PlanEdgeListConnection plans a connection for an edge-list relationship.
func PlanEdgeListConnection(
	junctionTable introspection.Table,
	junctionLocalFK string,
	fkValue interface{},
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

	fkCondition := sq.Eq{sqlutil.QuoteIdentifier(junctionLocalFK): fkValue}
	builder := sq.Select(columnNames(junctionTable, ca.selected)...).
		From(sqlutil.QuoteIdentifier(junctionTable.Name)).
		Where(fkCondition)

	if ca.whereClause != nil && ca.whereClause.Condition != nil {
		builder = builder.Where(ca.whereClause.Condition)
	}
	if ca.seekCondition != nil {
		builder = builder.Where(ca.seekCondition)
	}

	builder = builder.OrderBy(orderByClauses(ca.orderBy)...).
		Limit(uint64(ca.first + 1)).
		PlaceholderFormat(sq.Question)

	query, sqlArgs, err := builder.ToSql()
	if err != nil {
		return nil, err
	}

	countQuery, err := BuildEdgeListCountSQL(junctionTable, junctionLocalFK, fkValue, ca.whereClause)
	if err != nil {
		return nil, err
	}
	aggregateBase, err := BuildEdgeListAggregateBaseSQL(junctionTable, junctionLocalFK, fkValue, ca.whereClause)
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
		First:         ca.first,
		HasCursor:     ca.hasCursor,
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
	junctionLocalFK string,
	junctionRemoteFK string,
	targetPK string,
	fkValue interface{},
	whereClause *WhereClause,
) (SQLQuery, error) {
	base, err := BuildManyToManyAggregateBaseSQL(
		targetTable,
		junctionTable,
		junctionLocalFK,
		junctionRemoteFK,
		targetPK,
		fkValue,
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
	junctionLocalFK string,
	junctionRemoteFK string,
	targetPK string,
	fkValue interface{},
	whereClause *WhereClause,
) (SQLQuery, error) {
	quotedTarget := sqlutil.QuoteIdentifier(targetTable.Name)
	quotedJunction := sqlutil.QuoteIdentifier(junctionTable)
	quotedLocalFK := sqlutil.QuoteIdentifier(junctionLocalFK)
	quotedRemoteFK := sqlutil.QuoteIdentifier(junctionRemoteFK)
	quotedTargetPK := sqlutil.QuoteIdentifier(targetPK)

	builder := sq.Select("*").
		From(quotedTarget).
		Join(fmt.Sprintf("%s ON %s.%s = %s.%s", quotedJunction, quotedJunction, quotedRemoteFK, quotedTarget, quotedTargetPK)).
		Where(sq.Eq{fmt.Sprintf("%s.%s", quotedJunction, quotedLocalFK): fkValue})

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
	junctionLocalFK string,
	fkValue interface{},
	whereClause *WhereClause,
) (SQLQuery, error) {
	base, err := BuildEdgeListAggregateBaseSQL(junctionTable, junctionLocalFK, fkValue, whereClause)
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
	junctionLocalFK string,
	fkValue interface{},
	whereClause *WhereClause,
) (SQLQuery, error) {
	builder := sq.Select("*").
		From(sqlutil.QuoteIdentifier(junctionTable.Name)).
		Where(sq.Eq{sqlutil.QuoteIdentifier(junctionLocalFK): fkValue})

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
	junctionLocalFK string,
	junctionRemoteFK string,
	targetPK string,
	columns []introspection.Column,
	parentValues []interface{},
	first int,
	orderBy *OrderBy,
	whereClause *WhereClause,
) (SQLQuery, error) {
	return PlanManyToManyBatch(
		junctionTable,
		targetTable,
		junctionLocalFK,
		junctionRemoteFK,
		targetPK,
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
	junctionLocalFK string,
	columns []introspection.Column,
	parentValues []interface{},
	first int,
	orderBy *OrderBy,
	whereClause *WhereClause,
) (SQLQuery, error) {
	return PlanEdgeListBatch(
		junctionTable,
		junctionLocalFK,
		columns,
		parentValues,
		first+1,
		0,
		orderBy,
		whereClause,
	)
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
	raw, ok := args["first"]
	if !ok || raw == nil {
		return normalizeFirstLimit(fallback), nil
	}
	switch v := raw.(type) {
	case int:
		// first=0 is intentional: callers can request pageInfo/totalCount only.
		if v < 0 {
			return 0, fmt.Errorf("first must be non-negative")
		}
		if v > MaxConnectionLimit {
			return MaxConnectionLimit, nil
		}
		return v, nil
	case float64:
		iv := int(v)
		if iv < 0 {
			return 0, fmt.Errorf("first must be non-negative")
		}
		if iv > MaxConnectionLimit {
			return MaxConnectionLimit, nil
		}
		return iv, nil
	default:
		return 0, fmt.Errorf("first must be an integer")
	}
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
