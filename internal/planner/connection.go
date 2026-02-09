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
	Table         introspection.Table
	Columns       []introspection.Column // selected + orderBy + PK columns
	OrderBy       *OrderBy
	OrderByKey    string                 // GraphQL orderBy field name
	CursorColumns []introspection.Column // columns encoded in cursor
	First         int
	HasCursor     bool // whether an after cursor was provided
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

	first, err := parseFirst(args)
	if err != nil {
		return nil, err
	}

	orderBy, err := parseConnectionOrderBy(table, args, pkCols)
	if err != nil {
		return nil, err
	}

	orderByKey := OrderByKey(table, orderBy.Columns)
	typeName := introspection.GraphQLTypeName(table)

	// Identify cursor columns (the columns encoded in the cursor).
	cursorCols := cursorColumns(table, orderBy)

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
		seekCondition = BuildSeekCondition(colNames, parsedValues, orderBy.Direction)
		hasCursor = true
	}

	// Parse WHERE clause
	var whereClause *WhereClause
	if whereArg, ok := args["where"]; ok {
		if whereMap, ok := whereArg.(map[string]interface{}); ok {
			whereClause, err = BuildWhereClause(table, whereMap)
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

	// Build root SQL: SELECT columns WHERE (filter AND seek) ORDER BY LIMIT first+1
	rootSQL, err := buildConnectionSQL(table, selected, whereClause, seekCondition, orderBy, first+1)
	if err != nil {
		return nil, err
	}

	// Build count SQL: SELECT COUNT(*) WHERE (filter only)
	countSQL, err := buildCountSQL(table, whereClause)
	if err != nil {
		return nil, err
	}

	return &ConnectionPlan{
		Root:          rootSQL,
		Count:         countSQL,
		Table:         table,
		Columns:       selected,
		OrderBy:       orderBy,
		OrderByKey:    orderByKey,
		CursorColumns: cursorCols,
		First:         first,
		HasCursor:     hasCursor,
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

	first, err := parseFirst(args)
	if err != nil {
		return nil, err
	}

	orderBy, err := parseConnectionOrderBy(table, args, pkCols)
	if err != nil {
		return nil, err
	}

	orderByKey := OrderByKey(table, orderBy.Columns)
	typeName := introspection.GraphQLTypeName(table)

	cursorCols := cursorColumns(table, orderBy)

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
		seekCondition = BuildSeekCondition(colNames, parsedValues, orderBy.Direction)
		hasCursor = true
	}

	// FK filter: WHERE remoteColumn = fkValue
	fkCondition := sq.Eq{sqlutil.QuoteIdentifier(remoteColumn): fkValue}

	// Resolve selected columns
	selected := SelectedColumnsForConnection(table, field, options.fragments, orderBy)

	// Build root SQL with FK + seek
	builder := sq.Select(columnNames(table, selected)...).
		From(sqlutil.QuoteIdentifier(table.Name)).
		Where(fkCondition)

	if seekCondition != nil {
		builder = builder.Where(seekCondition)
	}

	builder = builder.OrderBy(orderByClauses(orderBy)...).
		Limit(uint64(first + 1)).
		PlaceholderFormat(sq.Question)

	query, sqlArgs, err := builder.ToSql()
	if err != nil {
		return nil, err
	}

	// Build count SQL with FK only
	countBuilder := sq.Select("COUNT(*)").
		From(sqlutil.QuoteIdentifier(table.Name)).
		Where(fkCondition).
		PlaceholderFormat(sq.Question)

	countQuery, countArgs, err := countBuilder.ToSql()
	if err != nil {
		return nil, err
	}

	return &ConnectionPlan{
		Root:          SQLQuery{SQL: query, Args: sqlArgs},
		Count:         SQLQuery{SQL: countQuery, Args: countArgs},
		Table:         table,
		Columns:       selected,
		OrderBy:       orderBy,
		OrderByKey:    orderByKey,
		CursorColumns: cursorCols,
		First:         first,
		HasCursor:     hasCursor,
	}, nil
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

func parseFirst(args map[string]interface{}) (int, error) {
	if args == nil {
		return DefaultConnectionLimit, nil
	}
	raw, ok := args["first"]
	if !ok || raw == nil {
		return DefaultConnectionLimit, nil
	}
	switch v := raw.(type) {
	case int:
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

// cursorColumns returns the columns that make up the cursor value.
// This is the orderBy columns (which already include PK tie-breaker from ParseOrderBy).
func cursorColumns(table introspection.Table, orderBy *OrderBy) []introspection.Column {
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
	builder := sq.Select("COUNT(*)").
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
