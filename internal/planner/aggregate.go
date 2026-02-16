package planner

import (
	"fmt"
	"strings"

	sq "github.com/Masterminds/squirrel"
	"github.com/graphql-go/graphql/language/ast"

	"tidb-graphql/internal/introspection"
	"tidb-graphql/internal/sqlutil"
)

// AggregateSelection represents which aggregate functions to compute.
type AggregateSelection struct {
	Count                bool
	CountDistinctColumns []string // Column names for COUNT(DISTINCT)
	AvgColumns           []string // Column names for AVG
	SumColumns           []string // Column names for SUM
	MinColumns           []string // Column names for MIN
	MaxColumns           []string // Column names for MAX
}

// AggregateFilters defines the list-style filters applicable to aggregate queries.
// These filters are applied to the base dataset BEFORE aggregation occurs,
// allowing queries like "sum of the 50 most recent orders" rather than
// "sum of all orders, limited to 50 results" (which would be meaningless).
type AggregateFilters struct {
	Where   *WhereClause
	OrderBy *OrderBy
	Limit   *int
	Offset  *int
}

// AggregateValueType indicates how to scan an aggregate result value.
type AggregateValueType int

const (
	// AggregateInt is for COUNT, COUNT DISTINCT - always returns integer.
	AggregateInt AggregateValueType = iota
	// AggregateFloat is for AVG, SUM - returns float (nullable).
	AggregateFloat
	// AggregateAny is for MIN, MAX - can return any comparable type.
	AggregateAny
)

// AggregateColumn represents a single aggregate computation with all metadata
// needed for both SQL generation and result scanning. This struct is the single
// source of truth for aggregate column ordering, ensuring SQL SELECT clauses
// and scan destinations stay in sync.
type AggregateColumn struct {
	SQLClause  string             // SQL fragment, e.g., "AVG(`salary`) AS `__avg_salary`"
	ResultKey  string             // Top-level result key: "count", "countDistinct", "avg", "sum", "min", "max"
	ColumnName string             // Original column name (empty for plain count)
	ValueType  AggregateValueType // Determines how to scan the value
}

// PlanAggregateFromBaseSQL builds aggregate SQL over a pre-scoped base query.
// The base query should define the dataset to aggregate over (filters/joins/etc).
func PlanAggregateFromBaseSQL(base SQLQuery, selection AggregateSelection) SQLQuery {
	selectClauses := buildAggregateSelectClauses(selection)
	query := fmt.Sprintf(
		"SELECT %s FROM (%s) AS __agg",
		strings.Join(selectClauses, ", "),
		base.SQL,
	)
	return SQLQuery{SQL: query, Args: base.Args}
}

// PlanAggregate builds SQL for aggregate queries on a table.
func PlanAggregate(
	table introspection.Table,
	selection AggregateSelection,
	filters *AggregateFilters,
) (SQLQuery, error) {
	base := sq.Select("*").From(sqlutil.QuoteIdentifier(table.Name))
	if filters != nil && filters.Where != nil && filters.Where.Condition != nil {
		base = base.Where(filters.Where.Condition)
	}
	if filters != nil && filters.OrderBy != nil {
		base = base.OrderBy(orderByClauses(filters.OrderBy)...)
	}
	if filters != nil && filters.Limit != nil {
		base = base.Limit(uint64(*filters.Limit))
	}
	if filters != nil && filters.Offset != nil {
		base = base.Offset(uint64(*filters.Offset))
	}

	baseSQL, args, err := base.PlaceholderFormat(sq.Question).ToSql()
	if err != nil {
		return SQLQuery{}, err
	}

	// Wrap in subquery to apply filters before aggregation.
	// This ensures ORDER BY + LIMIT filters the dataset, not the aggregate result.
	// Example: "sum of the 50 most recent orders" vs "sum of all orders" (wrong).
	return PlanAggregateFromBaseSQL(SQLQuery{SQL: baseSQL, Args: args}, selection), nil
}

// PlanRelationshipAggregate builds SQL for aggregating related rows via a foreign key.
func PlanRelationshipAggregate(
	relatedTable introspection.Table,
	selection AggregateSelection,
	remoteColumn string,
	fkValue interface{},
	filters *AggregateFilters,
) (SQLQuery, error) {
	// Build WHERE combining relationship filter and user filter
	fkCondition := sq.Eq{sqlutil.QuoteIdentifier(remoteColumn): fkValue}
	var finalCondition sq.Sqlizer = fkCondition

	if filters != nil && filters.Where != nil && filters.Where.Condition != nil {
		finalCondition = sq.And{fkCondition, filters.Where.Condition}
	}

	base := sq.Select("*").
		From(sqlutil.QuoteIdentifier(relatedTable.Name)).
		Where(finalCondition)

	if filters != nil && filters.OrderBy != nil {
		base = base.OrderBy(orderByClauses(filters.OrderBy)...)
	}
	if filters != nil && filters.Limit != nil {
		base = base.Limit(uint64(*filters.Limit))
	}
	if filters != nil && filters.Offset != nil {
		base = base.Offset(uint64(*filters.Offset))
	}

	baseSQL, args, err := base.PlaceholderFormat(sq.Question).ToSql()
	if err != nil {
		return SQLQuery{}, err
	}

	return PlanAggregateFromBaseSQL(SQLQuery{SQL: baseSQL, Args: args}, selection), nil
}

// PlanRelationshipAggregateBatch builds SQL for batched relationship aggregates with GROUP BY.
func PlanRelationshipAggregateBatch(
	relatedTable introspection.Table,
	selection AggregateSelection,
	remoteColumn string,
	values []interface{},
	whereClause *WhereClause,
) (SQLQuery, error) {
	if len(values) == 0 {
		return SQLQuery{}, nil
	}

	selectClauses := buildAggregateSelectClauses(selection)

	// Add grouping column to select
	groupCol := sqlutil.QuoteIdentifier(remoteColumn)
	selectClauses = append([]string{groupCol + " AS __group_key"}, selectClauses...)

	// Build WHERE combining IN clause and user filter
	inCondition := sq.Eq{groupCol: values}
	var finalCondition sq.Sqlizer = inCondition

	if whereClause != nil && whereClause.Condition != nil {
		finalCondition = sq.And{inCondition, whereClause.Condition}
	}

	builder := sq.Select(selectClauses...).
		From(sqlutil.QuoteIdentifier(relatedTable.Name)).
		Where(finalCondition).
		GroupBy(groupCol)

	query, args, err := builder.PlaceholderFormat(sq.Question).ToSql()
	if err != nil {
		return SQLQuery{}, err
	}

	return SQLQuery{SQL: query, Args: args}, nil
}

// BuildAggregateColumns returns the ordered list of aggregate columns for a selection.
// This is the SINGLE SOURCE OF TRUTH for the order of aggregate operations.
// Both SQL generation (via SQLClauses) and result scanning must use this order.
// NOTE: COUNT(*) is ALWAYS included first to match PlanAggregate SQL generation.
func BuildAggregateColumns(selection AggregateSelection) []AggregateColumn {
	var columns []AggregateColumn

	// Always include count - PlanAggregate always adds COUNT(*) to SQL
	columns = append(columns, AggregateColumn{
		SQLClause:  "COUNT(*) AS __count",
		ResultKey:  "count",
		ColumnName: "",
		ValueType:  AggregateInt,
	})

	for _, col := range selection.CountDistinctColumns {
		quotedCol := sqlutil.QuoteIdentifier(col)
		alias := fmt.Sprintf("__count_distinct_%s", col)
		columns = append(columns, AggregateColumn{
			SQLClause:  fmt.Sprintf("COUNT(DISTINCT %s) AS %s", quotedCol, sqlutil.QuoteIdentifier(alias)),
			ResultKey:  "countDistinct",
			ColumnName: col,
			ValueType:  AggregateInt,
		})
	}

	for _, col := range selection.AvgColumns {
		quotedCol := sqlutil.QuoteIdentifier(col)
		alias := fmt.Sprintf("__avg_%s", col)
		columns = append(columns, AggregateColumn{
			SQLClause:  fmt.Sprintf("AVG(%s) AS %s", quotedCol, sqlutil.QuoteIdentifier(alias)),
			ResultKey:  "avg",
			ColumnName: col,
			ValueType:  AggregateFloat,
		})
	}

	for _, col := range selection.SumColumns {
		quotedCol := sqlutil.QuoteIdentifier(col)
		alias := fmt.Sprintf("__sum_%s", col)
		columns = append(columns, AggregateColumn{
			SQLClause:  fmt.Sprintf("SUM(%s) AS %s", quotedCol, sqlutil.QuoteIdentifier(alias)),
			ResultKey:  "sum",
			ColumnName: col,
			ValueType:  AggregateFloat,
		})
	}

	for _, col := range selection.MinColumns {
		quotedCol := sqlutil.QuoteIdentifier(col)
		alias := fmt.Sprintf("__min_%s", col)
		columns = append(columns, AggregateColumn{
			SQLClause:  fmt.Sprintf("MIN(%s) AS %s", quotedCol, sqlutil.QuoteIdentifier(alias)),
			ResultKey:  "min",
			ColumnName: col,
			ValueType:  AggregateAny,
		})
	}

	for _, col := range selection.MaxColumns {
		quotedCol := sqlutil.QuoteIdentifier(col)
		alias := fmt.Sprintf("__max_%s", col)
		columns = append(columns, AggregateColumn{
			SQLClause:  fmt.Sprintf("MAX(%s) AS %s", quotedCol, sqlutil.QuoteIdentifier(alias)),
			ResultKey:  "max",
			ColumnName: col,
			ValueType:  AggregateAny,
		})
	}

	return columns
}

// SQLClauses extracts just the SQL clause strings from a list of AggregateColumns.
func SQLClauses(columns []AggregateColumn) []string {
	clauses := make([]string, len(columns))
	for i, col := range columns {
		clauses[i] = col.SQLClause
	}
	return clauses
}

// buildAggregateSelectClauses builds the SELECT clauses for aggregate functions.
// Deprecated: Use BuildAggregateColumns and SQLClauses instead for type-safe ordering.
func buildAggregateSelectClauses(selection AggregateSelection) []string {
	return SQLClauses(BuildAggregateColumns(selection))
}

// ParseAggregateSelection extracts aggregate column selections from GraphQL AST.
func ParseAggregateSelection(table introspection.Table, field *ast.Field, fragments map[string]ast.Definition) AggregateSelection {
	selection := AggregateSelection{}

	if field == nil || field.SelectionSet == nil {
		return selection
	}

	var visitSelections func(selections []ast.Selection)
	visitSelections = func(selections []ast.Selection) {
		for _, aggSel := range selections {
			switch f := aggSel.(type) {
			case *ast.Field:
				if f.Name == nil {
					continue
				}
				switch f.Name.Value {
				case "count":
					selection.Count = true
				case "countDistinct":
					selection.CountDistinctColumns = mergeColumns(selection.CountDistinctColumns, extractColumnNames(f, table, false))
				case "avg":
					selection.AvgColumns = mergeColumns(selection.AvgColumns, extractColumnNames(f, table, true))
				case "sum":
					selection.SumColumns = mergeColumns(selection.SumColumns, extractColumnNames(f, table, true))
				case "min":
					selection.MinColumns = mergeColumns(selection.MinColumns, extractColumnNames(f, table, false))
				case "max":
					selection.MaxColumns = mergeColumns(selection.MaxColumns, extractColumnNames(f, table, false))
				}
			case *ast.InlineFragment:
				if f.SelectionSet != nil {
					visitSelections(f.SelectionSet.Selections)
				}
			case *ast.FragmentSpread:
				if fragments == nil || f.Name == nil {
					continue
				}
				def, ok := fragments[f.Name.Value]
				if !ok {
					continue
				}
				fragment, ok := def.(*ast.FragmentDefinition)
				if !ok || fragment.SelectionSet == nil {
					continue
				}
				visitSelections(fragment.SelectionSet.Selections)
			}
		}
	}

	visitSelections(field.SelectionSet.Selections)

	return selection
}

// extractColumnNames extracts database column names from a GraphQL field selection.
func extractColumnNames(field *ast.Field, table introspection.Table, numericOnly bool) []string {
	if field == nil || field.SelectionSet == nil {
		return nil
	}

	var columns []string
	for _, sel := range field.SelectionSet.Selections {
		f, ok := sel.(*ast.Field)
		if !ok || f.Name == nil {
			continue
		}

		fieldName := f.Name.Value
		// Find the actual column name from GraphQL field name
		for _, col := range table.Columns {
			if introspection.GraphQLFieldName(col) == fieldName {
				if numericOnly && !introspection.EffectiveGraphQLType(col).IsNumeric() {
					continue
				}
				columns = append(columns, col.Name)
				break
			}
		}
	}

	return columns
}

// mergeColumns deduplicates columns when the same aggregate field appears
// multiple times in a query (e.g., via fragment spreads). Preserves order.
func mergeColumns(existing, added []string) []string {
	if len(added) == 0 {
		return existing
	}
	if len(existing) == 0 {
		return append([]string{}, added...)
	}

	seen := make(map[string]struct{}, len(existing))
	result := append([]string{}, existing...)
	for _, col := range existing {
		seen[col] = struct{}{}
	}
	for _, col := range added {
		if _, ok := seen[col]; ok {
			continue
		}
		seen[col] = struct{}{}
		result = append(result, col)
	}
	return result
}
