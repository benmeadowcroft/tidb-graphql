package planner

import (
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"

	"tidb-graphql/internal/cursor"
	"tidb-graphql/internal/introspection"
	"tidb-graphql/internal/sqlutil"

	sq "github.com/Masterminds/squirrel"
	"github.com/graphql-go/graphql/language/ast"
)

const (
	// DefaultVectorFirst is the default page size used by vector search fields
	// when first is omitted.
	DefaultVectorFirst = 20
)

const vectorDistanceAlias = "__vector_distance"

// VectorDistanceMetric controls which TiDB distance function is used.
type VectorDistanceMetric string

const (
	VectorDistanceMetricCosine VectorDistanceMetric = "COSINE"
	VectorDistanceMetricL2     VectorDistanceMetric = "L2"
)

// VectorConnectionPlan is the SQL plan for a vector-search connection field.
type VectorConnectionPlan struct {
	Root             SQLQuery
	Table            introspection.Table
	VectorColumn     introspection.Column
	Columns          []introspection.Column
	PKColumns        []introspection.Column
	DistanceAlias    string
	First            int
	HasAfter         bool
	OrderByKey       string
	CursorDirections []string
}

// PlanVectorSearchConnection builds SQL for a forward-only cursor-paginated
// vector search query.
func PlanVectorSearchConnection(
	schema *introspection.Schema,
	table introspection.Table,
	vectorColumn introspection.Column,
	field *ast.Field,
	args map[string]interface{},
	maxTopK int,
	defaultFirst int,
	opts ...PlanOption,
) (*VectorConnectionPlan, error) {
	if maxTopK <= 0 {
		maxTopK = MaxConnectionLimit
	}
	if defaultFirst <= 0 {
		defaultFirst = DefaultVectorFirst
	}
	if defaultFirst > maxTopK {
		defaultFirst = maxTopK
	}

	options := &planOptions{}
	for _, opt := range opts {
		opt(options)
	}
	if options.schema == nil {
		options.schema = schema
	}

	if options.limits != nil {
		cost := EstimateCost(field, args, defaultFirst, options.fragments)
		if err := validateLimits(cost, *options.limits); err != nil {
			return nil, err
		}
	}

	if !introspection.IsVectorColumn(vectorColumn) {
		return nil, fmt.Errorf("column %s is not a vector column", vectorColumn.Name)
	}

	pkCols := introspection.PrimaryKeyColumns(table)
	if len(pkCols) == 0 {
		return nil, fmt.Errorf("vector search requires primary key on table %s", table.Name)
	}

	window, err := parseVectorConnectionWindow(args, defaultFirst, maxTopK)
	if err != nil {
		return nil, err
	}

	metric, err := parseVectorDistanceMetricArg(args)
	if err != nil {
		return nil, err
	}

	rawVector, ok := args["vector"]
	if !ok || rawVector == nil {
		return nil, fmt.Errorf("vector is required")
	}
	vectorLiteral, vectorLen, err := normalizeVectorQueryValue(rawVector)
	if err != nil {
		return nil, fmt.Errorf("invalid vector: %w", err)
	}
	if vectorColumn.VectorDimension > 0 && vectorLen != vectorColumn.VectorDimension {
		return nil, fmt.Errorf("vector length %d does not match %s dimension %d", vectorLen, vectorColumn.Name, vectorColumn.VectorDimension)
	}

	var whereClause *WhereClause
	if rawWhere, ok := args["where"]; ok && rawWhere != nil {
		whereMap, ok := rawWhere.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("where must be an input object")
		}
		whereClause, err = BuildWhereClauseWithSchema(options.schema, table, whereMap)
		if err != nil {
			return nil, fmt.Errorf("invalid WHERE clause: %w", err)
		}
		if whereClause != nil {
			if err := ValidateWhereClauseIndexes(options.schema, table, whereClause); err != nil {
				return nil, err
			}
		}
	}

	pkOrderBy := &OrderBy{
		Columns:    columnNamesFromColumns(pkCols),
		Directions: ascDirections(len(pkCols)),
	}
	selected := SelectedColumnsForConnection(table, field, options.fragments, pkOrderBy)

	orderByKey := vectorOrderByKey(table, vectorColumn, metric, pkCols)
	cursorDirections := append([]string{"ASC"}, ascDirections(len(pkCols))...)

	var seekCondition sq.Sqlizer
	if window.hasAfter {
		cursorType, cursorKey, dirs, values, err := cursor.DecodeCursor(window.after)
		if err != nil {
			return nil, fmt.Errorf("invalid after cursor: %w", err)
		}
		if err := cursor.ValidateCursor(introspection.GraphQLTypeName(table), orderByKey, cursorDirections, cursorType, cursorKey, dirs); err != nil {
			return nil, fmt.Errorf("invalid after cursor: %w", err)
		}
		distance, pkValues, err := cursor.ParseVectorCursorValues(values, pkCols)
		if err != nil {
			return nil, fmt.Errorf("invalid after cursor: %w", err)
		}
		seekColumns := make([]string, 0, len(pkCols)+1)
		seekColumns = append(seekColumns, vectorDistanceAlias)
		for _, pk := range pkCols {
			seekColumns = append(seekColumns, pk.Name)
		}
		seekValues := make([]interface{}, 0, len(pkValues)+1)
		seekValues = append(seekValues, distance)
		seekValues = append(seekValues, pkValues...)
		seekCondition = BuildSeekCondition(seekColumns, seekValues, cursorDirections)
	}

	query, queryArgs, err := buildVectorConnectionSQL(table, selected, vectorColumn, metric, vectorLiteral, whereClause, seekCondition, window.first)
	if err != nil {
		return nil, err
	}

	return &VectorConnectionPlan{
		Root:             SQLQuery{SQL: query, Args: queryArgs},
		Table:            table,
		VectorColumn:     vectorColumn,
		Columns:          selected,
		PKColumns:        pkCols,
		DistanceAlias:    vectorDistanceAlias,
		First:            window.first,
		HasAfter:         window.hasAfter,
		OrderByKey:       orderByKey,
		CursorDirections: cursorDirections,
	}, nil
}

type vectorWindow struct {
	first    int
	hasAfter bool
	after    string
}

func parseVectorConnectionWindow(args map[string]interface{}, defaultFirst, maxTopK int) (vectorWindow, error) {
	window := vectorWindow{first: defaultFirst}
	if args == nil {
		return window, nil
	}
	if rawBefore, ok := args["before"]; ok && rawBefore != nil {
		return vectorWindow{}, fmt.Errorf("before is not supported for vector search")
	}
	if rawLast, ok := args["last"]; ok && rawLast != nil {
		return vectorWindow{}, fmt.Errorf("last is not supported for vector search")
	}

	first, hasFirst, err := parseVectorFirstArg(args, maxTopK)
	if err != nil {
		return vectorWindow{}, err
	}
	if hasFirst {
		window.first = first
	}

	after, hasAfter, err := parseOptionalStringArg(args, "after")
	if err != nil {
		return vectorWindow{}, err
	}
	window.after = after
	window.hasAfter = hasAfter && strings.TrimSpace(after) != ""

	return window, nil
}

func parseVectorFirstArg(args map[string]interface{}, maxTopK int) (int, bool, error) {
	raw, ok := args["first"]
	if !ok || raw == nil {
		return 0, false, nil
	}
	switch v := raw.(type) {
	case int:
		if v < 0 {
			return 0, false, fmt.Errorf("first must be non-negative")
		}
		if v == 0 {
			return 0, false, fmt.Errorf("first must be greater than 0")
		}
		if v > maxTopK {
			return maxTopK, true, nil
		}
		return v, true, nil
	case float64:
		if math.Trunc(v) != v {
			return 0, false, fmt.Errorf("first must be an integer")
		}
		iv := int(v)
		if iv < 0 {
			return 0, false, fmt.Errorf("first must be non-negative")
		}
		if iv == 0 {
			return 0, false, fmt.Errorf("first must be greater than 0")
		}
		if iv > maxTopK {
			return maxTopK, true, nil
		}
		return iv, true, nil
	default:
		return 0, false, fmt.Errorf("first must be an integer")
	}
}

func parseVectorDistanceMetricArg(args map[string]interface{}) (VectorDistanceMetric, error) {
	if args == nil {
		return VectorDistanceMetricCosine, nil
	}
	raw, ok := args["metric"]
	if !ok || raw == nil {
		return VectorDistanceMetricCosine, nil
	}
	value, ok := raw.(string)
	if !ok {
		return "", fmt.Errorf("metric must be COSINE or L2")
	}
	switch strings.ToUpper(value) {
	case string(VectorDistanceMetricCosine):
		return VectorDistanceMetricCosine, nil
	case string(VectorDistanceMetricL2):
		return VectorDistanceMetricL2, nil
	default:
		return "", fmt.Errorf("metric must be COSINE or L2")
	}
}

func buildVectorConnectionSQL(
	table introspection.Table,
	selected []introspection.Column,
	vectorColumn introspection.Column,
	metric VectorDistanceMetric,
	vectorLiteral string,
	whereClause *WhereClause,
	seekCondition sq.Sqlizer,
	first int,
) (string, []interface{}, error) {
	inner := sq.Select(columnNames(table, selected)...).
		Column(
			fmt.Sprintf("%s(%s, ?) AS %s", vectorMetricFunction(metric), sqlutil.QuoteIdentifier(vectorColumn.Name), sqlutil.QuoteIdentifier(vectorDistanceAlias)),
			vectorLiteral,
		).
		From(sqlutil.QuoteIdentifier(table.Name))
	if whereClause != nil && whereClause.Condition != nil {
		inner = inner.Where(whereClause.Condition)
	}

	alias := "vector_ranked"
	outerColumns := make([]string, 0, len(selected)+1)
	for _, col := range selected {
		outerColumns = append(outerColumns, fmt.Sprintf("%s.%s", sqlutil.QuoteIdentifier(alias), sqlutil.QuoteIdentifier(col.Name)))
	}
	outerColumns = append(outerColumns, fmt.Sprintf("%s.%s AS %s", sqlutil.QuoteIdentifier(alias), sqlutil.QuoteIdentifier(vectorDistanceAlias), sqlutil.QuoteIdentifier(vectorDistanceAlias)))

	outer := sq.Select(outerColumns...).
		FromSelect(inner, alias)

	if seekCondition != nil {
		outer = outer.Where(seekCondition)
	}

	orderClauses := []string{fmt.Sprintf("%s ASC", sqlutil.QuoteIdentifier(vectorDistanceAlias))}
	for _, pk := range introspection.PrimaryKeyColumns(table) {
		orderClauses = append(orderClauses, fmt.Sprintf("%s ASC", sqlutil.QuoteIdentifier(pk.Name)))
	}
	outer = outer.OrderBy(orderClauses...).
		Limit(uint64(first + 1)).
		PlaceholderFormat(sq.Question)

	return outer.ToSql()
}

func normalizeVectorQueryValue(raw interface{}) (string, int, error) {
	vector, err := parseVectorQueryValues(raw)
	if err != nil {
		return "", 0, err
	}
	encoded, err := json.Marshal(vector)
	if err != nil {
		return "", 0, err
	}
	return string(encoded), len(vector), nil
}

func parseVectorQueryValues(raw interface{}) ([]float64, error) {
	switch v := raw.(type) {
	case []float64:
		return validateFiniteVector(v)
	case []float32:
		out := make([]float64, len(v))
		for i, n := range v {
			out[i] = float64(n)
		}
		return validateFiniteVector(out)
	case []interface{}:
		out := make([]float64, len(v))
		for i, item := range v {
			n, err := parseVectorNumber(item)
			if err != nil {
				return nil, err
			}
			out[i] = n
		}
		return validateFiniteVector(out)
	default:
		return nil, fmt.Errorf("vector must be a list of numbers")
	}
}

func validateFiniteVector(values []float64) ([]float64, error) {
	out := make([]float64, len(values))
	copy(out, values)
	for _, n := range out {
		if math.IsNaN(n) || math.IsInf(n, 0) {
			return nil, fmt.Errorf("vector values must be finite numbers")
		}
	}
	return out, nil
}

func parseVectorNumber(raw interface{}) (float64, error) {
	switch v := raw.(type) {
	case float64:
		return v, nil
	case float32:
		return float64(v), nil
	case int:
		return float64(v), nil
	case int8:
		return float64(v), nil
	case int16:
		return float64(v), nil
	case int32:
		return float64(v), nil
	case int64:
		return float64(v), nil
	case uint:
		return float64(v), nil
	case uint8:
		return float64(v), nil
	case uint16:
		return float64(v), nil
	case uint32:
		return float64(v), nil
	case uint64:
		return float64(v), nil
	case string:
		parsed, err := strconv.ParseFloat(v, 64)
		if err != nil {
			return 0, fmt.Errorf("vector values must be numeric")
		}
		return parsed, nil
	default:
		return 0, fmt.Errorf("vector values must be numeric")
	}
}

func vectorMetricFunction(metric VectorDistanceMetric) string {
	switch metric {
	case VectorDistanceMetricL2:
		return "VEC_L2_DISTANCE"
	default:
		return "VEC_COSINE_DISTANCE"
	}
}

func vectorOrderByKey(table introspection.Table, vectorColumn introspection.Column, metric VectorDistanceMetric, pkCols []introspection.Column) string {
	pkParts := make([]string, len(pkCols))
	for i, pk := range pkCols {
		pkParts[i] = introspection.GraphQLFieldName(pk)
	}
	return fmt.Sprintf("vector:%s:%s:%s", introspection.GraphQLTypeName(table), introspection.GraphQLFieldName(vectorColumn), strings.ToLower(string(metric))+"_"+strings.Join(pkParts, "_"))
}

func columnNamesFromColumns(cols []introspection.Column) []string {
	names := make([]string, len(cols))
	for i, col := range cols {
		names[i] = col.Name
	}
	return names
}

func ascDirections(n int) []string {
	directions := make([]string, n)
	for i := range directions {
		directions[i] = "ASC"
	}
	return directions
}
