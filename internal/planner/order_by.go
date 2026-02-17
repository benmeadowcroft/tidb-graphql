package planner

import (
	"fmt"
	"sort"
	"strings"

	"tidb-graphql/internal/introspection"
)

// OrderBy describes an ordered prefix and per-column directions.
type OrderBy struct {
	Columns    []string
	Directions []string
}

// OrderByPolicy controls how explicit orderBy clauses are validated.
type OrderByPolicy string

const (
	OrderByPolicyIndexPrefixOnly OrderByPolicy = "INDEX_PREFIX_ONLY"
	OrderByPolicyAllowNonPrefix  OrderByPolicy = "ALLOW_NON_PREFIX"
)

// OrderByOptions returns allowed leftmost index prefixes mapped to field names.
func OrderByOptions(table introspection.Table) map[string][]string {
	options := make(map[string][]string)
	columnNames := make(map[string]string, len(table.Columns))
	for _, col := range table.Columns {
		columnNames[col.Name] = introspection.GraphQLFieldName(col)
	}

	for _, index := range table.Indexes {
		if len(index.Columns) == 0 {
			continue
		}
		for i := 1; i <= len(index.Columns); i++ {
			prefix := index.Columns[:i]
			fieldName := orderByFieldName(prefix, columnNames)
			if _, ok := options[fieldName]; ok {
				continue
			}
			options[fieldName] = prefix
		}
	}

	return options
}

// OrderByIndexedFields returns indexed GraphQL field names mapped to SQL column names.
// It includes any column that participates in at least one index.
func OrderByIndexedFields(table introspection.Table) map[string]string {
	fields := make(map[string]string)
	columnNames := make(map[string]string, len(table.Columns))
	for _, col := range table.Columns {
		columnNames[col.Name] = introspection.GraphQLFieldName(col)
	}
	for _, index := range table.Indexes {
		for _, colName := range index.Columns {
			fieldName, ok := columnNames[colName]
			if !ok || fieldName == "" {
				fieldName = introspection.ToGraphQLFieldName(colName)
			}
			fields[fieldName] = colName
		}
	}
	return fields
}

// ParseOrderBy validates and parses the orderBy argument for a table.
func ParseOrderBy(table introspection.Table, args map[string]interface{}) (*OrderBy, error) {
	if args == nil {
		return nil, nil
	}

	raw, ok := args["orderBy"]
	if !ok || raw == nil {
		return nil, nil
	}

	clauseArgs, ok := raw.([]interface{})
	if !ok {
		return nil, fmt.Errorf("orderBy must be a list of clauses")
	}
	if len(clauseArgs) == 0 {
		return nil, fmt.Errorf("orderBy must contain at least one clause")
	}
	policy, err := parseOrderByPolicy(args)
	if err != nil {
		return nil, err
	}

	indexedFields := OrderByIndexedFields(table)
	explicitColumns := make([]string, 0, len(clauseArgs))
	explicitDirections := make([]string, 0, len(clauseArgs))
	seenFields := make(map[string]struct{}, len(clauseArgs))

	for i, rawClause := range clauseArgs {
		clauseMap, ok := rawClause.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("orderBy[%d] must be an input object", i)
		}
		if len(clauseMap) != 1 {
			return nil, fmt.Errorf("orderBy[%d] must contain exactly one field", i)
		}

		var (
			fieldName    string
			rawDirection interface{}
		)
		for name, value := range clauseMap {
			fieldName = name
			rawDirection = value
		}
		if fieldName == "" {
			return nil, fmt.Errorf("orderBy[%d] must contain exactly one field", i)
		}
		if _, dup := seenFields[fieldName]; dup {
			return nil, fmt.Errorf("orderBy contains duplicate field %s", fieldName)
		}
		seenFields[fieldName] = struct{}{}

		columnName, ok := indexedFields[fieldName]
		if !ok {
			return nil, fmt.Errorf("orderBy field %s is not indexed", fieldName)
		}

		direction, ok := rawDirection.(string)
		if !ok {
			return nil, fmt.Errorf("orderBy[%d].%s must be ASC or DESC", i, fieldName)
		}
		direction = strings.ToUpper(direction)
		if direction != "ASC" && direction != "DESC" {
			return nil, fmt.Errorf("orderBy[%d].%s must be ASC or DESC", i, fieldName)
		}

		explicitColumns = append(explicitColumns, columnName)
		explicitDirections = append(explicitDirections, direction)
	}

	if policy == OrderByPolicyIndexPrefixOnly && !matchesIndexPrefix(table, explicitColumns) {
		allowed := sortedOrderByPrefixNames(table)
		return nil, fmt.Errorf("orderBy fields must match an indexed left-prefix (allowed: %s)", strings.Join(allowed, ", "))
	}

	columns := append([]string{}, explicitColumns...)
	directions := append([]string{}, explicitDirections...)
	for _, pkCol := range introspection.PrimaryKeyColumns(table) {
		if containsColumn(columns, pkCol.Name) {
			continue
		}
		columns = append(columns, pkCol.Name)
		directions = append(directions, "ASC")
	}

	return &OrderBy{
		Columns:    columns,
		Directions: directions,
	}, nil
}

func parseOrderByPolicy(args map[string]interface{}) (OrderByPolicy, error) {
	if args == nil {
		return OrderByPolicyIndexPrefixOnly, nil
	}
	raw, ok := args["orderByPolicy"]
	if !ok || raw == nil {
		return OrderByPolicyIndexPrefixOnly, nil
	}
	value, ok := raw.(string)
	if !ok {
		return "", fmt.Errorf("orderByPolicy must be INDEX_PREFIX_ONLY or ALLOW_NON_PREFIX")
	}
	value = strings.ToUpper(value)
	switch OrderByPolicy(value) {
	case OrderByPolicyIndexPrefixOnly, OrderByPolicyAllowNonPrefix:
		return OrderByPolicy(value), nil
	default:
		return "", fmt.Errorf("orderByPolicy must be INDEX_PREFIX_ONLY or ALLOW_NON_PREFIX")
	}
}

func sortedOrderByPrefixNames(table introspection.Table) []string {
	options := OrderByOptions(table)
	names := make([]string, 0, len(options))
	for name := range options {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func matchesIndexPrefix(table introspection.Table, explicitColumns []string) bool {
	if len(explicitColumns) == 0 {
		return false
	}
	for _, index := range table.Indexes {
		if len(index.Columns) < len(explicitColumns) {
			continue
		}
		match := true
		for i, col := range explicitColumns {
			if index.Columns[i] != col {
				match = false
				break
			}
		}
		if match {
			return true
		}
	}
	return false
}

func orderByFieldName(columns []string, columnNames map[string]string) string {
	parts := make([]string, len(columns))
	for i, col := range columns {
		if gqlName, ok := columnNames[col]; ok && gqlName != "" {
			parts[i] = gqlName
		} else {
			parts[i] = introspection.ToGraphQLFieldName(col)
		}
	}
	return strings.Join(parts, "_")
}

// OrderByKey returns the GraphQL field name for a set of orderBy columns,
// used as part of the cursor identity.
func OrderByKey(table introspection.Table, columns []string) string {
	columnNames := make(map[string]string, len(table.Columns))
	for _, col := range table.Columns {
		columnNames[col.Name] = introspection.GraphQLFieldName(col)
	}
	return orderByFieldName(columns, columnNames)
}

func containsColumn(columns []string, target string) bool {
	for _, col := range columns {
		if col == target {
			return true
		}
	}
	return false
}
