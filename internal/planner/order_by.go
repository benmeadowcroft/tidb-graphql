package planner

import (
	"fmt"
	"strings"

	"tidb-graphql/internal/introspection"
)

// OrderBy describes an ordered prefix and its direction.
type OrderBy struct {
	Columns   []string
	Direction string
}

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

// ParseOrderBy validates and parses the orderBy argument for a table.
func ParseOrderBy(table introspection.Table, args map[string]interface{}) (*OrderBy, error) {
	if args == nil {
		return nil, nil
	}

	raw, ok := args["orderBy"]
	if !ok || raw == nil {
		return nil, nil
	}

	orderArgs, ok := raw.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("orderBy must be an input object")
	}
	if len(orderArgs) == 0 {
		return nil, nil
	}
	if len(orderArgs) != 1 {
		return nil, fmt.Errorf("orderBy must contain a single field")
	}

	var fieldName string
	var dirValue interface{}
	for key, value := range orderArgs {
		fieldName = key
		dirValue = value
	}

	direction, ok := dirValue.(string)
	if !ok {
		return nil, fmt.Errorf("orderBy direction must be ASC or DESC")
	}
	direction = strings.ToUpper(direction)
	if direction != "ASC" && direction != "DESC" {
		return nil, fmt.Errorf("orderBy direction must be ASC or DESC")
	}

	options := OrderByOptions(table)
	columns, ok := options[fieldName]
	if !ok {
		return nil, fmt.Errorf("orderBy field %s is not indexed", fieldName)
	}

	columns = append([]string{}, columns...)
	if pk := introspection.PrimaryKeyColumn(table); pk != nil && !containsColumn(columns, pk.Name) {
		columns = append(columns, pk.Name)
	}

	return &OrderBy{
		Columns:   columns,
		Direction: direction,
	}, nil
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
