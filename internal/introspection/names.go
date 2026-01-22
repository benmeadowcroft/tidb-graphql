package introspection

import "tidb-graphql/internal/naming"

// defaultNamer is the package-level namer used for default GraphQL names.
// This uses default configuration without collision detection.
var defaultNamer = naming.Default()

// ToGraphQLTypeName converts a table name to a GraphQL type name (PascalCase).
func ToGraphQLTypeName(name string) string {
	return defaultNamer.ToGraphQLTypeName(name)
}

// ToGraphQLFieldName converts a column/table name to a GraphQL field name (camelCase).
func ToGraphQLFieldName(name string) string {
	return defaultNamer.ToGraphQLFieldName(name)
}

// GraphQLTypeName returns the resolved GraphQL type name for a table.
func GraphQLTypeName(table Table) string {
	if table.GraphQLTypeName != "" {
		return table.GraphQLTypeName
	}
	return ToGraphQLTypeName(table.Name)
}

// GraphQLQueryName returns the resolved GraphQL root field name for a table.
func GraphQLQueryName(table Table) string {
	if table.GraphQLQueryName != "" {
		return table.GraphQLQueryName
	}
	return ToGraphQLFieldName(table.Name)
}

// GraphQLFieldName returns the resolved GraphQL field name for a column.
func GraphQLFieldName(col Column) string {
	if col.GraphQLFieldName != "" {
		return col.GraphQLFieldName
	}
	return ToGraphQLFieldName(col.Name)
}
