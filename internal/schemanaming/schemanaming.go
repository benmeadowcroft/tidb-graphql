// Package schemanaming applies naming rules to introspected schema elements.
package schemanaming

import (
	"fmt"
	"log/slog"
	"strings"

	"tidb-graphql/internal/introspection"
	"tidb-graphql/internal/naming"
)

// Apply assigns GraphQL type/query/field names to the schema using the provided namer.
// It resets collision state to ensure deterministic naming per schema build.
func Apply(schema *introspection.Schema, namer *naming.Namer) {
	if schema == nil {
		return
	}
	if schema.NamesApplied {
		return
	}
	if namer == nil {
		namer = naming.Default()
	}
	namer.Reset()
	singularNamer := naming.New(namer.Config(), nil)
	singularNamer.Reset()

	for ti := range schema.Tables {
		table := &schema.Tables[ti]

		typeName := namer.RegisterType(table.Name)
		table.GraphQLTypeName = typeName
		pluralTableName := namer.Pluralize(table.Name)
		table.GraphQLQueryName = namer.RegisterQueryField(pluralTableName)
		singularTableName := singularNamer.Singularize(table.Name)
		table.GraphQLSingleQueryName = singularNamer.RegisterQueryField(singularTableName)
		table.GraphQLSingleTypeName = singularNamer.RegisterType(singularTableName)

		for ci := range table.Columns {
			col := &table.Columns[ci]
			col.GraphQLFieldName = namer.RegisterColumnField(typeName, col.Name)
		}

		for ci := range table.Columns {
			col := &table.Columns[ci]
			if col.IsPrimaryKey && strings.EqualFold(col.Name, "id") {
				desiredName := "databaseId"
				if hasColumnFieldName(table.Columns, desiredName, ci) {
					slog.Default().Warn("GraphQL name collision for databaseId; using databaseId_raw",
						"table", table.Name,
						"column", col.Name,
					)
					desiredName = "databaseId_raw"
				}
				col.GraphQLFieldName = desiredName
			}
		}

		for ri := range table.Relationships {
			rel := &table.Relationships[ri]
			baseName := rel.GraphQLFieldName
			if baseName == "" {
				baseName = namer.ToGraphQLFieldName(rel.RemoteTable)
			}
			source := fmt.Sprintf("%s:%s:%s", rel.RemoteTable, rel.LocalColumn, rel.RemoteColumn)
			// For collision suffix: ManyToOne uses "Ref", all others (OneToMany, ManyToMany, EdgeList) use "Rel"
			useRefSuffix := rel.IsManyToOne
			rel.GraphQLFieldName = namer.RegisterRelationshipField(typeName, baseName, source, useRefSuffix)
		}
	}

	schema.NamesApplied = true
}

func hasColumnFieldName(columns []introspection.Column, name string, skipIndex int) bool {
	for i := range columns {
		if i == skipIndex {
			continue
		}
		if columns[i].GraphQLFieldName == name {
			return true
		}
	}
	return false
}
