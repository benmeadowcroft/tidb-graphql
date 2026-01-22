// Package schemanaming applies naming rules to introspected schema elements.
package schemanaming

import (
	"fmt"

	"tidb-graphql/internal/introspection"
	"tidb-graphql/internal/naming"
)

// Apply assigns GraphQL type/query/field names to the schema using the provided namer.
// It resets collision state to ensure deterministic naming per schema build.
func Apply(schema *introspection.Schema, namer *naming.Namer) {
	if schema == nil {
		return
	}
	if namer == nil {
		namer = naming.Default()
	}
	namer.Reset()

	for ti := range schema.Tables {
		table := &schema.Tables[ti]

		typeName := namer.RegisterType(table.Name)
		table.GraphQLTypeName = typeName
		table.GraphQLQueryName = namer.RegisterQueryField(table.Name)

		for ci := range table.Columns {
			col := &table.Columns[ci]
			col.GraphQLFieldName = namer.RegisterColumnField(typeName, col.Name)
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
}
