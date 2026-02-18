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

		overrideTypeName, hasTypeOverride := findTypeOverride(namer.Config().TypeOverrides, table.Name)
		var typeName string
		if hasTypeOverride {
			typeName = namer.RegisterTypeName(overrideTypeName, table.Name)
		} else {
			typeName = namer.RegisterType(table.Name)
		}
		table.GraphQLTypeName = typeName
		pluralTableName := namer.Pluralize(table.Name)
		table.GraphQLQueryName = namer.RegisterQueryField(pluralTableName)
		singularTableName := singularNamer.Singularize(table.Name)
		table.GraphQLSingleQueryName = singularNamer.RegisterQueryField(singularTableName)
		if hasTypeOverride {
			table.GraphQLSingleTypeName = singularNamer.RegisterTypeName(overrideTypeName, singularTableName)
		} else {
			table.GraphQLSingleTypeName = singularNamer.RegisterType(singularTableName)
		}

		for ci := range table.Columns {
			col := &table.Columns[ci]
			col.GraphQLFieldName = namer.RegisterColumnField(typeName, col.Name)
		}

		for ci := range table.Columns {
			col := &table.Columns[ci]
			if col.IsPrimaryKey && strings.EqualFold(col.Name, "id") {
				desiredName := uniqueDatabaseIDName(table.Columns, ci)
				if desiredName != "databaseId" {
					slog.Default().Warn("GraphQL name collision for databaseId; using fallback name",
						"table", table.Name,
						"column", col.Name,
						"resolved_name", desiredName,
					)
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
			localCols := rel.EffectiveLocalColumns()
			remoteCols := rel.EffectiveRemoteColumns()
			source := fmt.Sprintf("%s:%s:%s", rel.RemoteTable, strings.Join(localCols, ","), strings.Join(remoteCols, ","))
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

func uniqueDatabaseIDName(columns []introspection.Column, skipIndex int) string {
	if !hasColumnFieldName(columns, "databaseId", skipIndex) {
		return "databaseId"
	}

	base := "databaseId_raw"
	candidate := base
	suffix := 2
	for hasColumnFieldName(columns, candidate, skipIndex) {
		candidate = fmt.Sprintf("%s%d", base, suffix)
		suffix++
	}
	return candidate
}

func findTypeOverride(overrides map[string]string, tableName string) (string, bool) {
	for key, value := range overrides {
		if strings.EqualFold(key, tableName) {
			return value, true
		}
	}
	return "", false
}
