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
// Single-database behaviour is unchanged from before Phase 3.
func Apply(schema *introspection.Schema, namer *naming.Namer) {
	applyCore(schema, namer, nil, nil)
}

// ApplyWithNamespaces is like Apply but enables multi-database namespace-prefixed naming.
//
// namespaceMap maps each SQL TABLE_SCHEMA name to its GraphQL namespace alias
// (e.g. "ecommerce" -> "shop"). When the map is non-empty, type names are
// prefixed: Shop_Order, Shop_OrderConnection, etc. Query field names are left
// un-prefixed — they will be mounted on namespace wrapper objects in Phase 4.
//
// namingPerDB maps each database name to its own naming.Config override. Per-db
// TypeOverrides, PluralOverrides, and SingularOverrides take precedence over the
// global namer config for tables in that database, falling back to the global
// config when no per-db entry exists.
//
// Passing nil for either map produces identical behaviour to Apply.
func ApplyWithNamespaces(schema *introspection.Schema, namer *naming.Namer, namespaceMap map[string]string, namingPerDB map[string]naming.Config) {
	applyCore(schema, namer, namespaceMap, namingPerDB)
}

func applyCore(schema *introspection.Schema, namer *naming.Namer, namespaceMap map[string]string, namingPerDB map[string]naming.Config) {
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

		// Resolve the effective per-db namer for name transformation.
		// Only transformation methods (Singularize, Pluralize, ToGraphQLTypeName)
		// are used from this namer; collision registration always goes through the
		// shared global namer / singularNamer so collision state is not fragmented.
		effectiveNamer := namer
		if namingPerDB != nil {
			if dbCfg, ok := namingPerDB[table.Key.Database]; ok {
				effectiveNamer = naming.New(dbCfg, nil)
			}
		}

		// Compute namespace prefix (PascalCase), empty in single-db mode.
		nsPrefix := resolveNamespacePrefix(namer, namespaceMap, table.Key.Database)

		overrideTypeName, hasTypeOverride := findTypeOverride(
			namer.Config().TypeOverrides, namingPerDB, table.Key.Database, table.MapKey(), table.Name,
		)

		var typeName string
		if nsPrefix != "" {
			// Multi-db mode: use singular form with namespace prefix so that
			// Shop_Order, Shop_OrderConnection, etc. all share the same base.
			singularName := effectiveNamer.Singularize(table.Name)
			var baseTypeName string
			if hasTypeOverride {
				baseTypeName = overrideTypeName
			} else {
				baseTypeName = namer.ToGraphQLTypeName(singularName)
			}
			fullTypeName := nsPrefix + "_" + baseTypeName
			typeName = namer.RegisterTypeName(fullTypeName, table.Name)
			// In multi-db mode both the plural-context type and the singular-context
			// type converge to the same namespace-qualified singular name.
			table.GraphQLTypeName = typeName
			table.GraphQLSingleTypeName = typeName
		} else {
			// Single-db mode: identical to the original Apply behaviour.
			if hasTypeOverride {
				typeName = namer.RegisterTypeName(overrideTypeName, table.Name)
			} else {
				typeName = namer.RegisterType(table.Name)
			}
			table.GraphQLTypeName = typeName

			singularTableName := effectiveNamer.Singularize(table.Name)
			if hasTypeOverride {
				table.GraphQLSingleTypeName = singularNamer.RegisterTypeName(overrideTypeName, singularTableName)
			} else {
				table.GraphQLSingleTypeName = singularNamer.RegisterType(singularTableName)
			}
		}

		pluralTableName := effectiveNamer.Pluralize(table.Name)
		table.GraphQLQueryName = namer.RegisterQueryField(pluralTableName)

		singularTableName := effectiveNamer.Singularize(table.Name)
		table.GraphQLSingleQueryName = singularNamer.RegisterQueryField(singularTableName)

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
			// For collision suffix: ManyToOne uses "Ref", all others use "Rel".
			useRefSuffix := rel.IsManyToOne
			rel.GraphQLFieldName = namer.RegisterRelationshipField(typeName, baseName, source, useRefSuffix)
		}
	}

	schema.NamesApplied = true
}

// resolveNamespacePrefix returns the PascalCase namespace prefix for a database,
// or "" when the namespace map is nil/empty or the database has no entry.
func resolveNamespacePrefix(namer *naming.Namer, namespaceMap map[string]string, database string) string {
	if len(namespaceMap) == 0 || database == "" {
		return ""
	}
	ns, ok := namespaceMap[database]
	if !ok || ns == "" {
		return ""
	}
	return namer.ToGraphQLTypeName(ns)
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

// findTypeOverride looks up a GraphQL type override for a table.
//
// Lookup order:
//  1. Per-db overrides (namingPerDB[database].TypeOverrides) — qualified key first,
//     then case-insensitive bare table name.
//  2. Global overrides — qualified key (mapKey) first, then case-insensitive
//     bare table name.
//
// This lets users write dot-delimited overrides ("mydb.users") in multi-db mode
// while keeping bare-name overrides ("users") working in single-db mode.
func findTypeOverride(globalOverrides map[string]string, namingPerDB map[string]naming.Config, database, mapKey, tableName string) (string, bool) {
	// 1. Per-db overrides take precedence.
	if namingPerDB != nil && database != "" {
		if dbCfg, ok := namingPerDB[database]; ok {
			if mapKey != tableName {
				if v, ok := dbCfg.TypeOverrides[mapKey]; ok {
					return v, true
				}
			}
			for key, value := range dbCfg.TypeOverrides {
				if strings.EqualFold(key, tableName) {
					return value, true
				}
			}
		}
	}

	// 2. Global overrides.
	if mapKey != tableName {
		if v, ok := globalOverrides[mapKey]; ok {
			return v, true
		}
	}
	for key, value := range globalOverrides {
		if strings.EqualFold(key, tableName) {
			return value, true
		}
	}
	return "", false
}
