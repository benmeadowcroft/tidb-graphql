// Package schemafilter applies allow/deny filters to schema snapshots.
package schemafilter

import (
	"context"
	"path"
	"slices"
	"strings"

	"tidb-graphql/internal/introspection"
)

// Config controls allow/deny filters for tables and columns.
type Config struct {
	AllowTables      []string            `mapstructure:"allow_tables"`
	DenyTables       []string            `mapstructure:"deny_tables"`
	ScanViewsEnabled bool                `mapstructure:"scan_views_enabled"`
	AllowColumns     map[string][]string `mapstructure:"allow_columns"`
	DenyColumns      map[string][]string `mapstructure:"deny_columns"`
	// DenyMutationTables and DenyMutationColumns apply additional restrictions to writes.
	// They do not affect query visibility and are evaluated during mutation schema generation.
	DenyMutationTables  []string            `mapstructure:"deny_mutation_tables"`
	DenyMutationColumns map[string][]string `mapstructure:"deny_mutation_columns"`
}

// Apply filters tables, columns, indexes, and relationships in place.
// Missing allow lists default to allow-all; deny rules always win.
func Apply(ctx context.Context, schema *introspection.Schema, cfg Config) {
	if schema == nil {
		return
	}

	allowedTableNames := make(map[string]bool)
	filteredTables := make([]introspection.Table, 0, len(schema.Tables))
	for _, table := range schema.Tables {
		if table.IsView && !cfg.ScanViewsEnabled {
			continue
		}
		if !tableAllowed(table.Name, cfg.AllowTables, cfg.DenyTables) {
			continue
		}
		filteredTables = append(filteredTables, table)
		allowedTableNames[table.Name] = true
	}

	if len(filteredTables) == 0 {
		schema.Tables = nil
		return
	}

	allowedColumnsByTable := make(map[string]map[string]bool, len(filteredTables))
	for i := range filteredTables {
		table := &filteredTables[i]
		allowedColumns := make(map[string]bool)
		filteredColumns := make([]introspection.Column, 0, len(table.Columns))
		for _, column := range table.Columns {
			if !columnAllowed(table.Name, column.Name, cfg.AllowColumns, cfg.DenyColumns) {
				continue
			}
			filteredColumns = append(filteredColumns, column)
			allowedColumns[column.Name] = true
		}

		table.Columns = filteredColumns
		allowedColumnsByTable[table.Name] = allowedColumns
	}

	finalTables := make([]introspection.Table, 0, len(filteredTables))
	for _, table := range filteredTables {
		if len(table.Columns) == 0 {
			continue
		}

		allowedColumns := allowedColumnsByTable[table.Name]
		table.Indexes = filterIndexes(table.Indexes, allowedColumns)
		table.ForeignKeys = filterForeignKeys(table.ForeignKeys, allowedColumns, allowedTableNames, allowedColumnsByTable)
		table.Relationships = nil
		finalTables = append(finalTables, table)
	}

	schema.Tables = finalTables
	if len(schema.Tables) == 0 {
		return
	}

	_ = introspection.RebuildRelationships(ctx, schema)
}

func tableAllowed(table string, allow, deny []string) bool {
	if matchesAny(table, deny) {
		return false
	}
	if len(allow) == 0 {
		return true
	}
	return matchesAny(table, allow)
}

func columnAllowed(table, column string, allow, deny map[string][]string) bool {
	denyPatterns := mergePatterns(deny, table)
	if matchesAny(column, denyPatterns) {
		return false
	}
	allowPatterns := mergePatterns(allow, table)
	if len(allowPatterns) == 0 {
		return true
	}
	return matchesAny(column, allowPatterns)
}

func mergePatterns(patterns map[string][]string, table string) []string {
	if patterns == nil {
		return nil
	}
	combined := append([]string{}, patterns["*"]...)
	combined = append(combined, patterns[table]...)
	return slices.Compact(combined)
}

func filterIndexes(indexes []introspection.Index, allowedColumns map[string]bool) []introspection.Index {
	filtered := make([]introspection.Index, 0, len(indexes))
	for _, idx := range indexes {
		keep := true
		for _, col := range idx.Columns {
			if !allowedColumns[col] {
				keep = false
				break
			}
		}
		if keep {
			filtered = append(filtered, idx)
		}
	}
	return filtered
}

func filterForeignKeys(fks []introspection.ForeignKey, allowedColumns map[string]bool, allowedTables map[string]bool, allowedColumnsByTable map[string]map[string]bool) []introspection.ForeignKey {
	filtered := make([]introspection.ForeignKey, 0, len(fks))
	for _, fk := range fks {
		if !allowedColumns[fk.ColumnName] {
			continue
		}
		if !allowedTables[fk.ReferencedTable] {
			continue
		}
		remoteColumns := allowedColumnsByTable[fk.ReferencedTable]
		if remoteColumns == nil || !remoteColumns[fk.ReferencedColumn] {
			continue
		}
		filtered = append(filtered, fk)
	}
	return filtered
}

func matchesAny(value string, patterns []string) bool {
	value = strings.ToLower(value)
	for _, pattern := range patterns {
		if pattern == "" {
			continue
		}
		// matching should be case-insensitive
		ok, err := path.Match(strings.ToLower(pattern), value)
		if err != nil {
			continue
		}
		if ok {
			return true
		}
	}
	return false
}

// MutationTableAllowed reports whether a table is eligible for mutations.
// It only applies deny lists and keeps matching logic consistent with query filters.
func MutationTableAllowed(table string, cfg Config) bool {
	return !matchesAny(table, cfg.DenyMutationTables)
}

// MutationColumnAllowed reports whether a column is eligible for mutation inputs.
// It only applies deny lists and keeps matching logic consistent with query filters.
func MutationColumnAllowed(table, column string, cfg Config) bool {
	denyPatterns := mergePatterns(cfg.DenyMutationColumns, table)
	return !matchesAny(column, denyPatterns)
}
