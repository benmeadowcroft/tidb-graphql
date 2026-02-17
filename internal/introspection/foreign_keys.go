package introspection

import (
	"fmt"
	"sort"
)

// ForeignKeyConstraint groups per-column KEY_COLUMN_USAGE rows into an ordered FK constraint mapping.
type ForeignKeyConstraint struct {
	ConstraintName    string
	ReferencedTable   string
	ColumnNames       []string
	ReferencedColumns []string
}

// ForeignKeyConstraints returns FK constraints for a table with deterministic ordering.
func ForeignKeyConstraints(table Table) []ForeignKeyConstraint {
	if len(table.ForeignKeys) == 0 {
		return nil
	}

	type row struct {
		key   string
		fk    ForeignKey
		index int
	}
	rows := make([]row, 0, len(table.ForeignKeys))
	for i, fk := range table.ForeignKeys {
		key := fk.ConstraintName
		if key == "" {
			// Unnamed constraints appear in tests; keep them isolated to avoid accidental merging.
			key = fmt.Sprintf("__unnamed_%d", i)
		}
		rows = append(rows, row{key: key, fk: fk, index: i})
	}

	sort.SliceStable(rows, func(i, j int) bool {
		if rows[i].key != rows[j].key {
			return rows[i].key < rows[j].key
		}
		iPos := rows[i].fk.OrdinalPosition
		jPos := rows[j].fk.OrdinalPosition
		if iPos != jPos {
			if iPos == 0 {
				return false
			}
			if jPos == 0 {
				return true
			}
			return iPos < jPos
		}
		if rows[i].fk.ColumnName != rows[j].fk.ColumnName {
			return rows[i].fk.ColumnName < rows[j].fk.ColumnName
		}
		return rows[i].index < rows[j].index
	})

	orderedKeys := make([]string, 0)
	keySeen := make(map[string]struct{})
	grouped := make(map[string]*ForeignKeyConstraint)

	for _, item := range rows {
		group, ok := grouped[item.key]
		if !ok {
			group = &ForeignKeyConstraint{
				ConstraintName:  item.fk.ConstraintName,
				ReferencedTable: item.fk.ReferencedTable,
			}
			grouped[item.key] = group
			if _, exists := keySeen[item.key]; !exists {
				keySeen[item.key] = struct{}{}
				orderedKeys = append(orderedKeys, item.key)
			}
		}
		group.ColumnNames = append(group.ColumnNames, item.fk.ColumnName)
		group.ReferencedColumns = append(group.ReferencedColumns, item.fk.ReferencedColumn)
	}

	result := make([]ForeignKeyConstraint, 0, len(orderedKeys))
	for _, key := range orderedKeys {
		result = append(result, *grouped[key])
	}
	return result
}
