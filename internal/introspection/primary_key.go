package introspection

// PrimaryKeyColumn returns the first primary key column for a table, if present.
// For tables with composite primary keys, use PrimaryKeyColumns instead.
func PrimaryKeyColumn(table Table) *Column {
	for i := range table.Columns {
		if table.Columns[i].IsPrimaryKey {
			return &table.Columns[i]
		}
	}
	return nil
}

// PrimaryKeyColumns returns all primary key columns for a table in column order.
// Returns an empty slice if the table has no primary key.
func PrimaryKeyColumns(table Table) []Column {
	var cols []Column
	for _, col := range table.Columns {
		if col.IsPrimaryKey {
			cols = append(cols, col)
		}
	}
	return cols
}
