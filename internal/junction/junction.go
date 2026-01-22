// Package junction provides classification logic for database junction tables.
// It identifies many-to-many relationships and classifies them as either pure
// junctions (only FK columns) or attribute junctions (has additional columns).
package junction

import (
	"tidb-graphql/internal/introspection"
)

// Type classifies how a junction table should be represented in GraphQL.
type Type int

const (
	// NotJunction indicates the table is not a junction table.
	NotJunction Type = iota
	// PureJunction indicates a junction with only FK columns.
	// The junction table is hidden and direct M2M fields are generated.
	PureJunction
	// AttributeJunction indicates a junction with additional non-FK columns.
	// The junction is exposed as an edge type with attribute fields.
	AttributeJunction
)

// String returns a human-readable representation of the junction type.
func (t Type) String() string {
	switch t {
	case NotJunction:
		return "NotJunction"
	case PureJunction:
		return "PureJunction"
	case AttributeJunction:
		return "AttributeJunction"
	default:
		return "Unknown"
	}
}

// FKInfo contains foreign key details for junction detection.
type FKInfo struct {
	ColumnName       string // FK column in junction table (e.g., "emp_no")
	ReferencedTable  string // Target table (e.g., "employees")
	ReferencedColumn string // Target column (e.g., "emp_no")
}

// Info contains classification metadata for a junction table.
type Info struct {
	// Table is the junction table name.
	Table string
	// Type indicates pure vs attribute junction.
	Type Type
	// LeftFK is the first foreign key (alphabetically by referenced table).
	LeftFK FKInfo
	// RightFK is the second foreign key.
	RightFK FKInfo
	// AttributeColumns lists non-FK column names (for attribute junctions).
	AttributeColumns []string
}

// Map maps junction table names to their classification info.
type Map map[string]Info

// ToIntrospectionMap converts junction.Map to introspection.JunctionMap.
// This allows passing junction info back to introspection for relationship building
// without creating an import cycle.
func (m Map) ToIntrospectionMap() introspection.JunctionMap {
	result := make(introspection.JunctionMap, len(m))
	for tableName, info := range m {
		var jType introspection.JunctionType
		switch info.Type {
		case PureJunction:
			jType = introspection.JunctionTypePure
		case AttributeJunction:
			jType = introspection.JunctionTypeAttribute
		default:
			continue // Skip non-junctions
		}

		result[tableName] = introspection.JunctionConfig{
			Table: info.Table,
			Type:  jType,
			LeftFK: introspection.JunctionFKInfo{
				ColumnName:       info.LeftFK.ColumnName,
				ReferencedTable:  info.LeftFK.ReferencedTable,
				ReferencedColumn: info.LeftFK.ReferencedColumn,
			},
			RightFK: introspection.JunctionFKInfo{
				ColumnName:       info.RightFK.ColumnName,
				ReferencedTable:  info.RightFK.ReferencedTable,
				ReferencedColumn: info.RightFK.ReferencedColumn,
			},
		}
	}
	return result
}

// ClassifyJunctions analyzes schema tables and returns junction classifications.
// A table is classified as a junction when:
//   - It has exactly 2 foreign keys to different tables
//   - All FK columns are NOT NULL
//   - There is a composite PK or unique index covering all FK columns
//   - Both referenced tables exist in the schema
func ClassifyJunctions(schema *introspection.Schema) Map {
	result := make(Map)
	tableByName := buildTableIndex(schema)

	for _, table := range schema.Tables {
		if table.IsView {
			continue
		}
		if info, ok := classifyTable(table, tableByName); ok {
			result[table.Name] = info
		}
	}
	return result
}

// buildTableIndex creates a lookup map from table name to table.
func buildTableIndex(schema *introspection.Schema) map[string]*introspection.Table {
	index := make(map[string]*introspection.Table, len(schema.Tables))
	for i := range schema.Tables {
		index[schema.Tables[i].Name] = &schema.Tables[i]
	}
	return index
}

// classifyTable checks if a table qualifies as a junction and returns its classification.
func classifyTable(table introspection.Table, tables map[string]*introspection.Table) (Info, bool) {
	// Rule 1: Must have exactly 2 foreign keys
	if len(table.ForeignKeys) != 2 {
		return Info{}, false
	}

	fk1 := table.ForeignKeys[0]
	fk2 := table.ForeignKeys[1]

	// Rule 2: FKs must reference different tables (no self-referential)
	if fk1.ReferencedTable == fk2.ReferencedTable {
		return Info{}, false
	}

	// Verify both referenced tables exist in schema
	if tables[fk1.ReferencedTable] == nil || tables[fk2.ReferencedTable] == nil {
		return Info{}, false
	}

	// Build set of FK column names
	fkColNames := map[string]bool{
		fk1.ColumnName: true,
		fk2.ColumnName: true,
	}

	// Rule 3: All FK columns must be NOT NULL
	for _, col := range table.Columns {
		if fkColNames[col.Name] && col.IsNullable {
			return Info{}, false
		}
	}

	// Rule 4: Must have composite PK or unique index covering all FK columns
	if !hasCoveringConstraint(table, fkColNames) {
		return Info{}, false
	}

	// Classify as pure or attribute junction
	attributeCols := findAttributeColumns(table, fkColNames)
	junctionType := PureJunction
	if len(attributeCols) > 0 {
		junctionType = AttributeJunction
	}

	// Order FKs alphabetically by referenced table for consistent naming
	leftFK, rightFK := orderFKs(fk1, fk2)

	return Info{
		Table:            table.Name,
		Type:             junctionType,
		LeftFK:           leftFK,
		RightFK:          rightFK,
		AttributeColumns: attributeCols,
	}, true
}

// hasCoveringConstraint checks if there's a PK or unique index covering all FK columns.
func hasCoveringConstraint(table introspection.Table, fkCols map[string]bool) bool {
	// Check if PK covers all FK columns
	pkCols := make(map[string]bool)
	for _, col := range table.Columns {
		if col.IsPrimaryKey {
			pkCols[col.Name] = true
		}
	}
	if coversAll(pkCols, fkCols) {
		return true
	}

	// Check unique indexes
	for _, idx := range table.Indexes {
		if !idx.Unique {
			continue
		}
		idxCols := make(map[string]bool)
		for _, col := range idx.Columns {
			idxCols[col] = true
		}
		if coversAll(idxCols, fkCols) {
			return true
		}
	}
	return false
}

// coversAll returns true if 'covering' contains all keys from 'required'.
func coversAll(covering, required map[string]bool) bool {
	for col := range required {
		if !covering[col] {
			return false
		}
	}
	return true
}

// findAttributeColumns returns column names that are not part of any FK.
func findAttributeColumns(table introspection.Table, fkCols map[string]bool) []string {
	var attrs []string
	for _, col := range table.Columns {
		if !fkCols[col.Name] {
			attrs = append(attrs, col.Name)
		}
	}
	return attrs
}

// orderFKs returns FKs ordered alphabetically by referenced table name.
func orderFKs(fk1, fk2 introspection.ForeignKey) (FKInfo, FKInfo) {
	left := FKInfo{
		ColumnName:       fk1.ColumnName,
		ReferencedTable:  fk1.ReferencedTable,
		ReferencedColumn: fk1.ReferencedColumn,
	}
	right := FKInfo{
		ColumnName:       fk2.ColumnName,
		ReferencedTable:  fk2.ReferencedTable,
		ReferencedColumn: fk2.ReferencedColumn,
	}

	if left.ReferencedTable > right.ReferencedTable {
		left, right = right, left
	}
	return left, right
}
