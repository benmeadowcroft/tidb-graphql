package introspection

import (
	"context"
	"log/slog"
	"strings"

	"tidb-graphql/internal/naming"
)

// buildRelationships creates bidirectional relationship metadata from foreign keys.
// If junctions is provided, it will also create M2M relationships for junction tables.
func buildRelationships(ctx context.Context, schema *Schema, namer *naming.Namer, junctions JunctionMap) error {
	_, span := startSpan(ctx, "introspection.build_relationships")
	defer span.End()

	if junctions == nil {
		junctions = make(JunctionMap)
	}

	// Build set of junction table types for quick checks
	junctionTypes := make(map[string]JunctionType)
	for tableName, jc := range junctions {
		junctionTypes[tableName] = jc.Type
	}

	// Emit each unsupported composite warning once per schema build.
	warnedComposite := make(map[string]struct{})
	warnCompositeSkip := func(kind, tableName, constraintName string, localCols []string, remoteTable string, remoteCols []string, reason string) {
		key := strings.Join([]string{
			kind,
			tableName,
			constraintName,
			strings.Join(localCols, ","),
			remoteTable,
			strings.Join(remoteCols, ","),
			reason,
		}, "|")
		if _, seen := warnedComposite[key]; seen {
			return
		}
		warnedComposite[key] = struct{}{}
		slog.Default().Warn("skipping unsupported composite relationship mapping",
			"kind", kind,
			"table", tableName,
			"constraint", constraintName,
			"local_columns", localCols,
			"remote_table", remoteTable,
			"remote_columns", remoteCols,
			"reason", reason,
		)
	}

	// Count FKs per (source_table, target_table) pair to determine naming strategy
	// When multiple FK constraints from the same table point to the same target,
	// we need to use FK column names to disambiguate.
	fkCount := make(map[string]map[string]int) // source → target → count
	for _, table := range schema.Tables {
		if table.IsView {
			continue
		}
		for _, fk := range ForeignKeyConstraints(table) {
			if fkCount[table.Name] == nil {
				fkCount[table.Name] = make(map[string]int)
			}
			fkCount[table.Name][fk.ReferencedTable]++
		}
	}

	// First pass: Create many-to-one relationships from FK columns
	// Uses FK column name (minus _id suffix) for the field name, except
	// attribute junctions which use the referenced table name.
	// Skip for pure junction tables (they are hidden)
	for i := range schema.Tables {
		table := &schema.Tables[i]
		if table.IsView {
			continue
		}

		// Skip M2O relationships for pure junction tables (hidden)
		jType := junctionTypes[table.Name]
		if jType == JunctionTypePure {
			continue
		}

		for _, fk := range ForeignKeyConstraints(*table) {
			if len(fk.ColumnNames) == 0 || len(fk.ColumnNames) != len(fk.ReferencedColumns) {
				warnCompositeSkip("many_to_one", table.Name, fk.ConstraintName, fk.ColumnNames, fk.ReferencedTable, fk.ReferencedColumns, "invalid_foreign_key_mapping")
				continue
			}
			fieldName := ""
			if jType == JunctionTypeAttribute {
				fieldName = namer.JunctionEdgeRefFieldName(fk.ReferencedTable)
			} else {
				fieldName = namer.ManyToOneFieldName(fk.ColumnNames[0])
			}
			localColumns := append([]string(nil), fk.ColumnNames...)
			remoteColumns := append([]string(nil), fk.ReferencedColumns...)
			rel := Relationship{
				IsManyToOne:      true,
				LocalColumns:     localColumns,
				RemoteTable:      fk.ReferencedTable,
				RemoteColumns:    remoteColumns,
				GraphQLFieldName: fieldName,
			}
			table.Relationships = append(table.Relationships, rel)
		}
	}

	// Second pass: Create one-to-many relationships (reverse direction)
	// Skip if the source table is any junction (edge or pure)
	// When single FK: use pluralized table name (e.g., "comments")
	// When multiple FKs: prefix with FK column name (e.g., "authorPosts", "editorPosts")
	for i := range schema.Tables {
		table := &schema.Tables[i]
		if table.IsView {
			continue
		}

		// Find all tables that reference this table
		for j := range schema.Tables {
			otherTable := &schema.Tables[j]
			if otherTable.IsView {
				continue
			}

			// Skip one-to-many from junction tables (edge or pure)
			if junctionTypes[otherTable.Name] != JunctionTypeNone {
				continue
			}

			for _, fk := range ForeignKeyConstraints(*otherTable) {
				if fk.ReferencedTable == table.Name {
					if len(fk.ColumnNames) != 1 || len(fk.ReferencedColumns) != 1 {
						warnCompositeSkip("one_to_many", otherTable.Name, fk.ConstraintName, fk.ColumnNames, table.Name, fk.ReferencedColumns, "composite_one_to_many_not_supported_in_phase")
						continue
					}
					isOnlyFK := fkCount[otherTable.Name][table.Name] == 1
					rel := Relationship{
						IsOneToMany:      true,
						LocalColumns:     append([]string(nil), fk.ReferencedColumns...),
						RemoteTable:      otherTable.Name,
						RemoteColumns:    append([]string(nil), fk.ColumnNames...),
						GraphQLFieldName: namer.OneToManyFieldName(otherTable.Name, fk.ColumnNames[0], isOnlyFK),
					}
					table.Relationships = append(table.Relationships, rel)
				}
			}
		}
	}

	// Third pass: Create M2M relationships for junction tables
	tableByName := make(map[string]*Table)
	for i := range schema.Tables {
		tableByName[schema.Tables[i].Name] = &schema.Tables[i]
	}

	for _, jc := range junctions {
		leftTable := tableByName[jc.LeftFK.ReferencedTable]
		rightTable := tableByName[jc.RightFK.ReferencedTable]
		if leftTable == nil || rightTable == nil {
			continue
		}

		// M2M/Edge mappings require full key-column alignment across endpoints and junction FKs.
		leftPKCols := PrimaryKeyColumns(*leftTable)
		rightPKCols := PrimaryKeyColumns(*rightTable)
		if len(leftPKCols) == 0 || len(rightPKCols) == 0 {
			continue
		}
		leftPKNames := columnNamesFromColumns(leftPKCols)
		rightPKNames := columnNamesFromColumns(rightPKCols)
		leftJunctionCols := jc.LeftFK.EffectiveColumnNames()
		rightJunctionCols := jc.RightFK.EffectiveColumnNames()

		switch jc.Type {
		case JunctionTypePure:
			if len(leftPKNames) != len(leftJunctionCols) || len(rightPKNames) != len(rightJunctionCols) {
				warnCompositeSkip("many_to_many", jc.Table, jc.LeftFK.ConstraintName, leftJunctionCols, jc.LeftFK.ReferencedTable, leftPKNames, "left_mapping_key_count_mismatch")
				warnCompositeSkip("many_to_many", jc.Table, jc.RightFK.ConstraintName, rightJunctionCols, jc.RightFK.ReferencedTable, rightPKNames, "right_mapping_key_count_mismatch")
				continue
			}
			// Pure junction: create direct M2M fields, hide junction table
			// Add M2M from left to right
			leftFieldName := namer.JunctionFieldName(jc.Table, leftTable.Name, rightTable.Name, rightTable.Name, false)
			rightFieldName := namer.JunctionFieldName(jc.Table, leftTable.Name, rightTable.Name, leftTable.Name, false)
			leftTable.Relationships = append(leftTable.Relationships, Relationship{
				IsManyToMany:            true,
				LocalColumns:            append([]string(nil), leftPKNames...),
				RemoteTable:             rightTable.Name,
				RemoteColumns:           append([]string(nil), rightPKNames...),
				JunctionTable:           jc.Table,
				JunctionLocalFKColumns:  append([]string(nil), leftJunctionCols...),
				JunctionRemoteFKColumns: append([]string(nil), rightJunctionCols...),
				GraphQLFieldName:        leftFieldName,
			})
			// Add M2M from right to left
			rightTable.Relationships = append(rightTable.Relationships, Relationship{
				IsManyToMany:            true,
				LocalColumns:            append([]string(nil), rightPKNames...),
				RemoteTable:             leftTable.Name,
				RemoteColumns:           append([]string(nil), leftPKNames...),
				JunctionTable:           jc.Table,
				JunctionLocalFKColumns:  append([]string(nil), rightJunctionCols...),
				JunctionRemoteFKColumns: append([]string(nil), leftJunctionCols...),
				GraphQLFieldName:        rightFieldName,
			})

		case JunctionTypeAttribute:
			if len(leftPKNames) != len(leftJunctionCols) || len(rightPKNames) != len(rightJunctionCols) {
				warnCompositeSkip("edge_list", jc.Table, jc.LeftFK.ConstraintName, leftJunctionCols, jc.LeftFK.ReferencedTable, leftPKNames, "left_mapping_key_count_mismatch")
				warnCompositeSkip("edge_list", jc.Table, jc.RightFK.ConstraintName, rightJunctionCols, jc.RightFK.ReferencedTable, rightPKNames, "right_mapping_key_count_mismatch")
				continue
			}
			// Attribute junction: create edge list fields
			edgeFieldName := namer.JunctionFieldName(jc.Table, leftTable.Name, rightTable.Name, "", true)

			// Add edge list from left to junction
			leftTable.Relationships = append(leftTable.Relationships, Relationship{
				IsEdgeList:              true,
				LocalColumns:            append([]string(nil), leftPKNames...),
				RemoteTable:             jc.Table, // Points to junction table for edge type
				RemoteColumns:           append([]string(nil), leftJunctionCols...),
				JunctionTable:           jc.Table,
				JunctionLocalFKColumns:  append([]string(nil), leftJunctionCols...),
				JunctionRemoteFKColumns: append([]string(nil), rightJunctionCols...),
				GraphQLFieldName:        edgeFieldName,
			})
			// Add edge list from right to junction
			rightTable.Relationships = append(rightTable.Relationships, Relationship{
				IsEdgeList:              true,
				LocalColumns:            append([]string(nil), rightPKNames...),
				RemoteTable:             jc.Table, // Points to junction table for edge type
				RemoteColumns:           append([]string(nil), rightJunctionCols...),
				JunctionTable:           jc.Table,
				JunctionLocalFKColumns:  append([]string(nil), rightJunctionCols...),
				JunctionRemoteFKColumns: append([]string(nil), leftJunctionCols...),
				GraphQLFieldName:        edgeFieldName,
			})
		}
	}

	return nil
}
