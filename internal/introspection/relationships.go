package introspection

import (
	"context"
	"log/slog"
	"strings"

	"tidb-graphql/internal/naming"
	"tidb-graphql/internal/tablekey"
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

	// Count FKs per (source_table, target_table) pair to determine naming strategy.
	// When multiple FK constraints from the same table point to the same target,
	// we need to use FK column names to disambiguate.
	// Keys use TableKey.MapKey() to avoid collisions when two databases share table names.
	fkCount := make(map[string]map[string]int) // source MapKey → target MapKey → count
	for _, table := range schema.Tables {
		if table.IsView {
			continue
		}
		srcKey := table.MapKey()
		for _, fk := range ForeignKeyConstraints(table) {
			if fkCount[srcKey] == nil {
				fkCount[srcKey] = make(map[string]int)
			}
			// Build the target key: same database unless this is a cross-database FK.
			var dstKey string
			if fk.ReferencedDatabase != "" {
				dstKey = tablekey.TableKey{Database: fk.ReferencedDatabase, Table: fk.ReferencedTable}.MapKey()
			} else {
				dstKey = tablekey.TableKey{Database: table.Key.Database, Table: fk.ReferencedTable}.MapKey()
			}
			fkCount[srcKey][dstKey]++
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
			// Determine the effective database for the remote table.
			remoteDB := table.Key.Database
			if fk.ReferencedDatabase != "" {
				remoteDB = fk.ReferencedDatabase
			}
			// A FK is cross-database only when REFERENCED_TABLE_SCHEMA differs from the
			// source table's own database. When both are introspected from the same database
			// (single-db mode or intra-db FK in multi-db mode), REFERENCED_TABLE_SCHEMA
			// equals the source database name and the FK is intra-database.
			isCrossDatabase := fk.ReferencedDatabase != "" && fk.ReferencedDatabase != table.Key.Database
			rel := Relationship{
				IsManyToOne:     true,
				IsCrossDatabase: isCrossDatabase,
				LocalColumns:    localColumns,
				RemoteTable:     fk.ReferencedTable,
				RemoteColumns:   remoteColumns,
				RemoteTableKey:  tablekey.TableKey{Database: remoteDB, Table: fk.ReferencedTable},
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
				// For intra-database one-to-many: only consider FKs pointing at this table within the same database.
				// Cross-database one-to-many relationships are handled by ResolveCrossDatabaseRelationships.
				// A FK is intra-database when ReferencedDatabase is empty (pre-Phase-2A introspection)
				// OR when ReferencedDatabase equals the source table's own database (post-Phase-2A, where
				// REFERENCED_TABLE_SCHEMA is always populated, even for same-database references).
				isIntraDB := fk.ReferencedDatabase == "" || fk.ReferencedDatabase == otherTable.Key.Database
				if fk.ReferencedTable == table.Name && isIntraDB {
					if len(fk.ColumnNames) != 1 || len(fk.ReferencedColumns) != 1 {
						warnCompositeSkip("one_to_many", otherTable.Name, fk.ConstraintName, fk.ColumnNames, table.Name, fk.ReferencedColumns, "composite_one_to_many_not_supported_in_phase")
						continue
					}
					srcKey := otherTable.MapKey()
					dstKey := tablekey.TableKey{Database: otherTable.Key.Database, Table: fk.ReferencedTable}.MapKey()
					isOnlyFK := fkCount[srcKey][dstKey] == 1
					rel := Relationship{
						IsOneToMany:      true,
						LocalColumns:     append([]string(nil), fk.ReferencedColumns...),
						RemoteTable:      otherTable.Name,
						RemoteColumns:    append([]string(nil), fk.ColumnNames...),
						RemoteTableKey:   tablekey.TableKey{Database: otherTable.Key.Database, Table: otherTable.Name},
						GraphQLFieldName: namer.OneToManyFieldName(otherTable.Name, fk.ColumnNames[0], isOnlyFK),
					}
					table.Relationships = append(table.Relationships, rel)
				}
			}
		}
	}

	// Third pass: Create M2M relationships for junction tables.
	// tableByKey is keyed primarily by Table.MapKey() to avoid collisions when two
	// databases share table names. It is also indexed by bare table name as a fallback
	// so that junction lookups using jc.Table (bare name from JunctionMap) still
	// resolve correctly after the Phase 0 migration to qualified keys.
	tableByKey := make(map[string]*Table, len(schema.Tables)*2)
	for i := range schema.Tables {
		t := &schema.Tables[i]
		mapKey := t.MapKey()
		tableByKey[mapKey] = t
		// Also index by bare name when the qualified key differs (single-db mode with
		// Key.Database set, or multi-db where two databases may share names — first
		// writer wins for the bare-name slot).
		if mapKey != t.Name {
			if _, exists := tableByKey[t.Name]; !exists {
				tableByKey[t.Name] = t
			}
		}
	}

	for _, jc := range junctions {
		// Junctions are always intra-database; find tables using same-db key.
		// Derive the database from the junction table itself.
		junctionKey := tablekey.TableKey{Table: jc.Table}
		if jt, ok := tableByKey[jc.Table]; ok {
			junctionKey = jt.Key
		}
		leftKey := tablekey.TableKey{Database: junctionKey.Database, Table: jc.LeftFK.ReferencedTable}.MapKey()
		rightKey := tablekey.TableKey{Database: junctionKey.Database, Table: jc.RightFK.ReferencedTable}.MapKey()
		leftTable := tableByKey[leftKey]
		rightTable := tableByKey[rightKey]
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
				RemoteTableKey:          rightTable.Key,
				JunctionTable:           jc.Table,
				JunctionTableKey:        junctionKey,
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
				RemoteTableKey:          leftTable.Key,
				JunctionTable:           jc.Table,
				JunctionTableKey:        junctionKey,
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
				RemoteTableKey:          junctionKey,
				JunctionTable:           jc.Table,
				JunctionTableKey:        junctionKey,
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
				RemoteTableKey:          junctionKey,
				JunctionTable:           jc.Table,
				JunctionTableKey:        junctionKey,
				JunctionLocalFKColumns:  append([]string(nil), rightJunctionCols...),
				JunctionRemoteFKColumns: append([]string(nil), leftJunctionCols...),
				GraphQLFieldName:        edgeFieldName,
			})
		}
	}

	return nil
}

// ResolveCrossDatabaseRelationships adds one-to-many relationships across database
// boundaries in a merged schema. It must be called after all per-database
// introspection and relationship building is complete.
//
// During per-database relationship building, many-to-one relationships are already
// created for cross-database FKs (IsCrossDatabase=true). This function adds the
// reverse one-to-many relationships on the referenced tables — but only when the
// referenced database is present in the merged schema (i.e. a configured database).
// FKs referencing unconfigured databases are silently skipped.
//
// Composite FK one-to-many are skipped, matching the intra-database limitation.
func ResolveCrossDatabaseRelationships(schema *Schema, namer *naming.Namer) error {
	// Build a full tableIndex across all databases, keyed by TableKey.MapKey().
	tableIndex := make(map[string]*Table)
	for i := range schema.Tables {
		tableIndex[schema.Tables[i].MapKey()] = &schema.Tables[i]
	}

	// Count cross-db FKs per (source, referenced) pair for naming disambiguation.
	// isOnlyFK is true when there's exactly one FK from the source table to the
	// referenced table, allowing the pluralised table name to be used directly.
	fkCount := make(map[string]map[string]int) // srcMapKey -> dstMapKey -> count
	for _, table := range schema.Tables {
		srcKey := table.MapKey()
		for _, fk := range ForeignKeyConstraints(table) {
			// Only count genuine cross-database FKs (same fix as the skip guard below).
			if fk.ReferencedDatabase == "" || fk.ReferencedDatabase == table.Key.Database {
				continue
			}
			dstKey := tablekey.TableKey{Database: fk.ReferencedDatabase, Table: fk.ReferencedTable}.MapKey()
			if fkCount[srcKey] == nil {
				fkCount[srcKey] = make(map[string]int)
			}
			fkCount[srcKey][dstKey]++
		}
	}

	// Add O2M reverse relationships on the referenced (remote) tables.
	for i := range schema.Tables {
		table := &schema.Tables[i]
		for _, fk := range ForeignKeyConstraints(*table) {
			// Skip intra-database FKs — they are handled by buildRelationships.
			// ReferencedDatabase may be empty (pre-Phase-2A introspection) or equal to the
			// source table's own database (post-Phase-2A, where REFERENCED_TABLE_SCHEMA is
			// always populated by the FK query even for same-database references).
			if fk.ReferencedDatabase == "" || fk.ReferencedDatabase == table.Key.Database {
				continue // intra-db, already handled by buildRelationships
			}
			if len(fk.ColumnNames) != 1 || len(fk.ReferencedColumns) != 1 {
				// Composite cross-db FK one-to-many not yet supported.
				continue
			}

			refKey := tablekey.TableKey{Database: fk.ReferencedDatabase, Table: fk.ReferencedTable}.MapKey()
			refTable := tableIndex[refKey]
			if refTable == nil {
				// Referenced database is not in the configured set; skip silently.
				continue
			}

			srcKey := table.MapKey()
			isOnlyFK := fkCount[srcKey][refKey] == 1
			refTable.Relationships = append(refTable.Relationships, Relationship{
				IsOneToMany:      true,
				IsCrossDatabase:  true,
				LocalColumns:     append([]string(nil), fk.ReferencedColumns...),
				RemoteTable:      table.Name,
				RemoteColumns:    append([]string(nil), fk.ColumnNames...),
				RemoteTableKey:   table.Key,
				GraphQLFieldName: namer.OneToManyFieldName(table.Name, fk.ColumnNames[0], isOnlyFK),
			})
		}
	}

	return nil
}
