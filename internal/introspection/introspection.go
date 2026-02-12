// Package introspection discovers database schema metadata from TiDB's information_schema.
// It extracts tables, columns, indexes, foreign keys, and relationships for use in GraphQL schema generation.
package introspection

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"strings"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"tidb-graphql/internal/naming"
	"tidb-graphql/internal/sqltype"
)

// Column represents a database column
type Column struct {
	Name            string
	DataType        string
	ColumnType      string
	IsNullable      bool
	IsPrimaryKey    bool
	IsGenerated     bool
	IsAutoIncrement bool
	IsAutoRandom    bool
	HasDefault      bool
	ColumnDefault   string
	EnumValues      []string
	Comment         string
	// OverrideType is an explicit GraphQL type override resolved during schema preparation.
	OverrideType    sqltype.GraphQLType
	HasOverrideType bool
	// GraphQLFieldName is the resolved GraphQL field name for this column.
	GraphQLFieldName string
}

// Index represents a database index with ordered columns.
type Index struct {
	Name    string
	Unique  bool
	Columns []string
}

// ForeignKey represents a foreign key constraint on a column
type ForeignKey struct {
	ColumnName       string // e.g., "volume_id"
	ReferencedTable  string // e.g., "volumes"
	ReferencedColumn string // e.g., "id"
	ConstraintName   string // e.g., "books_ibfk_1"
}

// Relationship represents either direction of a FK relationship
type Relationship struct {
	IsManyToOne      bool
	IsOneToMany      bool
	IsManyToMany     bool   // Direct M2M through pure junction (junction hidden)
	IsEdgeList       bool   // M2M through attribute junction (edge type visible)
	LocalColumn      string // For many-to-one: FK column; for one-to-many: PK column; for M2M: PK column
	RemoteTable      string // The related table name (for M2M: the other entity table, not junction)
	RemoteColumn     string // For many-to-one: referenced column; for one-to-many: FK column in remote table; for M2M: PK column in remote table
	JunctionTable    string // For M2M relationships: the intermediate junction table name
	JunctionLocalFK  string // For M2M: FK column in junction pointing to this table
	JunctionRemoteFK string // For M2M: FK column in junction pointing to remote table
	GraphQLFieldName string // e.g., "volume" or "books" or "departmentEmployees"
}

// JunctionType indicates how a junction table should be handled.
type JunctionType int

const (
	// JunctionTypeNone indicates the table is not a junction.
	JunctionTypeNone JunctionType = iota
	// JunctionTypePure indicates a junction with only FK columns (hidden in GraphQL).
	JunctionTypePure
	// JunctionTypeAttribute indicates a junction with extra columns (exposed as edge type).
	JunctionTypeAttribute
)

// JunctionFKInfo contains foreign key details for a junction relationship.
type JunctionFKInfo struct {
	ColumnName       string // FK column in junction table
	ReferencedTable  string // Target table
	ReferencedColumn string // Target column
}

// JunctionConfig contains configuration for a single junction table.
type JunctionConfig struct {
	Table   string         // Junction table name
	Type    JunctionType   // Pure or Attribute
	LeftFK  JunctionFKInfo // First FK (alphabetically by referenced table)
	RightFK JunctionFKInfo // Second FK
}

// JunctionMap maps junction table names to their configuration.
type JunctionMap map[string]JunctionConfig

// Table represents a database table
type Table struct {
	Name    string
	IsView  bool
	Comment string
	// GraphQLTypeName is the resolved GraphQL type name for this table.
	GraphQLTypeName string
	// GraphQLQueryName is the resolved GraphQL root field name for this table.
	GraphQLQueryName string
	// GraphQLSingleQueryName is the resolved root field name prefix for single-row lookups.
	GraphQLSingleQueryName string
	// GraphQLSingleTypeName is the resolved type name used for singular operations (mutations, payloads).
	GraphQLSingleTypeName string
	Columns               []Column
	ForeignKeys           []ForeignKey
	Relationships         []Relationship
	Indexes               []Index
}

// Schema represents the introspected database schema
type Schema struct {
	Tables    []Table
	Junctions JunctionMap
	// NamesApplied marks whether GraphQL naming has been applied to this schema.
	NamesApplied bool
}

// Queryer provides query access for schema introspection.
type Queryer interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

// IntrospectDatabase queries TiDB's information_schema to discover tables and columns.
func IntrospectDatabase(db *sql.DB, databaseName string) (*Schema, error) {
	return IntrospectDatabaseContext(context.Background(), db, databaseName)
}

// IntrospectDatabaseContext queries TiDB's information_schema with context support.
func IntrospectDatabaseContext(ctx context.Context, db Queryer, databaseName string) (*Schema, error) {
	ctx, span := startSpan(ctx, "introspection.build_schema",
		attribute.String("db.name", databaseName),
	)
	defer span.End()

	schema := &Schema{
		Tables: []Table{},
	}

	// Get all tables
	tables, err := getTables(ctx, db, databaseName)
	if err != nil {
		recordSpanError(span, err)
		return nil, fmt.Errorf("failed to get tables: %w", err)
	}

	// Get columns for each table
	for _, tableInfo := range tables {
		columns, err := getColumns(ctx, db, databaseName, tableInfo.Name)
		if err != nil {
			recordSpanError(span, err)
			return nil, fmt.Errorf("failed to get columns for %s: %w", tableInfo.Name, err)
		}
		if !tableInfo.IsView {
			columns = applyAutoRandomColumns(ctx, db, tableInfo.Name, columns)
		}

		var primaryKeys []string
		var foreignKeys []ForeignKey
		var indexes []Index
		if !tableInfo.IsView {
			primaryKeys, err = getPrimaryKeys(ctx, db, databaseName, tableInfo.Name)
			if err != nil {
				recordSpanError(span, err)
				return nil, fmt.Errorf("failed to get primary keys for table %s: %w", tableInfo.Name, err)
			}

			foreignKeys, err = getForeignKeys(ctx, db, databaseName, tableInfo.Name)
			if err != nil {
				recordSpanError(span, err)
				return nil, fmt.Errorf("failed to get foreign keys for table %s: %w", tableInfo.Name, err)
			}

			indexes, err = getIndexes(ctx, db, databaseName, tableInfo.Name)
			if err != nil {
				recordSpanError(span, err)
				return nil, fmt.Errorf("failed to get indexes for table %s: %w", tableInfo.Name, err)
			}
		}

		// Mark primary key columns
		for i := range columns {
			for _, pk := range primaryKeys {
				if columns[i].Name == pk {
					columns[i].IsPrimaryKey = true
					break
				}
			}
		}

		schema.Tables = append(schema.Tables, Table{
			Name:        tableInfo.Name,
			IsView:      tableInfo.IsView,
			Comment:     tableInfo.Comment,
			Columns:     columns,
			ForeignKeys: foreignKeys,
			Indexes:     indexes,
		})
	}

	// Build bidirectional relationships after all tables are loaded
	// Use default namer for basic introspection (no junction awareness at this level)
	namer := naming.Default()
	if err := buildRelationships(ctx, schema, namer, nil); err != nil {
		recordSpanError(span, err)
		return nil, fmt.Errorf("failed to build relationships: %w", err)
	}

	return schema, nil
}

// RebuildRelationships clears and rebuilds relationship metadata for a schema.
func RebuildRelationships(schema *Schema) error {
	return RebuildRelationshipsWithNamer(schema, naming.Default())
}

// RebuildRelationshipsWithNamer clears and rebuilds relationship metadata using a custom namer.
func RebuildRelationshipsWithNamer(schema *Schema, namer *naming.Namer) error {
	if schema == nil {
		return nil
	}
	for i := range schema.Tables {
		schema.Tables[i].Relationships = nil
	}
	return buildRelationships(context.Background(), schema, namer, nil)
}

// RebuildRelationshipsWithJunctions clears and rebuilds relationship metadata with junction awareness.
func RebuildRelationshipsWithJunctions(schema *Schema, namer *naming.Namer, junctions JunctionMap) error {
	if schema == nil {
		return nil
	}
	schema.Junctions = junctions
	for i := range schema.Tables {
		schema.Tables[i].Relationships = nil
	}
	return buildRelationships(context.Background(), schema, namer, junctions)
}

type tableInfo struct {
	Name    string
	IsView  bool
	Comment string
}

func getTables(ctx context.Context, db Queryer, databaseName string) ([]tableInfo, error) {
	ctx, span := startSpan(ctx, "introspection.get_tables",
		attribute.String("db.name", databaseName),
	)
	defer span.End()

	query := `
		SELECT TABLE_NAME, TABLE_TYPE, TABLE_COMMENT
		FROM INFORMATION_SCHEMA.TABLES
		WHERE TABLE_SCHEMA = ?
		AND TABLE_TYPE IN ('BASE TABLE', 'VIEW')
		ORDER BY TABLE_NAME
	`

	rows, err := db.QueryContext(ctx, query, databaseName)
	if err != nil {
		recordSpanError(span, err)
		return nil, err
	}
	defer func() {
		_ = rows.Close()
	}()

	var tables []tableInfo
	for rows.Next() {
		var tableName string
		var tableType string
		var tableComment sql.NullString
		if err := rows.Scan(&tableName, &tableType, &tableComment); err != nil {
			recordSpanError(span, err)
			return nil, err
		}
		comment := ""
		if tableComment.Valid {
			comment = strings.TrimSpace(tableComment.String)
		}
		tables = append(tables, tableInfo{
			Name:    tableName,
			IsView:  strings.EqualFold(tableType, "VIEW"),
			Comment: comment,
		})
	}

	if err := rows.Err(); err != nil {
		recordSpanError(span, err)
		return nil, err
	}
	return tables, nil
}

func getColumns(ctx context.Context, db Queryer, databaseName, tableName string) ([]Column, error) {
	ctx, span := startSpan(ctx, "introspection.get_columns",
		attribute.String("db.name", databaseName),
		attribute.String("db.table", tableName),
	)
	defer span.End()

	query := `
		SELECT
			COLUMN_NAME,
			DATA_TYPE,
			COLUMN_TYPE,
			COLUMN_COMMENT,
			IS_NULLABLE,
			COLUMN_DEFAULT,
			EXTRA
		FROM INFORMATION_SCHEMA.COLUMNS
		WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
		ORDER BY ORDINAL_POSITION
	`

	rows, err := db.QueryContext(ctx, query, databaseName, tableName)
	if err != nil {
		recordSpanError(span, err)
		return nil, err
	}
	defer func() {
		_ = rows.Close()
	}()

	var columns []Column
	for rows.Next() {
		var col Column
		var isNullable string
		var columnDefault sql.NullString
		var extra string
		var columnType string
		var columnComment sql.NullString
		if err := rows.Scan(&col.Name, &col.DataType, &columnType, &columnComment, &isNullable, &columnDefault, &extra); err != nil {
			recordSpanError(span, err)
			return nil, err
		}
		col.ColumnType = columnType
		if columnComment.Valid {
			col.Comment = strings.TrimSpace(columnComment.String)
		}
		col.IsNullable = strings.ToUpper(isNullable) == "YES"
		if columnDefault.Valid {
			col.ColumnDefault = columnDefault.String
			col.HasDefault = true
		}
		extraLower := strings.ToLower(extra)
		col.IsAutoIncrement = strings.Contains(extraLower, "auto_increment")
		col.IsAutoRandom = strings.Contains(extraLower, "auto_random")
		col.IsGenerated = strings.Contains(extraLower, "generated")
		if strings.EqualFold(col.DataType, "enum") {
			values, err := parseEnumValues(columnType)
			if err != nil {
				slog.Default().Warn("failed to parse enum values", slog.String("column", col.Name), slog.String("type", columnType), slog.String("error", err.Error()))
			} else {
				col.EnumValues = values
			}
		} else if strings.EqualFold(col.DataType, "set") {
			values, err := parseSetValues(columnType)
			if err != nil {
				slog.Default().Warn("failed to parse set values", slog.String("column", col.Name), slog.String("type", columnType), slog.String("error", err.Error()))
			} else {
				col.EnumValues = values
			}
		}
		columns = append(columns, col)
	}

	if err := rows.Err(); err != nil {
		recordSpanError(span, err)
		return nil, err
	}
	return columns, nil
}

func applyAutoRandomColumns(ctx context.Context, db Queryer, tableName string, columns []Column) []Column {
	for _, col := range columns {
		if col.IsAutoRandom {
			return columns
		}
	}

	createSQL, err := getCreateTableSQL(ctx, db, tableName)
	if err != nil {
		slog.Default().Warn("failed to load create table statement", slog.String("table", tableName), slog.String("error", err.Error()))
		return columns
	}

	autoCols := extractAutoRandomColumns(createSQL)
	if len(autoCols) == 0 {
		return columns
	}

	for i := range columns {
		if autoCols[columns[i].Name] {
			columns[i].IsAutoRandom = true
		}
	}
	return columns
}

func getCreateTableSQL(ctx context.Context, db Queryer, tableName string) (string, error) {
	query := fmt.Sprintf("SHOW CREATE TABLE `%s`", tableName)
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return "", err
	}
	defer func() {
		_ = rows.Close()
	}()

	var createSQL string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name, &createSQL); err != nil {
			return "", err
		}
		break
	}
	if err := rows.Err(); err != nil {
		return "", err
	}
	if createSQL == "" {
		return "", fmt.Errorf("empty create table statement for %s", tableName)
	}
	return createSQL, nil
}

func extractAutoRandomColumns(createSQL string) map[string]bool {
	autoCols := make(map[string]bool)
	for _, line := range strings.Split(createSQL, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		if !strings.HasPrefix(line, "`") {
			continue
		}
		lower := strings.ToLower(line)
		if !strings.Contains(lower, "/*t![auto_rand]") || !strings.Contains(lower, "auto_random") {
			continue
		}
		end := strings.Index(line[1:], "`")
		if end == -1 {
			continue
		}
		colName := line[1 : 1+end]
		autoCols[colName] = true
	}
	return autoCols
}

func getPrimaryKeys(ctx context.Context, db Queryer, databaseName, tableName string) ([]string, error) {
	ctx, span := startSpan(ctx, "introspection.get_primary_keys",
		attribute.String("db.name", databaseName),
		attribute.String("db.table", tableName),
	)
	defer span.End()

	query := `
		SELECT COLUMN_NAME
		FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
		WHERE TABLE_SCHEMA = ?
		AND TABLE_NAME = ?
		AND CONSTRAINT_NAME = 'PRIMARY'
		ORDER BY ORDINAL_POSITION
	`

	rows, err := db.QueryContext(ctx, query, databaseName, tableName)
	if err != nil {
		recordSpanError(span, err)
		return nil, err
	}
	defer func() {
		_ = rows.Close()
	}()

	var primaryKeys []string
	for rows.Next() {
		var columnName string
		if err := rows.Scan(&columnName); err != nil {
			recordSpanError(span, err)
			return nil, err
		}
		primaryKeys = append(primaryKeys, columnName)
	}

	if err := rows.Err(); err != nil {
		recordSpanError(span, err)
		return nil, err
	}
	return primaryKeys, nil
}

func getForeignKeys(ctx context.Context, db Queryer, databaseName, tableName string) ([]ForeignKey, error) {
	ctx, span := startSpan(ctx, "introspection.get_foreign_keys",
		attribute.String("db.name", databaseName),
		attribute.String("db.table", tableName),
	)
	defer span.End()

	query := `
		SELECT
			COLUMN_NAME,
			REFERENCED_TABLE_NAME,
			REFERENCED_COLUMN_NAME,
			CONSTRAINT_NAME
		FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
		WHERE TABLE_SCHEMA = ?
			AND TABLE_NAME = ?
			AND REFERENCED_TABLE_NAME IS NOT NULL
		ORDER BY ORDINAL_POSITION
	`

	rows, err := db.QueryContext(ctx, query, databaseName, tableName)
	if err != nil {
		recordSpanError(span, err)
		return nil, err
	}
	defer func() {
		_ = rows.Close()
	}()

	var foreignKeys []ForeignKey
	for rows.Next() {
		var fk ForeignKey
		if err := rows.Scan(&fk.ColumnName, &fk.ReferencedTable,
			&fk.ReferencedColumn, &fk.ConstraintName); err != nil {
			recordSpanError(span, err)
			return nil, err
		}
		foreignKeys = append(foreignKeys, fk)
	}

	if err := rows.Err(); err != nil {
		recordSpanError(span, err)
		return nil, err
	}
	return foreignKeys, nil
}

func getIndexes(ctx context.Context, db Queryer, databaseName, tableName string) ([]Index, error) {
	ctx, span := startSpan(ctx, "introspection.get_indexes",
		attribute.String("db.name", databaseName),
		attribute.String("db.table", tableName),
	)
	defer span.End()

	query := `
		SELECT
			INDEX_NAME,
			NON_UNIQUE,
			SEQ_IN_INDEX,
			COLUMN_NAME
		FROM INFORMATION_SCHEMA.STATISTICS
		WHERE TABLE_SCHEMA = ?
			AND TABLE_NAME = ?
		ORDER BY INDEX_NAME, SEQ_IN_INDEX
	`

	rows, err := db.QueryContext(ctx, query, databaseName, tableName)
	if err != nil {
		recordSpanError(span, err)
		return nil, err
	}
	defer func() {
		_ = rows.Close()
	}()

	indexByName := make(map[string]*Index)
	for rows.Next() {
		var indexName string
		var nonUnique int
		var seq int
		var columnName string
		if err := rows.Scan(&indexName, &nonUnique, &seq, &columnName); err != nil {
			recordSpanError(span, err)
			return nil, err
		}

		index, ok := indexByName[indexName]
		if !ok {
			index = &Index{
				Name:   indexName,
				Unique: nonUnique == 0,
			}
			indexByName[indexName] = index
		}
		index.Columns = append(index.Columns, columnName)
	}

	indexes := make([]Index, 0, len(indexByName))
	for _, index := range indexByName {
		indexes = append(indexes, *index)
	}

	if err := rows.Err(); err != nil {
		recordSpanError(span, err)
		return nil, err
	}
	return indexes, nil
}

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

	// Count FKs per (source_table, target_table) pair to determine naming strategy
	// When multiple FKs from the same table point to the same target, we need
	// to use FK column names to disambiguate
	fkCount := make(map[string]map[string]int) // source → target → count
	for _, table := range schema.Tables {
		if table.IsView {
			continue
		}
		for _, fk := range table.ForeignKeys {
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

		for _, fk := range table.ForeignKeys {
			fieldName := ""
			if jType == JunctionTypeAttribute {
				fieldName = namer.JunctionEdgeRefFieldName(fk.ReferencedTable)
			} else {
				fieldName = namer.ManyToOneFieldName(fk.ColumnName)
			}
			rel := Relationship{
				IsManyToOne:      true,
				LocalColumn:      fk.ColumnName,
				RemoteTable:      fk.ReferencedTable,
				RemoteColumn:     fk.ReferencedColumn,
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

			for _, fk := range otherTable.ForeignKeys {
				if fk.ReferencedTable == table.Name {
					isOnlyFK := fkCount[otherTable.Name][table.Name] == 1
					rel := Relationship{
						IsOneToMany:      true,
						LocalColumn:      fk.ReferencedColumn, // Usually PK
						RemoteTable:      otherTable.Name,
						RemoteColumn:     fk.ColumnName,
						GraphQLFieldName: namer.OneToManyFieldName(otherTable.Name, fk.ColumnName, isOnlyFK),
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

		// Get primary key columns for both tables
		leftPK := getPKColumn(leftTable)
		rightPK := getPKColumn(rightTable)
		if leftPK == "" || rightPK == "" {
			continue
		}

		switch jc.Type {
		case JunctionTypePure:
			// Pure junction: create direct M2M fields, hide junction table
			// Add M2M from left to right
			leftFieldName := namer.JunctionFieldName(jc.Table, leftTable.Name, rightTable.Name, rightTable.Name, false)
			rightFieldName := namer.JunctionFieldName(jc.Table, leftTable.Name, rightTable.Name, leftTable.Name, false)
			leftTable.Relationships = append(leftTable.Relationships, Relationship{
				IsManyToMany:     true,
				LocalColumn:      leftPK,
				RemoteTable:      rightTable.Name,
				RemoteColumn:     rightPK,
				JunctionTable:    jc.Table,
				JunctionLocalFK:  jc.LeftFK.ColumnName,
				JunctionRemoteFK: jc.RightFK.ColumnName,
				GraphQLFieldName: leftFieldName,
			})
			// Add M2M from right to left
			rightTable.Relationships = append(rightTable.Relationships, Relationship{
				IsManyToMany:     true,
				LocalColumn:      rightPK,
				RemoteTable:      leftTable.Name,
				RemoteColumn:     leftPK,
				JunctionTable:    jc.Table,
				JunctionLocalFK:  jc.RightFK.ColumnName,
				JunctionRemoteFK: jc.LeftFK.ColumnName,
				GraphQLFieldName: rightFieldName,
			})

		case JunctionTypeAttribute:
			// Attribute junction: create edge list fields
			edgeFieldName := namer.JunctionFieldName(jc.Table, leftTable.Name, rightTable.Name, "", true)

			// Add edge list from left to junction
			leftTable.Relationships = append(leftTable.Relationships, Relationship{
				IsEdgeList:       true,
				LocalColumn:      leftPK,
				RemoteTable:      jc.Table, // Points to junction table for edge type
				RemoteColumn:     jc.LeftFK.ColumnName,
				JunctionTable:    jc.Table,
				JunctionLocalFK:  jc.LeftFK.ColumnName,
				JunctionRemoteFK: jc.RightFK.ColumnName,
				GraphQLFieldName: edgeFieldName,
			})
			// Add edge list from right to junction
			rightTable.Relationships = append(rightTable.Relationships, Relationship{
				IsEdgeList:       true,
				LocalColumn:      rightPK,
				RemoteTable:      jc.Table, // Points to junction table for edge type
				RemoteColumn:     jc.RightFK.ColumnName,
				JunctionTable:    jc.Table,
				JunctionLocalFK:  jc.RightFK.ColumnName,
				JunctionRemoteFK: jc.LeftFK.ColumnName,
				GraphQLFieldName: edgeFieldName,
			})
		}
	}

	return nil
}

// getPKColumn returns the first primary key column name for a table, or empty string if none.
func getPKColumn(table *Table) string {
	for _, col := range table.Columns {
		if col.IsPrimaryKey {
			return col.Name
		}
	}
	return ""
}

// NumericColumns returns columns eligible for AVG/SUM aggregation (Int, Float types).
func NumericColumns(table Table) []Column {
	var cols []Column
	for _, col := range table.Columns {
		if EffectiveGraphQLType(col).IsNumeric() {
			cols = append(cols, col)
		}
	}
	return cols
}

// ComparableColumns returns columns eligible for MIN/MAX aggregation (all except JSON).
func ComparableColumns(table Table) []Column {
	var cols []Column
	for _, col := range table.Columns {
		if EffectiveGraphQLType(col).IsComparable() {
			cols = append(cols, col)
		}
	}
	return cols
}

func startSpan(ctx context.Context, name string, attrs ...attribute.KeyValue) (context.Context, trace.Span) {
	tracer := otel.Tracer("tidb-graphql/introspection")
	ctx, span := tracer.Start(ctx, name)
	if len(attrs) > 0 {
		span.SetAttributes(attrs...)
	}
	return ctx, span
}

func recordSpanError(span trace.Span, err error) {
	if err == nil {
		return
	}
	span.RecordError(err)
	span.SetStatus(codes.Error, err.Error())
}
