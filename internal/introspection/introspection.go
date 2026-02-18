// Package introspection discovers database schema metadata from TiDB's information_schema.
// It extracts tables, columns, indexes, foreign keys, and relationships for use in GraphQL schema generation.
package introspection

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"strconv"
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
	VectorDimension int
	IsNullable      bool
	IsPrimaryKey    bool
	IsGenerated     bool
	IsAutoIncrement bool
	IsAutoRandom    bool
	HasDefault      bool
	ColumnDefault   string
	// GenerationExpression stores INFORMATION_SCHEMA.COLUMNS.GENERATION_EXPRESSION.
	GenerationExpression string
	EnumValues           []string
	Comment              string
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
	Type    string
	Columns []string
	// IsVectorSearchCapable marks indexes confirmed as vector indexes via
	// INFORMATION_SCHEMA.TIFLASH_INDEXES (INDEX_KIND='Vector').
	//
	// This exists because some TiDB deployments (including Cloud Zero) can report
	// vector indexes as INDEX_TYPE='BTREE' in INFORMATION_SCHEMA.STATISTICS.
	// In those environments, STATISTICS alone cannot reliably identify vector
	// search capability.
	IsVectorSearchCapable bool
}

// ForeignKey represents a foreign key constraint on a column
type ForeignKey struct {
	ColumnName       string // e.g., "volume_id"
	ReferencedTable  string // e.g., "volumes"
	ReferencedColumn string // e.g., "id"
	ConstraintName   string // e.g., "books_ibfk_1"
	OrdinalPosition  int    // Column position within the FK constraint
}

// Relationship represents either direction of a FK relationship
type Relationship struct {
	IsManyToOne  bool
	IsOneToMany  bool
	IsManyToMany bool // Direct M2M through pure junction (junction hidden)
	IsEdgeList   bool // M2M through attribute junction (edge type visible)
	// LocalColumns/RemoteColumns are ordered positional mappings between local and remote keys.
	// Single-column compatibility fields are retained and populated from index 0.
	LocalColumns  []string // For many-to-one: FK columns; for one-to-many: referenced key columns on local table
	RemoteTable   string   // The related table name (for M2M: the other entity table, not junction)
	RemoteColumns []string // For many-to-one: referenced columns; for one-to-many: FK columns in remote table
	// Deprecated compatibility fields for single-column code paths.
	LocalColumn  string
	RemoteColumn string
	// Junction key mappings are positional: JunctionLocalFKColumns[i] joins to LocalColumns[i],
	// JunctionRemoteFKColumns[i] joins to RemoteColumns[i].
	JunctionTable           string   // For M2M relationships: the intermediate junction table name
	JunctionLocalFKColumns  []string // For M2M/Edge: FK columns in junction pointing to this table
	JunctionRemoteFKColumns []string // For M2M/Edge: FK columns in junction pointing to remote table
	// Deprecated compatibility fields for single-column code paths.
	JunctionLocalFK  string
	JunctionRemoteFK string
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
	ConstraintName    string   // FK constraint name
	ColumnNames       []string // FK columns in junction table (ordered)
	ReferencedTable   string   // Target table
	ReferencedColumns []string // Target columns (ordered)
	// Deprecated compatibility fields for single-column paths.
	ColumnName       string
	ReferencedColumn string
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
			EXTRA,
			GENERATION_EXPRESSION
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
		var generationExpression sql.NullString
		if err := rows.Scan(&col.Name, &col.DataType, &columnType, &columnComment, &isNullable, &columnDefault, &extra, &generationExpression); err != nil {
			recordSpanError(span, err)
			return nil, err
		}
		col.ColumnType = columnType
		col.VectorDimension = parseVectorDimension(columnType)
		if columnComment.Valid {
			col.Comment = strings.TrimSpace(columnComment.String)
		}
		col.IsNullable = strings.ToUpper(isNullable) == "YES"
		if columnDefault.Valid {
			col.ColumnDefault = columnDefault.String
			col.HasDefault = true
		}
		if generationExpression.Valid {
			col.GenerationExpression = strings.TrimSpace(generationExpression.String)
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
			CONSTRAINT_NAME,
			ORDINAL_POSITION
		FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
		WHERE TABLE_SCHEMA = ?
			AND TABLE_NAME = ?
			AND REFERENCED_TABLE_NAME IS NOT NULL
		ORDER BY CONSTRAINT_NAME, ORDINAL_POSITION
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
			&fk.ReferencedColumn, &fk.ConstraintName, &fk.OrdinalPosition); err != nil {
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
			COLUMN_NAME,
			INDEX_TYPE
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
		var indexType string
		if err := rows.Scan(&indexName, &nonUnique, &seq, &columnName, &indexType); err != nil {
			recordSpanError(span, err)
			return nil, err
		}

		index, ok := indexByName[indexName]
		if !ok {
			index = &Index{
				Name:   indexName,
				Unique: nonUnique == 0,
				Type:   strings.ToUpper(strings.TrimSpace(indexType)),
			}
			indexByName[indexName] = index
		}
		index.Columns = append(index.Columns, columnName)
	}
	if err := rows.Err(); err != nil {
		recordSpanError(span, err)
		return nil, err
	}

	vectorIndexNames, err := getVectorSearchIndexNames(ctx, db, databaseName, tableName)
	if err != nil {
		// Best-effort enrichment only. Keep introspection working even when
		// TIFLASH_INDEXES is unavailable or restricted.
		slog.Default().Warn(
			"failed to load vector index metadata from TIFLASH_INDEXES; falling back to STATISTICS index type detection",
			slog.String("table", tableName),
			slog.String("error", err.Error()),
		)
	} else {
		// TiDB Cloud Zero can expose vector indexes in SHOW CREATE TABLE and
		// TIFLASH_INDEXES while STATISTICS still reports INDEX_TYPE='BTREE'.
		// We therefore treat TIFLASH_INDEXES(INDEX_KIND='Vector') as the source
		// of truth for vector index kind, and keep STATISTICS for index-column
		// mapping.
		for name := range vectorIndexNames {
			if idx, ok := indexByName[name]; ok {
				idx.IsVectorSearchCapable = true
			}
		}
	}

	indexes := make([]Index, 0, len(indexByName))
	for _, index := range indexByName {
		indexes = append(indexes, *index)
	}

	return indexes, nil
}

func getVectorSearchIndexNames(ctx context.Context, db Queryer, databaseName, tableName string) (map[string]struct{}, error) {
	ctx, span := startSpan(ctx, "introspection.get_vector_indexes",
		attribute.String("db.name", databaseName),
		attribute.String("db.table", tableName),
	)
	defer span.End()

	query := `
		SELECT DISTINCT INDEX_NAME
		FROM INFORMATION_SCHEMA.TIFLASH_INDEXES
		WHERE TIDB_DATABASE = ?
			AND TIDB_TABLE = ?
			AND UPPER(INDEX_KIND) = 'VECTOR'
	`

	rows, err := db.QueryContext(ctx, query, databaseName, tableName)
	if err != nil {
		recordSpanError(span, err)
		return nil, err
	}
	defer func() {
		_ = rows.Close()
	}()

	result := make(map[string]struct{})
	for rows.Next() {
		var indexName string
		if err := rows.Scan(&indexName); err != nil {
			recordSpanError(span, err)
			return nil, err
		}
		result[indexName] = struct{}{}
	}

	if err := rows.Err(); err != nil {
		recordSpanError(span, err)
		return nil, err
	}
	return result, nil
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
				LocalColumn:      firstColumnOrEmpty(localColumns),
				RemoteColumn:     firstColumnOrEmpty(remoteColumns),
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
						LocalColumn:      fk.ReferencedColumns[0], // compatibility
						RemoteColumn:     fk.ColumnNames[0],       // compatibility
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
				LocalColumn:             firstColumnOrEmpty(leftPKNames),
				RemoteColumn:            firstColumnOrEmpty(rightPKNames),
				JunctionTable:           jc.Table,
				JunctionLocalFKColumns:  append([]string(nil), leftJunctionCols...),
				JunctionRemoteFKColumns: append([]string(nil), rightJunctionCols...),
				JunctionLocalFK:         firstColumnOrEmpty(leftJunctionCols),
				JunctionRemoteFK:        firstColumnOrEmpty(rightJunctionCols),
				GraphQLFieldName:        leftFieldName,
			})
			// Add M2M from right to left
			rightTable.Relationships = append(rightTable.Relationships, Relationship{
				IsManyToMany:            true,
				LocalColumns:            append([]string(nil), rightPKNames...),
				RemoteTable:             leftTable.Name,
				RemoteColumns:           append([]string(nil), leftPKNames...),
				LocalColumn:             firstColumnOrEmpty(rightPKNames),
				RemoteColumn:            firstColumnOrEmpty(leftPKNames),
				JunctionTable:           jc.Table,
				JunctionLocalFKColumns:  append([]string(nil), rightJunctionCols...),
				JunctionRemoteFKColumns: append([]string(nil), leftJunctionCols...),
				JunctionLocalFK:         firstColumnOrEmpty(rightJunctionCols),
				JunctionRemoteFK:        firstColumnOrEmpty(leftJunctionCols),
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
				LocalColumn:             firstColumnOrEmpty(leftPKNames),
				RemoteColumn:            firstColumnOrEmpty(leftJunctionCols),
				JunctionTable:           jc.Table,
				JunctionLocalFKColumns:  append([]string(nil), leftJunctionCols...),
				JunctionRemoteFKColumns: append([]string(nil), rightJunctionCols...),
				JunctionLocalFK:         firstColumnOrEmpty(leftJunctionCols),
				JunctionRemoteFK:        firstColumnOrEmpty(rightJunctionCols),
				GraphQLFieldName:        edgeFieldName,
			})
			// Add edge list from right to junction
			rightTable.Relationships = append(rightTable.Relationships, Relationship{
				IsEdgeList:              true,
				LocalColumns:            append([]string(nil), rightPKNames...),
				RemoteTable:             jc.Table, // Points to junction table for edge type
				RemoteColumns:           append([]string(nil), rightJunctionCols...),
				LocalColumn:             firstColumnOrEmpty(rightPKNames),
				RemoteColumn:            firstColumnOrEmpty(rightJunctionCols),
				JunctionTable:           jc.Table,
				JunctionLocalFKColumns:  append([]string(nil), rightJunctionCols...),
				JunctionRemoteFKColumns: append([]string(nil), leftJunctionCols...),
				JunctionLocalFK:         firstColumnOrEmpty(rightJunctionCols),
				JunctionRemoteFK:        firstColumnOrEmpty(leftJunctionCols),
				GraphQLFieldName:        edgeFieldName,
			})
		}
	}

	return nil
}

func parseVectorDimension(columnType string) int {
	normalized := strings.ToLower(strings.TrimSpace(columnType))
	if normalized == "" || normalized == "vector" {
		return 0
	}
	if !strings.HasPrefix(normalized, "vector(") || !strings.HasSuffix(normalized, ")") {
		return 0
	}
	raw := strings.TrimSpace(normalized[len("vector(") : len(normalized)-1])
	if raw == "" {
		return 0
	}
	dimension, err := strconv.Atoi(raw)
	if err != nil || dimension <= 0 {
		return 0
	}
	return dimension
}

// IsVectorColumn reports whether the column uses TiDB's VECTOR type.
func IsVectorColumn(col Column) bool {
	return EffectiveGraphQLType(col) == sqltype.TypeVector
}

// IsAutoEmbeddingVectorColumn reports whether a vector column is generated via
// EMBED_TEXT(...), indicating TiDB auto-embedding behavior.
func IsAutoEmbeddingVectorColumn(col Column) bool {
	if !IsVectorColumn(col) {
		return false
	}
	expr := strings.ToLower(strings.TrimSpace(col.GenerationExpression))
	if expr == "" {
		return false
	}
	return strings.Contains(expr, "embed_text(")
}

// VectorColumns returns vector-typed columns in table column order.
func VectorColumns(table Table) []Column {
	cols := make([]Column, 0)
	for _, col := range table.Columns {
		if IsVectorColumn(col) {
			cols = append(cols, col)
		}
	}
	return cols
}

// HasVectorIndexForColumn reports whether a vector-search-capable index exists
// for the given table column.
func HasVectorIndexForColumn(table Table, columnName string) bool {
	for _, idx := range table.Indexes {
		if !isVectorSearchIndex(idx) {
			continue
		}
		for _, idxCol := range idx.Columns {
			if idxCol == columnName {
				return true
			}
		}
	}
	return false
}

func isVectorSearchIndex(idx Index) bool {
	if idx.IsVectorSearchCapable {
		return true
	}
	// Backward-compatible fallback for environments that expose vector search
	// index kind via STATISTICS.INDEX_TYPE (e.g. HNSW).
	indexType := strings.ToUpper(strings.TrimSpace(idx.Type))
	return strings.Contains(indexType, "HNSW")
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
