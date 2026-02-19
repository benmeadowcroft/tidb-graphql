package resolver

import (
	"fmt"

	"tidb-graphql/internal/introspection"
	"tidb-graphql/internal/naming"
	"tidb-graphql/internal/schemanaming"
	"tidb-graphql/internal/sqltype"

	"github.com/graphql-go/graphql"
)

func (r *Resolver) applyNaming() {
	if r.dbSchema == nil {
		return
	}
	namingConfig := r.singularNamer.Config()
	schemanaming.Apply(r.dbSchema, naming.New(namingConfig, nil))
}

func (r *Resolver) singularQueryName(table introspection.Table) string {
	key := table.Name
	r.mu.RLock()
	if cached, ok := r.singularQueryCache[key]; ok {
		r.mu.RUnlock()
		return cached
	}
	r.mu.RUnlock()

	name := introspection.GraphQLSingleQueryName(table)

	r.mu.Lock()
	if cached, ok := r.singularQueryCache[key]; ok {
		r.mu.Unlock()
		return cached
	}
	r.singularQueryCache[key] = name
	r.mu.Unlock()

	return name
}

func (r *Resolver) singularTypeName(table introspection.Table) string {
	key := table.Name
	r.mu.RLock()
	if cached, ok := r.singularTypeCache[key]; ok {
		r.mu.RUnlock()
		return cached
	}
	r.mu.RUnlock()

	name := introspection.GraphQLSingleTypeName(table)

	r.mu.Lock()
	if cached, ok := r.singularTypeCache[key]; ok {
		r.mu.Unlock()
		return cached
	}
	r.singularTypeCache[key] = name
	r.mu.Unlock()

	return name
}

func (r *Resolver) addTableQueries(fields graphql.Fields, table introspection.Table) graphql.Fields {
	if r.dbSchema != nil {
		if jc, ok := r.dbSchema.Junctions[table.Name]; ok && jc.Type == introspection.JunctionTypePure {
			return fields
		}
	}

	// Create the GraphQL type for this table
	tableType := r.buildGraphQLType(table)

	fieldName := introspection.GraphQLQueryName(table)

	// Primary key query (supports both single and composite primary keys)
	// Uses singular name (e.g., "user" not "user_by_pk") for cleaner API
	pkCols := introspection.PrimaryKeyColumns(table)
	if len(pkCols) > 0 {
		pkFieldName := r.singularQueryName(table)
		r.addPrimaryKeyQuery(fields, table, tableType, pkFieldName, pkCols)
		r.addPrimaryKeyUniqueLookup(fields, table, tableType, pkFieldName, pkCols)
	}

	// Unique key queries
	r.addUniqueKeyQueries(fields, table, tableType, r.singularQueryName(table))

	// Root collection query (connection shape, only for tables with primary keys).
	if len(pkCols) > 0 {
		connectionType := r.buildConnectionType(table, tableType)
		fields[fieldName] = &graphql.Field{
			Type:    graphql.NewNonNull(connectionType),
			Args:    r.connectionFieldArgs(table),
			Resolve: r.makeConnectionResolver(table),
		}
	}

	r.addVectorSearchQueries(fields, table, tableType, pkCols)

	return fields
}

// addSingleRowQuery adds a query field that returns a single row based on the given columns.
// Used for primary key and unique key lookups.
func (r *Resolver) addSingleRowQuery(fields graphql.Fields, table introspection.Table, tableType *graphql.Object, queryName string, cols []introspection.Column) {
	args := graphql.FieldConfigArgument{}
	for i := range cols {
		col := &cols[i]
		argName := introspection.GraphQLFieldName(*col)
		argType := r.mapColumnTypeToGraphQL(table, col)
		args[argName] = &graphql.ArgumentConfig{
			Type: graphql.NewNonNull(argType),
		}
	}

	fields[queryName] = &graphql.Field{
		Type:    tableType,
		Args:    args,
		Resolve: r.makeSingleRowResolver(table),
	}
}

// addPrimaryKeyQuery adds a query field that returns a single row by global node ID.
func (r *Resolver) addPrimaryKeyQuery(fields graphql.Fields, table introspection.Table, tableType *graphql.Object, queryName string, pkCols []introspection.Column) {
	fields[queryName] = &graphql.Field{
		Type: tableType,
		Args: graphql.FieldConfigArgument{
			"id": &graphql.ArgumentConfig{
				Type: graphql.NewNonNull(graphql.ID),
			},
		},
		Resolve: r.makePrimaryKeyResolver(table, pkCols),
	}
}

// addPrimaryKeyUniqueLookup adds a unique-key style lookup for raw primary key fields.
func (r *Resolver) addPrimaryKeyUniqueLookup(fields graphql.Fields, table introspection.Table, tableType *graphql.Object, baseName string, pkCols []introspection.Column) {
	if len(pkCols) == 0 {
		return
	}
	queryName := baseName + "_by"
	for _, col := range pkCols {
		queryName += "_" + introspection.GraphQLFieldName(col)
	}
	r.addSingleRowQuery(fields, table, tableType, queryName, pkCols)
}

// addUniqueKeyQueries adds resolver fields for unique index lookups
func (r *Resolver) addUniqueKeyQueries(fields graphql.Fields, table introspection.Table, tableType *graphql.Object, fieldName string) {
	// Get column map for type lookup
	colMap := make(map[string]*introspection.Column)
	for i := range table.Columns {
		colMap[table.Columns[i].Name] = &table.Columns[i]
	}

	// Iterate through all unique indexes
	for _, idx := range table.Indexes {
		if !idx.Unique || idx.Name == "PRIMARY" {
			continue
		}

		// Collect columns for this index
		var cols []introspection.Column
		queryName := fieldName + "_by"
		for _, colName := range idx.Columns {
			col, exists := colMap[colName]
			if !exists {
				continue
			}
			cols = append(cols, *col)
			queryName += "_" + introspection.GraphQLFieldName(*col)
		}

		if len(cols) > 0 {
			r.addSingleRowQuery(fields, table, tableType, queryName, cols)
		}
	}
}

func (r *Resolver) addVectorSearchQueries(fields graphql.Fields, table introspection.Table, tableType *graphql.Object, pkCols []introspection.Column) {
	if len(pkCols) == 0 {
		return
	}
	vectorColumns := introspection.VectorColumns(table)
	if len(vectorColumns) == 0 {
		return
	}

	r.mu.RLock()
	cfg := r.vectorSearch
	r.mu.RUnlock()

	for _, vectorCol := range vectorColumns {
		if cfg.RequireIndex && !introspection.HasVectorIndexForColumn(table, vectorCol.Name) {
			continue
		}

		fieldName := uniqueRootFieldName(fields, r.vectorSearchFieldName(table, vectorCol))
		connType := r.buildVectorConnectionType(table, vectorCol, tableType)

		args := graphql.FieldConfigArgument{
			"metric": &graphql.ArgumentConfig{
				Type: r.vectorDistanceMetricEnum(),
			},
			"first": &graphql.ArgumentConfig{
				Type: r.nonNegativeIntScalar(),
			},
			"after": &graphql.ArgumentConfig{
				Type: graphql.String,
			},
		}
		if introspection.IsAutoEmbeddingVectorColumn(vectorCol) {
			args["vector"] = &graphql.ArgumentConfig{
				Type: r.vectorScalar(),
			}
			args["queryText"] = &graphql.ArgumentConfig{
				Type: graphql.String,
			}
		} else {
			args["vector"] = &graphql.ArgumentConfig{
				Type: graphql.NewNonNull(r.vectorScalar()),
			}
		}
		if whereInput := r.whereInput(table); whereInput != nil {
			args["where"] = &graphql.ArgumentConfig{
				Type: whereInput,
			}
		}

		fields[fieldName] = &graphql.Field{
			Type:    graphql.NewNonNull(connType),
			Args:    args,
			Resolve: r.makeVectorConnectionResolver(table, vectorCol),
		}
	}
}

func (r *Resolver) vectorSearchFieldName(table introspection.Table, vectorCol introspection.Column) string {
	tablePart := r.singularNamer.ToGraphQLTypeName(introspection.GraphQLQueryName(table))
	columnPart := r.singularNamer.ToGraphQLTypeName(introspection.GraphQLFieldName(vectorCol))
	return "search" + tablePart + "By" + columnPart + "Vector"
}

func uniqueRootFieldName(fields graphql.Fields, base string) string {
	if _, exists := fields[base]; !exists {
		return base
	}
	for i := 2; ; i++ {
		candidate := fmt.Sprintf("%s%d", base, i)
		if _, exists := fields[candidate]; !exists {
			return candidate
		}
	}
}

func (r *Resolver) buildGraphQLType(table introspection.Table) *graphql.Object {
	typeName := introspection.GraphQLTypeName(table)

	// Check cache first
	r.mu.RLock()
	cached, ok := r.typeCache[typeName]
	r.mu.RUnlock()
	if ok {
		return cached
	}

	interfaces := []*graphql.Interface{}
	if len(introspection.PrimaryKeyColumns(table)) > 0 {
		interfaces = append(interfaces, r.nodeInterfaceType())
	}

	// Create type with FieldsThunk for lazy field initialization
	// This prevents circular reference issues
	objType := graphql.NewObject(graphql.ObjectConfig{
		Name:        typeName,
		Description: table.Comment,
		Fields: graphql.FieldsThunk(func() graphql.Fields {
			return r.buildFieldsForTable(table)
		}),
		Interfaces: interfaces,
	})

	// Cache immediately before building fields (important for circular refs)
	r.mu.Lock()
	if cached, ok := r.typeCache[typeName]; ok {
		r.mu.Unlock()
		return cached
	}
	r.typeCache[typeName] = objType
	r.mu.Unlock()

	return objType
}

// buildFieldsForTable builds the GraphQL fields for a table (called lazily by FieldsThunk)
func (r *Resolver) buildFieldsForTable(table introspection.Table) graphql.Fields {
	fields := graphql.Fields{}

	// Add scalar fields from columns
	for _, col := range table.Columns {
		fieldType := r.mapColumnTypeToGraphQL(table, &col)
		if !col.IsNullable {
			fieldType = graphql.NewNonNull(fieldType)
		}

		field := &graphql.Field{
			Type:        fieldType,
			Description: col.Comment,
		}
		if introspection.EffectiveGraphQLType(col) == sqltype.TypeUUID {
			field.Resolve = r.uuidColumnResolver(col)
		}
		fields[introspection.GraphQLFieldName(col)] = field
	}

	pkCols := introspection.PrimaryKeyColumns(table)
	if len(pkCols) > 0 {
		fields["id"] = &graphql.Field{
			Type:    graphql.NewNonNull(graphql.ID),
			Resolve: r.makeNodeIDResolver(table, pkCols),
		}
	}

	// Add relationship fields
	for _, rel := range table.Relationships {
		if rel.IsManyToOne {
			// Many-to-one: returns single object
			relatedTable, err := r.findTable(rel.RemoteTable)
			if err != nil {
				// Log error but continue - this shouldn't happen if schema was built correctly
				// The error will be caught at query time instead
				continue
			}
			relatedType := r.buildGraphQLType(relatedTable)

			// Keep many-to-one fields nullable even when FK is NOT NULL.
			// Row-level/table-level security can hide the related row; non-null would
			// bubble errors and null out parent objects/lists.
			fields[rel.GraphQLFieldName] = &graphql.Field{
				Type:    relatedType,
				Resolve: r.makeManyToOneResolver(table, rel),
			}
		} else if rel.IsOneToMany {
			// One-to-many: returns connection (only when related table has PK)
			relatedTable, err := r.findTable(rel.RemoteTable)
			if err != nil {
				// Log error but continue - this shouldn't happen if schema was built correctly
				// The error will be caught at query time instead
				continue
			}
			relatedType := r.buildGraphQLType(relatedTable)
			relPKCols := introspection.PrimaryKeyColumns(relatedTable)
			if len(relPKCols) > 0 {
				connType := r.buildConnectionType(relatedTable, relatedType)
				addRelConnectionField(fields, rel.GraphQLFieldName, connType, r.connectionFieldArgs(relatedTable), r.makeOneToManyConnectionResolver(table, rel))
			}
		} else if rel.IsManyToMany {
			// Many-to-many through pure junction: returns connection (only when related table has PK)
			relatedTable, err := r.findTable(rel.RemoteTable)
			if err != nil {
				continue
			}
			relatedType := r.buildGraphQLType(relatedTable)
			relPKCols := introspection.PrimaryKeyColumns(relatedTable)
			if len(relPKCols) > 0 {
				connType := r.buildConnectionType(relatedTable, relatedType)
				addRelConnectionField(fields, rel.GraphQLFieldName, connType, r.connectionFieldArgs(relatedTable), r.makeManyToManyConnectionResolver(table, rel))
			}
		} else if rel.IsEdgeList {
			// Edge list through attribute junction: returns connection (only when junction table has PK)
			junctionTable, err := r.findTable(rel.JunctionTable)
			if err != nil {
				continue
			}
			edgeType := r.buildGraphQLType(junctionTable)
			relPKCols := introspection.PrimaryKeyColumns(junctionTable)
			if len(relPKCols) > 0 {
				connType := r.buildConnectionType(junctionTable, edgeType)
				addRelConnectionField(fields, rel.GraphQLFieldName, connType, r.connectionFieldArgs(junctionTable), r.makeEdgeListConnectionResolver(table, rel))
			}
		}
	}

	return fields
}

func addRelConnectionField(fields graphql.Fields, name string, connType *graphql.Object, args graphql.FieldConfigArgument, resolve graphql.FieldResolveFn) {
	fields[name] = &graphql.Field{
		Type:    graphql.NewNonNull(connType),
		Args:    args,
		Resolve: resolve,
	}
}

func (r *Resolver) connectionFieldArgs(table introspection.Table) graphql.FieldConfigArgument {
	args := graphql.FieldConfigArgument{
		"first": &graphql.ArgumentConfig{
			Type: r.nonNegativeIntScalar(),
		},
		"after": &graphql.ArgumentConfig{
			Type: graphql.String,
		},
		"last": &graphql.ArgumentConfig{
			Type: r.nonNegativeIntScalar(),
		},
		"before": &graphql.ArgumentConfig{
			Type: graphql.String,
		},
	}

	if orderByArgType := r.orderByArgType(table); orderByArgType != nil {
		args["orderBy"] = &graphql.ArgumentConfig{
			Type: orderByArgType,
		}
		args["orderByPolicy"] = &graphql.ArgumentConfig{
			Type: r.orderByPolicyEnum(),
		}
	}
	if whereInput := r.whereInput(table); whereInput != nil {
		args["where"] = &graphql.ArgumentConfig{
			Type: whereInput,
		}
	}
	return args
}

// findTable finds a table by name in the schema
func (r *Resolver) findTable(tableName string) (introspection.Table, error) {
	for _, table := range r.dbSchema.Tables {
		if table.Name == tableName {
			return table, nil
		}
	}
	return introspection.Table{}, fmt.Errorf("table not found: %s", tableName)
}

// buildAggregateFieldsType creates the aggregate fields container.
// Example: EmployeeAggregateFields { count: Int!, avg: EmployeeAvgFields, ... }
func (r *Resolver) buildAggregateFieldsType(table introspection.Table) *graphql.Object {
	typeName := introspection.GraphQLTypeName(table) + "AggregateFields"

	r.mu.RLock()
	if cached, ok := r.aggregateCache[typeName]; ok {
		r.mu.RUnlock()
		return cached
	}
	r.mu.RUnlock()

	fields := graphql.Fields{
		"count": &graphql.Field{
			Type: graphql.NewNonNull(graphql.Int),
		},
	}

	// Add avg/sum fields if table has numeric columns
	numericCols := introspection.NumericColumns(table)
	if len(numericCols) > 0 {
		fields["avg"] = &graphql.Field{
			Type: r.buildNumericAggregateFieldsType(table, "Avg"),
		}
		fields["sum"] = &graphql.Field{
			Type: r.buildNumericAggregateFieldsType(table, "Sum"),
		}
	}

	// Add min/max fields for comparable columns
	comparableCols := introspection.ComparableColumns(table)
	if len(comparableCols) > 0 {
		fields["countDistinct"] = &graphql.Field{
			Type: r.buildCountDistinctFieldsType(table),
		}
		fields["min"] = &graphql.Field{
			Type: r.buildComparableAggregateFieldsType(table, "Min"),
		}
		fields["max"] = &graphql.Field{
			Type: r.buildComparableAggregateFieldsType(table, "Max"),
		}
	}

	objType := graphql.NewObject(graphql.ObjectConfig{
		Name:   typeName,
		Fields: fields,
	})

	r.mu.Lock()
	if cached, ok := r.aggregateCache[typeName]; ok {
		r.mu.Unlock()
		return cached
	}
	r.aggregateCache[typeName] = objType
	r.mu.Unlock()

	return objType
}

// buildNumericAggregateFieldsType creates fields for avg/sum operations.
// Example: EmployeeAvgFields { salary: Float, age: Float }
func (r *Resolver) buildNumericAggregateFieldsType(table introspection.Table, suffix string) *graphql.Object {
	typeName := introspection.GraphQLTypeName(table) + suffix + "Fields"

	r.mu.RLock()
	if cached, ok := r.aggregateCache[typeName]; ok {
		r.mu.RUnlock()
		return cached
	}
	r.mu.RUnlock()

	fields := graphql.Fields{}
	for _, col := range introspection.NumericColumns(table) {
		fieldName := introspection.GraphQLFieldName(col)
		fields[fieldName] = &graphql.Field{
			Type: graphql.Float, // AVG/SUM always returns Float
		}
	}

	objType := graphql.NewObject(graphql.ObjectConfig{
		Name:   typeName,
		Fields: fields,
	})

	r.mu.Lock()
	if cached, ok := r.aggregateCache[typeName]; ok {
		r.mu.Unlock()
		return cached
	}
	r.aggregateCache[typeName] = objType
	r.mu.Unlock()

	return objType
}

// buildComparableAggregateFieldsType creates fields for min/max operations.
// Example: EmployeeMinFields { salary: Float, name: String, hireDate: String }
func (r *Resolver) buildComparableAggregateFieldsType(table introspection.Table, suffix string) *graphql.Object {
	typeName := introspection.GraphQLTypeName(table) + suffix + "Fields"

	r.mu.RLock()
	if cached, ok := r.aggregateCache[typeName]; ok {
		r.mu.RUnlock()
		return cached
	}
	r.mu.RUnlock()

	fields := graphql.Fields{}
	for _, col := range introspection.ComparableColumns(table) {
		fieldName := introspection.GraphQLFieldName(col)
		// MIN/MAX preserve the original column type
		fields[fieldName] = &graphql.Field{
			Type: r.mapColumnTypeToGraphQL(table, &col),
		}
	}

	objType := graphql.NewObject(graphql.ObjectConfig{
		Name:   typeName,
		Fields: fields,
	})

	r.mu.Lock()
	if cached, ok := r.aggregateCache[typeName]; ok {
		r.mu.Unlock()
		return cached
	}
	r.aggregateCache[typeName] = objType
	r.mu.Unlock()

	return objType
}

func (r *Resolver) buildCountDistinctFieldsType(table introspection.Table) *graphql.Object {
	typeName := introspection.GraphQLTypeName(table) + "CountDistinctFields"

	r.mu.RLock()
	if cached, ok := r.aggregateCache[typeName]; ok {
		r.mu.RUnlock()
		return cached
	}
	r.mu.RUnlock()

	fields := graphql.Fields{}
	for _, col := range introspection.ComparableColumns(table) {
		fieldName := introspection.GraphQLFieldName(col)
		fields[fieldName] = &graphql.Field{
			Type: graphql.NewNonNull(graphql.Int),
		}
	}

	objType := graphql.NewObject(graphql.ObjectConfig{
		Name:   typeName,
		Fields: fields,
	})

	r.mu.Lock()
	if cached, ok := r.aggregateCache[typeName]; ok {
		r.mu.Unlock()
		return cached
	}
	r.aggregateCache[typeName] = objType
	r.mu.Unlock()

	return objType
}
