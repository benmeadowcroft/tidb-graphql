// Package resolver builds and executes GraphQL schemas from database introspection.
// It dynamically generates GraphQL types, queries, and resolvers based on the database schema,
// supporting filtering, ordering, pagination, and relationship resolution with N+1 query prevention.
package resolver

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"tidb-graphql/internal/dbexec"
	"tidb-graphql/internal/introspection"
	"tidb-graphql/internal/naming"
	"tidb-graphql/internal/nodeid"
	"tidb-graphql/internal/observability"
	"tidb-graphql/internal/schemanaming"
	"tidb-graphql/internal/planner"
	"tidb-graphql/internal/schemafilter"
	"tidb-graphql/internal/sqltype"

	"github.com/go-sql-driver/mysql"
	"github.com/graphql-go/graphql"
	"github.com/graphql-go/graphql/language/ast"
)

// Resolver handles GraphQL query execution against a database.
// It maintains caches for GraphQL types and input objects to avoid redundant construction.
type Resolver struct {
	executor           dbexec.QueryExecutor
	dbSchema           *introspection.Schema
	typeCache          map[string]*graphql.Object
	orderByCache       map[string]*graphql.InputObject
	whereCache         map[string]*graphql.InputObject
	filterCache        map[string]*graphql.InputObject
	aggregateCache     map[string]*graphql.Object // Cache for aggregate types (XxxAggregate, XxxAvgFields, etc.)
	createInputCache   map[string]*graphql.InputObject
	updateInputCache   map[string]*graphql.InputObject
	deletePayloadCache map[string]*graphql.Object
	enumCache          map[string]*graphql.Enum
	enumFilterCache    map[string]*graphql.InputObject
	singularQueryCache map[string]string
	singularTypeCache  map[string]string
	singularNamer      *naming.Namer
	namesApplied       bool
	orderDirection     *graphql.Enum
	nonNegativeInt     *graphql.Scalar
	jsonType           *graphql.Scalar
	dateType           *graphql.Scalar
	nodeInterface      *graphql.Interface
	limits             *planner.PlanLimits
	defaultLimit       int
	filters            schemafilter.Config
	mu                 sync.RWMutex
}

// NewResolver creates a new resolver with the given executor, schema, and optional limits.
// The executor handles SQL query execution, dbSchema provides the database structure,
// and limits (if non-nil) enforces query depth, complexity, and row count constraints.
func NewResolver(executor dbexec.QueryExecutor, dbSchema *introspection.Schema, limits *planner.PlanLimits, defaultLimit int, filters schemafilter.Config, namingConfig naming.Config) *Resolver {
	if defaultLimit <= 0 {
		defaultLimit = planner.DefaultListLimit
	}
	return &Resolver{
		executor:           executor,
		dbSchema:           dbSchema,
		typeCache:          make(map[string]*graphql.Object),
		orderByCache:       make(map[string]*graphql.InputObject),
		whereCache:         make(map[string]*graphql.InputObject),
		filterCache:        make(map[string]*graphql.InputObject),
		aggregateCache:     make(map[string]*graphql.Object),
		createInputCache:   make(map[string]*graphql.InputObject),
		updateInputCache:   make(map[string]*graphql.InputObject),
		deletePayloadCache: make(map[string]*graphql.Object),
		enumCache:          make(map[string]*graphql.Enum),
		enumFilterCache:    make(map[string]*graphql.InputObject),
		singularQueryCache: make(map[string]string),
		singularTypeCache:  make(map[string]string),
		singularNamer:      naming.New(namingConfig, nil),
		limits:             limits,
		defaultLimit:       defaultLimit,
		filters:            filters,
	}
}

// BuildGraphQLSchema constructs an executable GraphQL schema from the database schema.
// It creates types for each table, adds list and by-primary-key queries, and wires up
// relationship resolvers for foreign key navigation.
func (r *Resolver) BuildGraphQLSchema() (graphql.Schema, error) {
	r.applyNaming()

	queryFields := graphql.Fields{}

	for _, table := range r.dbSchema.Tables {
		queryFields = r.addTableQueries(queryFields, table)
	}

	// If no tables exist, add a placeholder query to satisfy GraphQL requirements
	if len(queryFields) == 0 {
		queryFields["_schema"] = &graphql.Field{
			Type: graphql.String,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				return "No tables found in database", nil
			},
			Description: "Placeholder field when database has no tables",
		}
	}
	if r.hasNodeTypes() {
		queryFields["node"] = &graphql.Field{
			Type: r.nodeInterfaceType(),
			Args: graphql.FieldConfigArgument{
				"id": &graphql.ArgumentConfig{
					Type: graphql.NewNonNull(graphql.ID),
				},
			},
			Resolve: r.makeNodeResolver(),
		}
	}

	rootQuery := graphql.ObjectConfig{
		Name:   "Query",
		Fields: queryFields,
	}

	mutationFields := graphql.Fields{}
	for _, table := range r.dbSchema.Tables {
		mutationFields = r.addTableMutations(mutationFields, table)
	}

	schemaConfig := graphql.SchemaConfig{
		Query: graphql.NewObject(rootQuery),
	}
	if len(mutationFields) > 0 {
		schemaConfig.Mutation = graphql.NewObject(graphql.ObjectConfig{
			Name:   "Mutation",
			Fields: mutationFields,
		})
	}
	if types := r.schemaTypes(); len(types) > 0 {
		schemaConfig.Types = types
	}

	return graphql.NewSchema(schemaConfig)
}

func (r *Resolver) applyNaming() {
	if r.dbSchema == nil {
		return
	}

	r.mu.Lock()
	if r.namesApplied {
		r.mu.Unlock()
		return
	}
	r.namesApplied = true
	namingConfig := r.singularNamer.Config()
	r.mu.Unlock()

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

	// List query
	fields[fieldName] = &graphql.Field{
		Type: graphql.NewNonNull(graphql.NewList(graphql.NewNonNull(tableType))),
		Args: graphql.FieldConfigArgument{
			"limit": &graphql.ArgumentConfig{
				Type:         r.nonNegativeIntScalar(),
				DefaultValue: r.defaultLimit,
			},
			"offset": &graphql.ArgumentConfig{
				Type:         r.nonNegativeIntScalar(),
				DefaultValue: 0,
			},
		},
		Resolve: r.makeListResolver(table),
	}
	if orderByInput := r.orderByInput(table); orderByInput != nil {
		fields[fieldName].Args["orderBy"] = &graphql.ArgumentConfig{
			Type: orderByInput,
		}
	}
	if whereInput := r.whereInput(table); whereInput != nil {
		fields[fieldName].Args["where"] = &graphql.ArgumentConfig{
			Type: whereInput,
		}
	}

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

	// Aggregate query
	aggregateFieldName := fieldName + "_aggregate"
	aggregateType := r.buildAggregateFieldsType(table)

	fields[aggregateFieldName] = &graphql.Field{
		Type:    aggregateType,
		Args:    r.aggregateArgs(table),
		Resolve: r.makeAggregateResolver(table),
	}

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

		fields[introspection.GraphQLFieldName(col)] = &graphql.Field{
			Type:        fieldType,
			Description: col.Comment,
		}
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
			// One-to-many: returns list of objects
			relatedTable, err := r.findTable(rel.RemoteTable)
			if err != nil {
				// Log error but continue - this shouldn't happen if schema was built correctly
				// The error will be caught at query time instead
				continue
			}
			relatedType := r.buildGraphQLType(relatedTable)

			fields[rel.GraphQLFieldName] = &graphql.Field{
				Type: graphql.NewNonNull(graphql.NewList(graphql.NewNonNull(relatedType))),
				Args: graphql.FieldConfigArgument{
					"limit": &graphql.ArgumentConfig{
						Type:         r.nonNegativeIntScalar(),
						DefaultValue: r.defaultLimit,
					},
					"offset": &graphql.ArgumentConfig{
						Type:         r.nonNegativeIntScalar(),
						DefaultValue: 0,
					},
				},
				Resolve: r.makeOneToManyResolver(table, rel),
			}
			if orderByInput := r.orderByInput(relatedTable); orderByInput != nil {
				fields[rel.GraphQLFieldName].Args["orderBy"] = &graphql.ArgumentConfig{
					Type: orderByInput,
				}
			}

			// Add aggregate field for this one-to-many relationship
			aggregateFieldName := rel.GraphQLFieldName + "_aggregate"
			aggregateType := r.buildAggregateFieldsType(relatedTable)
			fields[aggregateFieldName] = &graphql.Field{
				Type:    aggregateType,
				Args:    r.aggregateArgs(relatedTable),
				Resolve: r.makeRelationshipAggregateResolver(table, rel),
			}
		} else if rel.IsManyToMany {
			// Many-to-many through pure junction: returns list of related entities
			relatedTable, err := r.findTable(rel.RemoteTable)
			if err != nil {
				continue
			}
			relatedType := r.buildGraphQLType(relatedTable)

			fields[rel.GraphQLFieldName] = &graphql.Field{
				Type: graphql.NewNonNull(graphql.NewList(graphql.NewNonNull(relatedType))),
				Args: graphql.FieldConfigArgument{
					"limit": &graphql.ArgumentConfig{
						Type:         r.nonNegativeIntScalar(),
						DefaultValue: r.defaultLimit,
					},
					"offset": &graphql.ArgumentConfig{
						Type:         r.nonNegativeIntScalar(),
						DefaultValue: 0,
					},
				},
				Resolve: r.makeManyToManyResolver(table, rel),
			}
			if orderByInput := r.orderByInput(relatedTable); orderByInput != nil {
				fields[rel.GraphQLFieldName].Args["orderBy"] = &graphql.ArgumentConfig{
					Type: orderByInput,
				}
			}
		} else if rel.IsEdgeList {
			// Edge list through attribute junction: returns list of edge/junction objects
			junctionTable, err := r.findTable(rel.JunctionTable)
			if err != nil {
				continue
			}
			edgeType := r.buildGraphQLType(junctionTable)

			fields[rel.GraphQLFieldName] = &graphql.Field{
				Type: graphql.NewNonNull(graphql.NewList(graphql.NewNonNull(edgeType))),
				Args: graphql.FieldConfigArgument{
					"limit": &graphql.ArgumentConfig{
						Type:         r.nonNegativeIntScalar(),
						DefaultValue: r.defaultLimit,
					},
					"offset": &graphql.ArgumentConfig{
						Type:         r.nonNegativeIntScalar(),
						DefaultValue: 0,
					},
				},
				Resolve: r.makeEdgeListResolver(table, rel),
			}
			if orderByInput := r.orderByInput(junctionTable); orderByInput != nil {
				fields[rel.GraphQLFieldName].Args["orderBy"] = &graphql.ArgumentConfig{
					Type: orderByInput,
				}
			}
		}
	}

	return fields
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

func (r *Resolver) makeListResolver(table introspection.Table) graphql.FieldResolveFn {
	return func(p graphql.ResolveParams) (interface{}, error) {
		planned, err := r.planFromParams(p)
		if err != nil {
			return nil, fmt.Errorf("failed to build query: %w", err)
		}

		if planned.Table.Name != table.Name {
			return nil, fmt.Errorf("planned table mismatch: expected %s got %s", table.Name, planned.Table.Name)
		}

		rows, err := r.executor.QueryContext(p.Context, planned.Root.SQL, planned.Root.Args...)
		if err != nil {
			return nil, normalizeQueryError(err)
		}
		defer func() {
			_ = rows.Close()
		}()

		results, err := scanRows(rows, columnsForPlan(planned))
		if err != nil {
			return nil, err
		}

		seedBatchRows(p, results)

		return results, nil
	}
}

// makeSingleRowResolver creates a resolver that returns at most one row.
// Used for primary key lookups and unique key lookups.
func (r *Resolver) makeSingleRowResolver(table introspection.Table) graphql.FieldResolveFn {
	return func(p graphql.ResolveParams) (interface{}, error) {
		planned, err := r.planFromParams(p)
		if err != nil {
			return nil, fmt.Errorf("failed to build query: %w", err)
		}

		if planned.Table.Name != table.Name {
			return nil, fmt.Errorf("planned table mismatch: expected %s got %s", table.Name, planned.Table.Name)
		}

		rows, err := r.executor.QueryContext(p.Context, planned.Root.SQL, planned.Root.Args...)
		if err != nil {
			return nil, normalizeQueryError(err)
		}
		defer func() {
			_ = rows.Close()
		}()

		results, err := scanRows(rows, columnsForPlan(planned))
		if err != nil {
			return nil, err
		}

		if len(results) == 0 {
			return nil, nil
		}

		return results[0], nil
	}
}

func (r *Resolver) makeNodeIDResolver(table introspection.Table, pkCols []introspection.Column) graphql.FieldResolveFn {
	return func(p graphql.ResolveParams) (interface{}, error) {
		source, ok := p.Source.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("invalid source type")
		}
		values := make([]interface{}, len(pkCols))
		for i, col := range pkCols {
			fieldName := introspection.GraphQLFieldName(col)
			value, ok := source[fieldName]
			if !ok {
				return nil, fmt.Errorf("missing primary key field %s", fieldName)
			}
			values[i] = value
		}
		encoded := nodeid.Encode(introspection.GraphQLTypeName(table), values...)
		if encoded == "" {
			return nil, fmt.Errorf("failed to encode node id")
		}
		return encoded, nil
	}
}

func (r *Resolver) makePrimaryKeyResolver(table introspection.Table, pkCols []introspection.Column) graphql.FieldResolveFn {
	return func(p graphql.ResolveParams) (interface{}, error) {
		rawID, ok := p.Args["id"]
		if !ok || rawID == nil {
			return nil, fmt.Errorf("missing id argument")
		}
		id, ok := rawID.(string)
		if !ok {
			id = fmt.Sprint(rawID)
		}

		pkValues, err := r.pkValuesFromNodeID(table, pkCols, id)
		if err != nil {
			return nil, err
		}

		field := firstFieldAST(p.Info.FieldASTs)
		if field == nil {
			return nil, fmt.Errorf("missing field AST")
		}
		selected := planner.SelectedColumns(table, field, p.Info.Fragments)

		var query planner.SQLQuery
		if len(pkCols) == 1 {
			pk := &pkCols[0]
			query, err = planner.PlanTableByPK(table, selected, pk, pkValues[pk.Name])
		} else {
			query, err = planner.PlanTableByPKColumns(table, selected, pkCols, pkValues)
		}
		if err != nil {
			return nil, err
		}

		rows, err := r.executor.QueryContext(p.Context, query.SQL, query.Args...)
		if err != nil {
			return nil, normalizeQueryError(err)
		}
		defer func() {
			_ = rows.Close()
		}()

		results, err := scanRows(rows, selected)
		if err != nil {
			return nil, err
		}
		if len(results) == 0 {
			return nil, nil
		}
		return results[0], nil
	}
}

func (r *Resolver) makeNodeResolver() graphql.FieldResolveFn {
	return func(p graphql.ResolveParams) (interface{}, error) {
		rawID, ok := p.Args["id"]
		if !ok || rawID == nil {
			return nil, fmt.Errorf("missing id argument")
		}
		id, ok := rawID.(string)
		if !ok {
			id = fmt.Sprint(rawID)
		}

		typeName, rawValues, err := nodeid.Decode(id)
		if err != nil {
			return nil, err
		}

		table, err := r.findTableByTypeName(typeName)
		if err != nil {
			return nil, err
		}
		pkCols := introspection.PrimaryKeyColumns(table)
		if len(pkCols) == 0 {
			return nil, fmt.Errorf("table %s has no primary key", table.Name)
		}
		if len(rawValues) != len(pkCols) {
			return nil, fmt.Errorf("invalid id for %s", typeName)
		}

		pkValues := make(map[string]interface{}, len(pkCols))
		for i, col := range pkCols {
			parsed, err := nodeid.ParsePKValue(col, rawValues[i])
			if err != nil {
				return nil, err
			}
			pkValues[col.Name] = parsed
		}

		field := firstFieldAST(p.Info.FieldASTs)
		if field == nil {
			return nil, fmt.Errorf("missing field AST")
		}
		selected := planner.SelectedColumns(table, field, p.Info.Fragments)

		var query planner.SQLQuery
		if len(pkCols) == 1 {
			pk := &pkCols[0]
			query, err = planner.PlanTableByPK(table, selected, pk, pkValues[pk.Name])
		} else {
			query, err = planner.PlanTableByPKColumns(table, selected, pkCols, pkValues)
		}
		if err != nil {
			return nil, err
		}

		rows, err := r.executor.QueryContext(p.Context, query.SQL, query.Args...)
		if err != nil {
			return nil, normalizeQueryError(err)
		}
		defer func() {
			_ = rows.Close()
		}()

		results, err := scanRows(rows, selected)
		if err != nil {
			return nil, err
		}
		if len(results) == 0 {
			return nil, nil
		}
		results[0]["__typename"] = typeName
		return results[0], nil
	}
}

func (r *Resolver) planFromParams(p graphql.ResolveParams) (*planner.Plan, error) {
	field := firstFieldAST(p.Info.FieldASTs)
	if field == nil {
		return nil, fmt.Errorf("missing field AST")
	}

	if r.limits != nil {
		return planner.PlanQuery(r.dbSchema, field, p.Args, planner.WithFragments(p.Info.Fragments), planner.WithLimits(*r.limits), planner.WithDefaultListLimit(r.defaultLimit))
	}
	return planner.PlanQuery(r.dbSchema, field, p.Args, planner.WithFragments(p.Info.Fragments), planner.WithDefaultListLimit(r.defaultLimit))
}

func (r *Resolver) pkValuesFromNodeID(table introspection.Table, pkCols []introspection.Column, id string) (map[string]interface{}, error) {
	typeName, values, err := nodeid.Decode(id)
	if err != nil {
		return nil, err
	}
	expectedType := introspection.GraphQLTypeName(table)
	if typeName != expectedType {
		return nil, fmt.Errorf("invalid id for %s", expectedType)
	}
	if len(values) != len(pkCols) {
		return nil, fmt.Errorf("invalid id for %s", expectedType)
	}
	pkValues := make(map[string]interface{}, len(pkCols))
	for i, col := range pkCols {
		parsed, err := nodeid.ParsePKValue(col, values[i])
		if err != nil {
			return nil, err
		}
		pkValues[col.Name] = parsed
	}
	return pkValues, nil
}

func (r *Resolver) findTableByTypeName(typeName string) (introspection.Table, error) {
	if r.dbSchema == nil {
		return introspection.Table{}, fmt.Errorf("schema not available")
	}
	for _, table := range r.dbSchema.Tables {
		if introspection.GraphQLTypeName(table) == typeName {
			return table, nil
		}
	}
	return introspection.Table{}, fmt.Errorf("unknown type %s", typeName)
}

func (r *Resolver) hasNodeTypes() bool {
	if r.dbSchema == nil {
		return false
	}
	for _, table := range r.dbSchema.Tables {
		if len(introspection.PrimaryKeyColumns(table)) > 0 {
			return true
		}
	}
	return false
}

func (r *Resolver) schemaTypes() []graphql.Type {
	r.mu.RLock()
	if len(r.typeCache) == 0 {
		r.mu.RUnlock()
		return nil
	}
	keys := make([]string, 0, len(r.typeCache))
	for key := range r.typeCache {
		keys = append(keys, key)
	}
	r.mu.RUnlock()

	sort.Strings(keys)

	types := make([]graphql.Type, 0, len(keys))
	r.mu.RLock()
	for _, key := range keys {
		if objType, ok := r.typeCache[key]; ok {
			types = append(types, objType)
		}
	}
	r.mu.RUnlock()

	return types
}

func seedBatchRows(p graphql.ResolveParams, rows []map[string]interface{}) {
	if len(rows) == 0 {
		return
	}
	state, ok := getBatchState(p.Context)
	if !ok {
		return
	}
	parentKey := parentKeyFromResolve(p)
	for _, row := range rows {
		row[batchParentKeyField] = parentKey
	}
	state.setParentRows(parentKey, rows)
}

func (r *Resolver) aggregateArgs(table introspection.Table) graphql.FieldConfigArgument {
	args := graphql.FieldConfigArgument{
		"limit": &graphql.ArgumentConfig{
			Type: r.nonNegativeIntScalar(),
		},
		"offset": &graphql.ArgumentConfig{
			Type: r.nonNegativeIntScalar(),
		},
		// inheritListDefaults: When true, applies the same default limit/offset as list queries.
		// Useful for ensuring aggregate queries match their corresponding list queries
		// (e.g., "show me 50 orders AND their total").
		"inheritListDefaults": &graphql.ArgumentConfig{
			Type:         graphql.Boolean,
			DefaultValue: false,
		},
	}

	if orderByInput := r.orderByInput(table); orderByInput != nil {
		args["orderBy"] = &graphql.ArgumentConfig{
			Type: orderByInput,
		}
	}
	if whereInput := r.whereInput(table); whereInput != nil {
		args["where"] = &graphql.ArgumentConfig{
			Type: whereInput,
		}
	}

	return args
}

// aggregateFiltersFromArgs parses aggregate filter arguments from GraphQL args.
// extraIndexedColumns are added to index validation (e.g., relationship FK columns
// which are implicitly indexed but not in the user's WHERE clause).
func (r *Resolver) aggregateFiltersFromArgs(table introspection.Table, args map[string]interface{}, extraIndexedColumns ...string) (*planner.AggregateFilters, error) {
	limit, hasLimit := optionalIntArg(args, "limit")
	offset, hasOffset := optionalIntArg(args, "offset")
	inheritDefaults := boolArg(args, "inheritListDefaults")
	if inheritDefaults {
		if !hasLimit {
			limit = r.defaultLimit
			hasLimit = true
		}
		if !hasOffset {
			offset = 0
			hasOffset = true
		}
	}

	orderBy, err := planner.ParseOrderBy(table, args)
	if err != nil {
		return nil, err
	}

	var whereClause *planner.WhereClause
	if whereArg, ok := args["where"]; ok {
		if whereMap, ok := whereArg.(map[string]interface{}); ok {
			whereClause, err = planner.BuildWhereClause(table, whereMap)
			if err != nil {
				return nil, fmt.Errorf("invalid WHERE clause: %w", err)
			}
			if whereClause != nil {
				usedColumns := append([]string{}, whereClause.UsedColumns...)
				if len(extraIndexedColumns) > 0 {
					usedColumns = append(usedColumns, extraIndexedColumns...)
				}
				if err := planner.ValidateIndexedColumns(table, usedColumns); err != nil {
					return nil, err
				}
			}
		}
	}

	filters := &planner.AggregateFilters{
		Where:   whereClause,
		OrderBy: orderBy,
	}
	if hasLimit {
		filters.Limit = &limit
	}
	if hasOffset {
		filters.Offset = &offset
	}

	return filters, nil
}

func firstFieldAST(fields []*ast.Field) *ast.Field {
	if len(fields) == 0 {
		return nil
	}
	return fields[0]
}

func optionalIntArg(args map[string]interface{}, key string) (int, bool) {
	if args == nil {
		return 0, false
	}
	value, ok := args[key]
	if !ok || value == nil {
		return 0, false
	}
	intValue, ok := value.(int)
	if !ok {
		return 0, false
	}
	if intValue < 0 {
		return 0, false
	}
	return intValue, true
}

func boolArg(args map[string]interface{}, key string) bool {
	if args == nil {
		return false
	}
	value, ok := args[key]
	if !ok || value == nil {
		return false
	}
	boolValue, ok := value.(bool)
	if !ok {
		return false
	}
	return boolValue
}

func (r *Resolver) orderByInput(table introspection.Table) *graphql.InputObject {
	options := planner.OrderByOptions(table)
	if len(options) == 0 {
		return nil
	}

	typeName := introspection.GraphQLTypeName(table) + "OrderBy"
	r.mu.RLock()
	cached, ok := r.orderByCache[typeName]
	r.mu.RUnlock()
	if ok {
		return cached
	}

	fields := graphql.InputObjectConfigFieldMap{}
	for _, name := range sortedOrderByFieldNames(options) {
		fields[name] = &graphql.InputObjectFieldConfig{
			Type: r.orderDirectionEnum(),
		}
	}

	input := graphql.NewInputObject(graphql.InputObjectConfig{
		Name:   typeName,
		Fields: fields,
	})
	r.mu.Lock()
	if cached, ok := r.orderByCache[typeName]; ok {
		r.mu.Unlock()
		return cached
	}
	r.orderByCache[typeName] = input
	r.mu.Unlock()

	return input
}

func (r *Resolver) orderDirectionEnum() *graphql.Enum {
	r.mu.RLock()
	cached := r.orderDirection
	r.mu.RUnlock()
	if cached != nil {
		return cached
	}

	enumValue := graphql.NewEnum(graphql.EnumConfig{
		Name: "OrderDirection",
		Values: graphql.EnumValueConfigMap{
			"ASC":  &graphql.EnumValueConfig{Value: "ASC"},
			"DESC": &graphql.EnumValueConfig{Value: "DESC"},
		},
	})

	r.mu.Lock()
	if r.orderDirection == nil {
		r.orderDirection = enumValue
	}
	cached = r.orderDirection
	r.mu.Unlock()

	return cached
}

func (r *Resolver) nonNegativeIntScalar() *graphql.Scalar {
	r.mu.RLock()
	cached := r.nonNegativeInt
	r.mu.RUnlock()
	if cached != nil {
		return cached
	}

	scalar := graphql.NewScalar(graphql.ScalarConfig{
		Name:        "NonNegativeInt",
		Description: "An integer greater than or equal to zero.",
		Serialize: func(value interface{}) interface{} {
			if parsed, ok := coerceNonNegativeInt(value); ok {
				return parsed
			}
			return nil
		},
		ParseValue: func(value interface{}) interface{} {
			if parsed, ok := coerceNonNegativeInt(value); ok {
				return parsed
			}
			return nil
		},
		ParseLiteral: func(valueAST ast.Value) interface{} {
			intValue, ok := valueAST.(*ast.IntValue)
			if !ok {
				return nil
			}
			parsed, err := strconv.Atoi(intValue.Value)
			if err != nil || parsed < 0 {
				return nil
			}
			return parsed
		},
	})

	r.mu.Lock()
	if r.nonNegativeInt == nil {
		r.nonNegativeInt = scalar
	}
	cached = r.nonNegativeInt
	r.mu.Unlock()

	return cached
}

func (r *Resolver) jsonScalar() *graphql.Scalar {
	r.mu.RLock()
	cached := r.jsonType
	r.mu.RUnlock()
	if cached != nil {
		return cached
	}

	scalar := graphql.NewScalar(graphql.ScalarConfig{
		Name:        "JSON",
		Description: "Arbitrary JSON value serialized as a string.",
		Serialize: func(value interface{}) interface{} {
			switch v := value.(type) {
			case []byte:
				return string(v)
			case string:
				return v
			case nil:
				return nil
			default:
				serialized, err := json.Marshal(v)
				if err != nil {
					slog.Default().Warn("failed to serialize JSON scalar", slog.String("error", err.Error()))
					return nil
				}
				return string(serialized)
			}
		},
		ParseValue: func(value interface{}) interface{} {
			if s, ok := value.(string); ok {
				return s
			}
			return nil
		},
		ParseLiteral: func(valueAST ast.Value) interface{} {
			if sv, ok := valueAST.(*ast.StringValue); ok {
				return sv.Value
			}
			return nil
		},
	})

	r.mu.Lock()
	if r.jsonType == nil {
		r.jsonType = scalar
	}
	cached = r.jsonType
	r.mu.Unlock()

	return cached
}

func (r *Resolver) dateScalar() *graphql.Scalar {
	r.mu.RLock()
	cached := r.dateType
	r.mu.RUnlock()
	if cached != nil {
		return cached
	}

	scalar := graphql.NewScalar(graphql.ScalarConfig{
		Name:        "Date",
		Description: "Date value serialized as YYYY-MM-DD.",
		Serialize: func(value interface{}) interface{} {
			switch v := value.(type) {
			case time.Time:
				return v.UTC().Format("2006-01-02")
			case *time.Time:
				if v == nil {
					return nil
				}
				return v.UTC().Format("2006-01-02")
			default:
				return nil
			}
		},
		ParseValue: func(value interface{}) interface{} {
			switch v := value.(type) {
			case time.Time:
				return v
			case string:
				if parsed, err := time.Parse("2006-01-02", v); err == nil {
					return parsed
				}
				if parsed, err := time.Parse(time.RFC3339, v); err == nil {
					return time.Date(parsed.Year(), parsed.Month(), parsed.Day(), 0, 0, 0, 0, time.UTC)
				}
				return nil
			default:
				return nil
			}
		},
		ParseLiteral: func(valueAST ast.Value) interface{} {
			if sv, ok := valueAST.(*ast.StringValue); ok {
				if parsed, err := time.Parse("2006-01-02", sv.Value); err == nil {
					return parsed
				}
				if parsed, err := time.Parse(time.RFC3339, sv.Value); err == nil {
					return time.Date(parsed.Year(), parsed.Month(), parsed.Day(), 0, 0, 0, 0, time.UTC)
				}
			}
			return nil
		},
	})

	r.mu.Lock()
	if r.dateType == nil {
		r.dateType = scalar
	}
	cached = r.dateType
	r.mu.Unlock()

	return cached
}

func (r *Resolver) nodeInterfaceType() *graphql.Interface {
	r.mu.RLock()
	cached := r.nodeInterface
	r.mu.RUnlock()
	if cached != nil {
		return cached
	}

	nodeInterface := graphql.NewInterface(graphql.InterfaceConfig{
		Name: "Node",
		Fields: graphql.Fields{
			"id": &graphql.Field{Type: graphql.NewNonNull(graphql.ID)},
		},
		ResolveType: func(p graphql.ResolveTypeParams) *graphql.Object {
			source, ok := p.Value.(map[string]interface{})
			if !ok {
				return nil
			}
			typeName, ok := source["__typename"].(string)
			if !ok || typeName == "" {
				return nil
			}
			r.mu.RLock()
			objType := r.typeCache[typeName]
			r.mu.RUnlock()
			return objType
		},
	})

	r.mu.Lock()
	if r.nodeInterface == nil {
		r.nodeInterface = nodeInterface
	}
	cached = r.nodeInterface
	r.mu.Unlock()

	return cached
}

func coerceNonNegativeInt(value interface{}) (int, bool) {
	switch v := value.(type) {
	case int:
		if v < 0 {
			return 0, false
		}
		return v, true
	case int64:
		if v < 0 || v > math.MaxInt {
			return 0, false
		}
		return int(v), true
	case float64:
		if v < 0 || v > math.MaxInt || v != math.Trunc(v) {
			return 0, false
		}
		return int(v), true
	default:
		return 0, false
	}
}

func (r *Resolver) whereInput(table introspection.Table) *graphql.InputObject {
	if table.IsView {
		return nil
	}
	typeName := introspection.GraphQLTypeName(table) + "Where"
	r.mu.RLock()
	cached, ok := r.whereCache[typeName]
	r.mu.RUnlock()
	if ok {
		return cached
	}

	// Build field map for WHERE input
	fields := graphql.InputObjectConfigFieldMap{}

	for _, col := range table.Columns {
		// Skip JSON columns
		if sqltype.MapToGraphQL(col.DataType) == sqltype.TypeJSON {
			continue
		}

		fieldName := introspection.GraphQLFieldName(col)
		filterType := r.getFilterInputType(table, col)
		if filterType != nil {
			fields[fieldName] = &graphql.InputObjectFieldConfig{
				Type:        filterType,
				Description: col.Comment,
			}
		}
	}

	// Create a lazy-initialized input object to handle recursive reference
	var inputObj *graphql.InputObject
	inputObj = graphql.NewInputObject(graphql.InputObjectConfig{
		Name: typeName,
		Fields: graphql.InputObjectConfigFieldMapThunk(func() graphql.InputObjectConfigFieldMap {
			// Add AND/OR operators that reference this type
			fields["AND"] = &graphql.InputObjectFieldConfig{
				Type: graphql.NewList(graphql.NewNonNull(inputObj)),
			}
			fields["OR"] = &graphql.InputObjectFieldConfig{
				Type: graphql.NewList(graphql.NewNonNull(inputObj)),
			}
			return fields
		}),
	})

	r.mu.Lock()
	if cached, ok := r.whereCache[typeName]; ok {
		r.mu.Unlock()
		return cached
	}
	r.whereCache[typeName] = inputObj
	r.mu.Unlock()
	return inputObj
}

func (r *Resolver) enumTypeName(table introspection.Table, col introspection.Column) string {
	singularTable := r.singularNamer.Singularize(table.Name)
	return r.singularNamer.ToGraphQLTypeName(singularTable) + r.singularNamer.ToGraphQLTypeName(col.Name)
}

// normalizeEnumValueName gives a stable GraphQL-safe name for arbitrary SQL enum values.
func normalizeEnumValueName(value string) string {
	var b strings.Builder
	lastUnderscore := false
	for _, r := range value {
		if r <= 0x7F {
			ch := byte(r)
			isLetter := (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z')
			isDigit := ch >= '0' && ch <= '9'
			if isLetter || isDigit {
				if ch >= 'a' && ch <= 'z' {
					ch = ch - 'a' + 'A'
				}
				b.WriteByte(ch)
				lastUnderscore = false
				continue
			}
			if !lastUnderscore && b.Len() > 0 {
				b.WriteByte('_')
				lastUnderscore = true
			}
			continue
		}
		if b.Len() > 0 && !lastUnderscore {
			b.WriteByte('_')
		}
		// escape non-ascii characters. GraphQL (unlike TiDB) doesn't support non-ascii characters
		// in enum value names, so we need to escape them to ensure we can represent all possible
		// enum values.
		if r <= 0xFFFF {
			fmt.Fprintf(&b, "U%04X", r)
		} else {
			fmt.Fprintf(&b, "U%06X", r)
		}
		lastUnderscore = false
	}
	name := strings.Trim(b.String(), "_")
	if name == "" {
		name = "VALUE" // fallback for empty enum values
	}
	if name[0] >= '0' && name[0] <= '9' {
		name = "VALUE_" + name // GraphQL enum values can't start with a digit, so prefix with "VALUE_"
	}
	return name
}

func uniqueEnumValueName(base string, used map[string]int) string {
	name := base
	for {
		if count, ok := used[name]; ok {
			count++
			used[name] = count
			name = fmt.Sprintf("%s_%d", base, count)
			continue
		}
		used[name] = 1
		return name
	}
}

// enumTypeForColumn preserves DB enum values while exposing GraphQL enums consistently.
func (r *Resolver) enumTypeForColumn(table introspection.Table, col *introspection.Column) *graphql.Enum {
	if col == nil || len(col.EnumValues) == 0 {
		return nil
	}
	enumName := r.enumTypeName(table, *col)
	r.mu.RLock()
	cached := r.enumCache[enumName]
	r.mu.RUnlock()
	if cached != nil {
		return cached
	}

	used := make(map[string]int)
	values := graphql.EnumValueConfigMap{}
	for _, value := range col.EnumValues {
		enumValueName := normalizeEnumValueName(value)
		enumValueName = uniqueEnumValueName(enumValueName, used)
		values[enumValueName] = &graphql.EnumValueConfig{Value: value}
	}

	enumType := graphql.NewEnum(graphql.EnumConfig{
		Name:   enumName,
		Values: values,
	})

	r.mu.Lock()
	if cached := r.enumCache[enumName]; cached != nil {
		r.mu.Unlock()
		return cached
	}
	r.enumCache[enumName] = enumType
	r.mu.Unlock()

	return enumType
}

func (r *Resolver) enumFilterType(enumType *graphql.Enum) *graphql.InputObject {
	if enumType == nil {
		return nil
	}
	filterName := enumType.Name() + "Filter"

	r.mu.RLock()
	cached := r.enumFilterCache[filterName]
	r.mu.RUnlock()
	if cached != nil {
		return cached
	}

	filterType := graphql.NewInputObject(graphql.InputObjectConfig{
		Name: filterName,
		Fields: graphql.InputObjectConfigFieldMap{
			"eq":     &graphql.InputObjectFieldConfig{Type: enumType},
			"ne":     &graphql.InputObjectFieldConfig{Type: enumType},
			"in":     &graphql.InputObjectFieldConfig{Type: graphql.NewList(graphql.NewNonNull(enumType))},
			"notIn":  &graphql.InputObjectFieldConfig{Type: graphql.NewList(graphql.NewNonNull(enumType))},
			"isNull": &graphql.InputObjectFieldConfig{Type: graphql.Boolean},
		},
	})

	r.mu.Lock()
	if cached := r.enumFilterCache[filterName]; cached != nil {
		r.mu.Unlock()
		return cached
	}
	r.enumFilterCache[filterName] = filterType
	r.mu.Unlock()

	return filterType
}

func (r *Resolver) getFilterInputType(table introspection.Table, col introspection.Column) *graphql.InputObject {
	if enumType := r.enumTypeForColumn(table, &col); enumType != nil {
		return r.enumFilterType(enumType)
	}

	filterName := sqltype.MapToGraphQL(col.DataType).FilterTypeName()

	// Check cache
	r.mu.RLock()
	cached, ok := r.filterCache[filterName]
	r.mu.RUnlock()
	if ok {
		return cached
	}

	// Create the filter input type
	var filterType *graphql.InputObject
	switch filterName {
	case "IntFilter":
		filterType = graphql.NewInputObject(graphql.InputObjectConfig{
			Name: "IntFilter",
			Fields: graphql.InputObjectConfigFieldMap{
				"eq":     &graphql.InputObjectFieldConfig{Type: graphql.Int},
				"ne":     &graphql.InputObjectFieldConfig{Type: graphql.Int},
				"lt":     &graphql.InputObjectFieldConfig{Type: graphql.Int},
				"lte":    &graphql.InputObjectFieldConfig{Type: graphql.Int},
				"gt":     &graphql.InputObjectFieldConfig{Type: graphql.Int},
				"gte":    &graphql.InputObjectFieldConfig{Type: graphql.Int},
				"in":     &graphql.InputObjectFieldConfig{Type: graphql.NewList(graphql.NewNonNull(graphql.Int))},
				"notIn":  &graphql.InputObjectFieldConfig{Type: graphql.NewList(graphql.NewNonNull(graphql.Int))},
				"isNull": &graphql.InputObjectFieldConfig{Type: graphql.Boolean},
			},
		})
	case "FloatFilter":
		filterType = graphql.NewInputObject(graphql.InputObjectConfig{
			Name: "FloatFilter",
			Fields: graphql.InputObjectConfigFieldMap{
				"eq":     &graphql.InputObjectFieldConfig{Type: graphql.Float},
				"ne":     &graphql.InputObjectFieldConfig{Type: graphql.Float},
				"lt":     &graphql.InputObjectFieldConfig{Type: graphql.Float},
				"lte":    &graphql.InputObjectFieldConfig{Type: graphql.Float},
				"gt":     &graphql.InputObjectFieldConfig{Type: graphql.Float},
				"gte":    &graphql.InputObjectFieldConfig{Type: graphql.Float},
				"in":     &graphql.InputObjectFieldConfig{Type: graphql.NewList(graphql.NewNonNull(graphql.Float))},
				"notIn":  &graphql.InputObjectFieldConfig{Type: graphql.NewList(graphql.NewNonNull(graphql.Float))},
				"isNull": &graphql.InputObjectFieldConfig{Type: graphql.Boolean},
			},
		})
	case "StringFilter":
		filterType = graphql.NewInputObject(graphql.InputObjectConfig{
			Name: "StringFilter",
			Fields: graphql.InputObjectConfigFieldMap{
				"eq":      &graphql.InputObjectFieldConfig{Type: graphql.String},
				"ne":      &graphql.InputObjectFieldConfig{Type: graphql.String},
				"lt":      &graphql.InputObjectFieldConfig{Type: graphql.String},
				"lte":     &graphql.InputObjectFieldConfig{Type: graphql.String},
				"gt":      &graphql.InputObjectFieldConfig{Type: graphql.String},
				"gte":     &graphql.InputObjectFieldConfig{Type: graphql.String},
				"in":      &graphql.InputObjectFieldConfig{Type: graphql.NewList(graphql.NewNonNull(graphql.String))},
				"notIn":   &graphql.InputObjectFieldConfig{Type: graphql.NewList(graphql.NewNonNull(graphql.String))},
				"like":    &graphql.InputObjectFieldConfig{Type: graphql.String},
				"notLike": &graphql.InputObjectFieldConfig{Type: graphql.String},
				"isNull":  &graphql.InputObjectFieldConfig{Type: graphql.Boolean},
			},
		})
	case "BooleanFilter":
		filterType = graphql.NewInputObject(graphql.InputObjectConfig{
			Name: "BooleanFilter",
			Fields: graphql.InputObjectConfigFieldMap{
				"eq":     &graphql.InputObjectFieldConfig{Type: graphql.Boolean},
				"ne":     &graphql.InputObjectFieldConfig{Type: graphql.Boolean},
				"isNull": &graphql.InputObjectFieldConfig{Type: graphql.Boolean},
			},
		})
	case "DateFilter":
		filterType = graphql.NewInputObject(graphql.InputObjectConfig{
			Name: "DateFilter",
			Fields: graphql.InputObjectConfigFieldMap{
				"eq":     &graphql.InputObjectFieldConfig{Type: r.dateScalar()},
				"ne":     &graphql.InputObjectFieldConfig{Type: r.dateScalar()},
				"lt":     &graphql.InputObjectFieldConfig{Type: r.dateScalar()},
				"lte":    &graphql.InputObjectFieldConfig{Type: r.dateScalar()},
				"gt":     &graphql.InputObjectFieldConfig{Type: r.dateScalar()},
				"gte":    &graphql.InputObjectFieldConfig{Type: r.dateScalar()},
				"in":     &graphql.InputObjectFieldConfig{Type: graphql.NewList(graphql.NewNonNull(r.dateScalar()))},
				"notIn":  &graphql.InputObjectFieldConfig{Type: graphql.NewList(graphql.NewNonNull(r.dateScalar()))},
				"isNull": &graphql.InputObjectFieldConfig{Type: graphql.Boolean},
			},
		})
	case "DateTimeFilter":
		filterType = graphql.NewInputObject(graphql.InputObjectConfig{
			Name: "DateTimeFilter",
			Fields: graphql.InputObjectConfigFieldMap{
				"eq":     &graphql.InputObjectFieldConfig{Type: graphql.DateTime},
				"ne":     &graphql.InputObjectFieldConfig{Type: graphql.DateTime},
				"lt":     &graphql.InputObjectFieldConfig{Type: graphql.DateTime},
				"lte":    &graphql.InputObjectFieldConfig{Type: graphql.DateTime},
				"gt":     &graphql.InputObjectFieldConfig{Type: graphql.DateTime},
				"gte":    &graphql.InputObjectFieldConfig{Type: graphql.DateTime},
				"in":     &graphql.InputObjectFieldConfig{Type: graphql.NewList(graphql.NewNonNull(graphql.DateTime))},
				"notIn":  &graphql.InputObjectFieldConfig{Type: graphql.NewList(graphql.NewNonNull(graphql.DateTime))},
				"isNull": &graphql.InputObjectFieldConfig{Type: graphql.Boolean},
			},
		})
	}

	r.mu.Lock()
	if cached, ok := r.filterCache[filterName]; ok {
		r.mu.Unlock()
		return cached
	}
	r.filterCache[filterName] = filterType
	r.mu.Unlock()
	return filterType
}

func sortedOrderByFieldNames(options map[string][]string) []string {
	names := make([]string, 0, len(options))
	for name := range options {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func (r *Resolver) tryBatchOneToMany(p graphql.ResolveParams, table introspection.Table, rel introspection.Relationship, pkValue interface{}) ([]map[string]interface{}, bool, error) {
	metrics := graphQLMetricsFromContext(p.Context)

	state, ok := getBatchState(p.Context)
	if !ok {
		if metrics != nil {
			metrics.RecordBatchSkipped(p.Context, relationOneToMany, "no_batch_state")
		}
		return nil, false, nil
	}

	parentKey, ok := parentKeyFromSource(p.Source)
	if !ok {
		if metrics != nil {
			metrics.RecordBatchSkipped(p.Context, relationOneToMany, "missing_parent_key")
		}
		return nil, false, nil
	}

	parentRows := state.getParentRows(parentKey)
	if len(parentRows) == 0 {
		if metrics != nil {
			metrics.RecordBatchSkipped(p.Context, relationOneToMany, "missing_parent_rows")
		}
		return nil, false, nil
	}

	limit := planner.GetArgInt(p.Args, "limit", r.defaultLimit)
	offset := planner.GetArgInt(p.Args, "offset", 0)

	relatedTable, err := r.findTable(rel.RemoteTable)
	if err != nil {
		return nil, true, fmt.Errorf("failed to find related table %s: %w", rel.RemoteTable, err)
	}

	orderBy, err := planner.ParseOrderBy(relatedTable, p.Args)
	if err != nil {
		return nil, true, err
	}

	selection := planner.SelectedColumns(relatedTable, firstFieldAST(p.Info.FieldASTs), p.Info.Fragments)

	relKey := fmt.Sprintf(
		"%s|%s|%s|%d|%d|%s|%s|%s",
		table.Name,
		rel.RemoteTable,
		rel.RemoteColumn,
		limit,
		offset,
		orderByKey(orderBy),
		columnsKey(selection),
		parentKey,
	)

	if cached := state.getChildRows(relKey); cached != nil {
		state.IncrementCacheHit()
		if metrics != nil {
			metrics.RecordBatchCacheHit(p.Context, relationOneToMany)
		}
		return cached[fmt.Sprint(pkValue)], true, nil
	}
	state.IncrementCacheMiss()
	if metrics != nil {
		metrics.RecordBatchCacheMiss(p.Context, relationOneToMany)
	}

	parentField := graphQLFieldNameForColumn(table, rel.LocalColumn)
	parentValues := uniqueParentValues(parentRows, parentField)
	if len(parentValues) == 0 {
		state.setChildRows(relKey, map[string][]map[string]interface{}{})
		return []map[string]interface{}{}, true, nil
	}

	chunks := chunkValues(parentValues, batchMaxInClause)
	if metrics != nil {
		metrics.RecordBatchQueriesSaved(p.Context, listBatchQueriesSaved(len(parentValues), len(chunks)), relationOneToMany)
	}

	grouped := make(map[string][]map[string]interface{})
	for _, chunk := range chunks {
		if metrics != nil {
			metrics.RecordBatchParentCount(p.Context, int64(len(chunk)), relationOneToMany)
		}
		planned, err := planner.PlanOneToManyBatch(relatedTable, selection, rel.RemoteColumn, chunk, limit, offset, orderBy)
		if err != nil {
			if errors.Is(err, planner.ErrNoPrimaryKey) {
				if metrics != nil {
					metrics.RecordBatchSkipped(p.Context, relationOneToMany, "no_primary_key")
				}
				return nil, false, nil
			}
			return nil, true, err
		}
		if planned.SQL == "" {
			continue
		}

		rows, err := r.executor.QueryContext(p.Context, planned.SQL, planned.Args...)
		if err != nil {
			return nil, true, normalizeQueryError(err)
		}
		results, err := scanRowsWithExtras(rows, selection, []string{planner.BatchParentAlias})
		rows.Close()
		if err != nil {
			return nil, true, err
		}
		if metrics != nil {
			metrics.RecordBatchResultRows(p.Context, int64(len(results)), relationOneToMany)
		}

		mergeGrouped(grouped, groupByAlias(results, planner.BatchParentAlias))
	}
	if len(grouped) == 0 {
		state.setChildRows(relKey, map[string][]map[string]interface{}{})
		return []map[string]interface{}{}, true, nil
	}
	state.setChildRows(relKey, grouped)

	return grouped[fmt.Sprint(pkValue)], true, nil
}

func (r *Resolver) tryBatchManyToMany(p graphql.ResolveParams, table introspection.Table, rel introspection.Relationship, pkValue interface{}) ([]map[string]interface{}, bool, error) {
	metrics := graphQLMetricsFromContext(p.Context)

	state, ok := getBatchState(p.Context)
	if !ok {
		if metrics != nil {
			metrics.RecordBatchSkipped(p.Context, relationManyToMany, "no_batch_state")
		}
		return nil, false, nil
	}

	parentKey, ok := parentKeyFromSource(p.Source)
	if !ok {
		if metrics != nil {
			metrics.RecordBatchSkipped(p.Context, relationManyToMany, "missing_parent_key")
		}
		return nil, false, nil
	}

	parentRows := state.getParentRows(parentKey)
	if len(parentRows) == 0 {
		if metrics != nil {
			metrics.RecordBatchSkipped(p.Context, relationManyToMany, "missing_parent_rows")
		}
		return nil, false, nil
	}

	limit := planner.GetArgInt(p.Args, "limit", r.defaultLimit)
	offset := planner.GetArgInt(p.Args, "offset", 0)

	relatedTable, err := r.findTable(rel.RemoteTable)
	if err != nil {
		return nil, true, fmt.Errorf("failed to find related table %s: %w", rel.RemoteTable, err)
	}

	orderBy, err := planner.ParseOrderBy(relatedTable, p.Args)
	if err != nil {
		return nil, true, err
	}

	field := firstFieldAST(p.Info.FieldASTs)
	if field == nil {
		return nil, true, fmt.Errorf("missing field AST")
	}
	selection := planner.SelectedColumns(relatedTable, field, p.Info.Fragments)

	relKey := fmt.Sprintf(
		"%s|%s|%s|%s|%d|%d|%s|%s|%s",
		table.Name,
		rel.RemoteTable,
		rel.RemoteColumn,
		rel.JunctionTable,
		limit,
		offset,
		orderByKey(orderBy),
		columnsKey(selection),
		parentKey,
	)

	if cached := state.getChildRows(relKey); cached != nil {
		state.IncrementCacheHit()
		if metrics != nil {
			metrics.RecordBatchCacheHit(p.Context, relationManyToMany)
		}
		return cached[fmt.Sprint(pkValue)], true, nil
	}
	state.IncrementCacheMiss()
	if metrics != nil {
		metrics.RecordBatchCacheMiss(p.Context, relationManyToMany)
	}

	parentField := graphQLFieldNameForColumn(table, rel.LocalColumn)
	parentValues := uniqueParentValues(parentRows, parentField)
	if len(parentValues) == 0 {
		state.setChildRows(relKey, map[string][]map[string]interface{}{})
		return []map[string]interface{}{}, true, nil
	}

	chunks := chunkValues(parentValues, batchMaxInClause)
	if metrics != nil {
		metrics.RecordBatchQueriesSaved(p.Context, listBatchQueriesSaved(len(parentValues), len(chunks)), relationManyToMany)
	}

	grouped := make(map[string][]map[string]interface{})
	for _, chunk := range chunks {
		if metrics != nil {
			metrics.RecordBatchParentCount(p.Context, int64(len(chunk)), relationManyToMany)
		}
		planned, err := planner.PlanManyToManyBatch(
			rel.JunctionTable,
			relatedTable,
			rel.JunctionLocalFK,
			rel.JunctionRemoteFK,
			rel.RemoteColumn,
			selection,
			chunk,
			limit,
			offset,
			orderBy,
		)
		if err != nil {
			if errors.Is(err, planner.ErrNoPrimaryKey) {
				if metrics != nil {
					metrics.RecordBatchSkipped(p.Context, relationManyToMany, "no_primary_key")
				}
				return nil, false, nil
			}
			return nil, true, err
		}
		if planned.SQL == "" {
			continue
		}

		rows, err := r.executor.QueryContext(p.Context, planned.SQL, planned.Args...)
		if err != nil {
			return nil, true, normalizeQueryError(err)
		}
		results, err := scanRowsWithExtras(rows, selection, []string{planner.BatchParentAlias})
		rows.Close()
		if err != nil {
			return nil, true, err
		}
		if metrics != nil {
			metrics.RecordBatchResultRows(p.Context, int64(len(results)), relationManyToMany)
		}

		mergeGrouped(grouped, groupByAlias(results, planner.BatchParentAlias))
	}

	if len(grouped) == 0 {
		state.setChildRows(relKey, map[string][]map[string]interface{}{})
		return []map[string]interface{}{}, true, nil
	}
	state.setChildRows(relKey, grouped)

	return grouped[fmt.Sprint(pkValue)], true, nil
}

func (r *Resolver) tryBatchEdgeList(p graphql.ResolveParams, table introspection.Table, rel introspection.Relationship, pkValue interface{}) ([]map[string]interface{}, bool, error) {
	metrics := graphQLMetricsFromContext(p.Context)

	state, ok := getBatchState(p.Context)
	if !ok {
		if metrics != nil {
			metrics.RecordBatchSkipped(p.Context, relationEdgeList, "no_batch_state")
		}
		return nil, false, nil
	}

	parentKey, ok := parentKeyFromSource(p.Source)
	if !ok {
		if metrics != nil {
			metrics.RecordBatchSkipped(p.Context, relationEdgeList, "missing_parent_key")
		}
		return nil, false, nil
	}

	parentRows := state.getParentRows(parentKey)
	if len(parentRows) == 0 {
		if metrics != nil {
			metrics.RecordBatchSkipped(p.Context, relationEdgeList, "missing_parent_rows")
		}
		return nil, false, nil
	}

	limit := planner.GetArgInt(p.Args, "limit", r.defaultLimit)
	offset := planner.GetArgInt(p.Args, "offset", 0)

	junctionTable, err := r.findTable(rel.JunctionTable)
	if err != nil {
		return nil, true, fmt.Errorf("failed to find junction table %s: %w", rel.JunctionTable, err)
	}

	orderBy, err := planner.ParseOrderBy(junctionTable, p.Args)
	if err != nil {
		return nil, true, err
	}

	field := firstFieldAST(p.Info.FieldASTs)
	if field == nil {
		return nil, true, fmt.Errorf("missing field AST")
	}
	selection := planner.SelectedColumns(junctionTable, field, p.Info.Fragments)

	relKey := fmt.Sprintf(
		"%s|%s|%s|%d|%d|%s|%s|%s",
		table.Name,
		rel.JunctionTable,
		rel.JunctionLocalFK,
		limit,
		offset,
		orderByKey(orderBy),
		columnsKey(selection),
		parentKey,
	)

	if cached := state.getChildRows(relKey); cached != nil {
		state.IncrementCacheHit()
		if metrics != nil {
			metrics.RecordBatchCacheHit(p.Context, relationEdgeList)
		}
		return cached[fmt.Sprint(pkValue)], true, nil
	}
	state.IncrementCacheMiss()
	if metrics != nil {
		metrics.RecordBatchCacheMiss(p.Context, relationEdgeList)
	}

	parentField := graphQLFieldNameForColumn(table, rel.LocalColumn)
	parentValues := uniqueParentValues(parentRows, parentField)
	if len(parentValues) == 0 {
		state.setChildRows(relKey, map[string][]map[string]interface{}{})
		return []map[string]interface{}{}, true, nil
	}

	chunks := chunkValues(parentValues, batchMaxInClause)
	if metrics != nil {
		metrics.RecordBatchQueriesSaved(p.Context, listBatchQueriesSaved(len(parentValues), len(chunks)), relationEdgeList)
	}

	grouped := make(map[string][]map[string]interface{})
	for _, chunk := range chunks {
		if metrics != nil {
			metrics.RecordBatchParentCount(p.Context, int64(len(chunk)), relationEdgeList)
		}
		planned, err := planner.PlanEdgeListBatch(junctionTable, rel.JunctionLocalFK, selection, chunk, limit, offset, orderBy)
		if err != nil {
			if errors.Is(err, planner.ErrNoPrimaryKey) {
				if metrics != nil {
					metrics.RecordBatchSkipped(p.Context, relationEdgeList, "no_primary_key")
				}
				return nil, false, nil
			}
			return nil, true, err
		}
		if planned.SQL == "" {
			continue
		}

		rows, err := r.executor.QueryContext(p.Context, planned.SQL, planned.Args...)
		if err != nil {
			return nil, true, normalizeQueryError(err)
		}
		results, err := scanRowsWithExtras(rows, selection, []string{planner.BatchParentAlias})
		rows.Close()
		if err != nil {
			return nil, true, err
		}
		if metrics != nil {
			metrics.RecordBatchResultRows(p.Context, int64(len(results)), relationEdgeList)
		}

		mergeGrouped(grouped, groupByAlias(results, planner.BatchParentAlias))
	}

	if len(grouped) == 0 {
		state.setChildRows(relKey, map[string][]map[string]interface{}{})
		return []map[string]interface{}{}, true, nil
	}
	state.setChildRows(relKey, grouped)

	return grouped[fmt.Sprint(pkValue)], true, nil
}

func (r *Resolver) tryBatchRelationshipAggregate(
	p graphql.ResolveParams,
	table introspection.Table,
	rel introspection.Relationship,
	pkValue interface{},
	selection planner.AggregateSelection,
	filters *planner.AggregateFilters,
) (map[string]interface{}, bool, error) {
	metrics := graphQLMetricsFromContext(p.Context)

	state, ok := getBatchState(p.Context)
	if !ok {
		if metrics != nil {
			metrics.RecordBatchSkipped(p.Context, relationAggregate, "no_batch_state")
		}
		return nil, false, nil
	}

	parentKey, ok := parentKeyFromSource(p.Source)
	if !ok {
		if metrics != nil {
			metrics.RecordBatchSkipped(p.Context, relationAggregate, "missing_parent_key")
		}
		return nil, false, nil
	}

	parentRows := state.getParentRows(parentKey)
	if len(parentRows) == 0 {
		if metrics != nil {
			metrics.RecordBatchSkipped(p.Context, relationAggregate, "missing_parent_rows")
		}
		return nil, false, nil
	}

	// Skip batching if per-parent limit/offset or orderBy is requested.
	if filters != nil {
		if filters.Limit != nil || filters.Offset != nil || filters.OrderBy != nil {
			if metrics != nil {
				metrics.RecordBatchSkipped(p.Context, relationAggregate, "aggregate_filters")
			}
			return nil, false, nil
		}
	}

	relatedTable, err := r.findTable(rel.RemoteTable)
	if err != nil {
		return nil, true, fmt.Errorf("failed to find related table %s: %w", rel.RemoteTable, err)
	}

	columns := planner.BuildAggregateColumns(selection)
	relKey := fmt.Sprintf(
		"%s|%s|%s|%s|%s|%s",
		table.Name,
		rel.RemoteTable,
		rel.RemoteColumn,
		aggregateColumnsKey(columns),
		stableArgsKey(p.Args),
		parentKey,
	)

	if cached := state.getAggregateRows(relKey); cached != nil {
		state.IncrementCacheHit()
		if metrics != nil {
			metrics.RecordBatchCacheHit(p.Context, relationAggregate)
		}
		if result, ok := cached[fmt.Sprint(pkValue)]; ok {
			return result, true, nil
		}
		return map[string]interface{}{"count": int64(0)}, true, nil
	}
	state.IncrementCacheMiss()
	if metrics != nil {
		metrics.RecordBatchCacheMiss(p.Context, relationAggregate)
	}

	parentField := graphQLFieldNameForColumn(table, rel.LocalColumn)
	parentValues := uniqueParentValues(parentRows, parentField)
	if len(parentValues) == 0 {
		state.setAggregateRows(relKey, map[string]map[string]interface{}{})
		return map[string]interface{}{"count": int64(0)}, true, nil
	}

	if metrics != nil {
		metrics.RecordBatchParentCount(p.Context, int64(len(parentValues)), relationAggregate)
		metrics.RecordBatchQueriesSaved(p.Context, int64(len(parentValues)-1), relationAggregate)
	}

	var whereClause *planner.WhereClause
	if filters != nil {
		whereClause = filters.Where
	}

	planned, err := planner.PlanRelationshipAggregateBatch(relatedTable, selection, rel.RemoteColumn, parentValues, whereClause)
	if err != nil {
		return nil, true, err
	}
	if planned.SQL == "" {
		state.setAggregateRows(relKey, map[string]map[string]interface{}{})
		return map[string]interface{}{"count": int64(0)}, true, nil
	}

	rows, err := r.executor.QueryContext(p.Context, planned.SQL, planned.Args...)
	if err != nil {
		return nil, true, normalizeQueryError(err)
	}
	grouped, err := scanAggregateRows(rows, columns, relatedTable)
	rows.Close()
	if err != nil {
		return nil, true, err
	}
	if metrics != nil {
		metrics.RecordBatchResultRows(p.Context, int64(len(grouped)), relationAggregate)
	}

	state.setAggregateRows(relKey, grouped)
	if result, ok := grouped[fmt.Sprint(pkValue)]; ok {
		return result, true, nil
	}
	return map[string]interface{}{"count": int64(0)}, true, nil
}

func (r *Resolver) tryBatchManyToOne(p graphql.ResolveParams, table introspection.Table, rel introspection.Relationship, fkValue interface{}) (map[string]interface{}, bool, error) {
	metrics := graphQLMetricsFromContext(p.Context)

	state, ok := getBatchState(p.Context)
	if !ok {
		if metrics != nil {
			metrics.RecordBatchSkipped(p.Context, relationManyToOne, "no_batch_state")
		}
		return nil, false, nil
	}

	parentKey, ok := parentKeyFromSource(p.Source)
	if !ok {
		if metrics != nil {
			metrics.RecordBatchSkipped(p.Context, relationManyToOne, "missing_parent_key")
		}
		return nil, false, nil
	}

	parentRows := state.getParentRows(parentKey)
	if len(parentRows) == 0 {
		if metrics != nil {
			metrics.RecordBatchSkipped(p.Context, relationManyToOne, "missing_parent_rows")
		}
		return nil, false, nil
	}

	relatedTable, err := r.findTable(rel.RemoteTable)
	if err != nil {
		return nil, true, fmt.Errorf("failed to find related table %s: %w", rel.RemoteTable, err)
	}

	field := firstFieldAST(p.Info.FieldASTs)
	if field == nil {
		return nil, true, fmt.Errorf("missing field AST")
	}
	selection := planner.SelectedColumns(relatedTable, field, p.Info.Fragments)

	relKey := fmt.Sprintf("%s|%s|%s|%s", relatedTable.Name, rel.RemoteColumn, parentKey, columnsKey(selection))
	if cached := state.getChildRows(relKey); cached != nil {
		state.IncrementCacheHit()
		if metrics != nil {
			metrics.RecordBatchCacheHit(p.Context, relationManyToOne)
		}
		return firstGroupedRecord(cached, fkValue), true, nil
	}
	state.IncrementCacheMiss()
	if metrics != nil {
		metrics.RecordBatchCacheMiss(p.Context, relationManyToOne)
	}

	parentField := graphQLFieldNameForColumn(table, rel.LocalColumn)
	parentValues := uniqueParentValues(parentRows, parentField)
	if len(parentValues) == 0 {
		state.setChildRows(relKey, map[string][]map[string]interface{}{})
		return nil, true, nil
	}

	chunks := chunkValues(parentValues, batchMaxInClause)
	if metrics != nil {
		metrics.RecordBatchQueriesSaved(p.Context, listBatchQueriesSaved(len(parentValues), len(chunks)), relationManyToOne)
	}

	grouped := make(map[string][]map[string]interface{})
	for _, chunk := range chunks {
		if metrics != nil {
			metrics.RecordBatchParentCount(p.Context, int64(len(chunk)), relationManyToOne)
		}
		planned, err := planner.PlanManyToOneBatch(relatedTable, selection, rel.RemoteColumn, chunk)
		if err != nil {
			return nil, true, err
		}
		if planned.SQL == "" {
			continue
		}

		rows, err := r.executor.QueryContext(p.Context, planned.SQL, planned.Args...)
		if err != nil {
			return nil, true, normalizeQueryError(err)
		}
		results, err := scanRowsWithExtras(rows, selection, []string{planner.BatchParentAlias})
		rows.Close()
		if err != nil {
			return nil, true, err
		}
		if metrics != nil {
			metrics.RecordBatchResultRows(p.Context, int64(len(results)), relationManyToOne)
		}

		mergeGrouped(grouped, groupByAlias(results, planner.BatchParentAlias))
	}
	if len(grouped) == 0 {
		state.setChildRows(relKey, map[string][]map[string]interface{}{})
		return nil, true, nil
	}
	state.setChildRows(relKey, grouped)

	return firstGroupedRecord(grouped, fkValue), true, nil
}

func uniqueParentValues(rows []map[string]interface{}, key string) []interface{} {
	seen := make(map[string]struct{})
	values := make([]interface{}, 0, len(rows))

	for _, row := range rows {
		raw := row[key]
		if raw == nil {
			continue
		}
		normalized := fmt.Sprint(raw)
		if _, ok := seen[normalized]; ok {
			continue
		}
		seen[normalized] = struct{}{}
		values = append(values, raw)
	}

	return values
}

func groupByField(rows []map[string]interface{}, fieldName string) map[string][]map[string]interface{} {
	grouped := make(map[string][]map[string]interface{})
	for _, row := range rows {
		key := fmt.Sprint(row[fieldName])
		grouped[key] = append(grouped[key], row)
	}
	return grouped
}

func groupByAlias(rows []map[string]interface{}, alias string) map[string][]map[string]interface{} {
	grouped := make(map[string][]map[string]interface{})
	for _, row := range rows {
		key := fmt.Sprint(row[alias])
		delete(row, alias)
		grouped[key] = append(grouped[key], row)
	}
	return grouped
}

func mergeGrouped(target, src map[string][]map[string]interface{}) {
	for key, rows := range src {
		target[key] = append(target[key], rows...)
	}
}

func chunkValues(values []interface{}, max int) [][]interface{} {
	if len(values) == 0 {
		return nil
	}
	if max <= 0 || len(values) <= max {
		return [][]interface{}{values}
	}
	chunks := make([][]interface{}, 0, (len(values)+max-1)/max)
	for start := 0; start < len(values); start += max {
		end := start + max
		if end > len(values) {
			end = len(values)
		}
		chunks = append(chunks, values[start:end])
	}
	return chunks
}

func listBatchQueriesSaved(parentCount, chunkCount int) int64 {
	// For list batches, compare per-parent queries to chunked queries (1 per chunk).
	if parentCount <= 0 || chunkCount <= 0 {
		return 0
	}
	if saved := parentCount - chunkCount; saved > 0 {
		return int64(saved)
	}
	return 0
}

func columnsKey(columns []introspection.Column) string {
	if len(columns) == 0 {
		return ""
	}
	names := make([]string, len(columns))
	for i, col := range columns {
		names[i] = col.Name
	}
	sort.Strings(names)
	return strings.Join(names, ",")
}

func orderByKey(orderBy *planner.OrderBy) string {
	if orderBy == nil {
		return ""
	}
	return strings.Join(orderBy.Columns, ",") + ":" + orderBy.Direction
}

func aggregateColumnsKey(columns []planner.AggregateColumn) string {
	if len(columns) == 0 {
		return ""
	}
	parts := make([]string, len(columns))
	for i, col := range columns {
		parts[i] = col.SQLClause
	}
	return strings.Join(parts, "|")
}

func firstGroupedRecord(grouped map[string][]map[string]interface{}, key interface{}) map[string]interface{} {
	if grouped == nil {
		return nil
	}
	rows := grouped[fmt.Sprint(key)]
	if len(rows) == 0 {
		return nil
	}
	return rows[0]
}

const batchParentKeyField = "__batch_parent_key"

var batchMaxInClause = 1000

const (
	relationOneToMany  = "one_to_many"
	relationManyToOne  = "many_to_one"
	relationManyToMany = "many_to_many"
	relationEdgeList   = "edge_list"
	relationAggregate  = "aggregate"
)

func parentKeyFromResolve(p graphql.ResolveParams) string {
	return fmt.Sprintf("%s|%s|%s", responsePathString(p.Info.Path), fieldNameWithAlias(p.Info.FieldASTs), stableArgsKey(p.Args))
}

func parentKeyFromSource(source interface{}) (string, bool) {
	row, ok := source.(map[string]interface{})
	if !ok {
		return "", false
	}
	key, ok := row[batchParentKeyField].(string)
	return key, ok
}

func graphQLMetricsFromContext(ctx context.Context) *observability.GraphQLMetrics {
	return observability.GraphQLMetricsFromContext(ctx)
}

func fieldNameWithAlias(fields []*ast.Field) string {
	if len(fields) == 0 || fields[0] == nil {
		return ""
	}
	if fields[0].Alias != nil {
		return fields[0].Alias.Value
	}
	return fields[0].Name.Value
}

func stableArgsKey(args map[string]interface{}) string {
	if len(args) == 0 {
		return ""
	}
	return encodeCanonicalValue(args)
}

func responsePathString(path *graphql.ResponsePath) string {
	if path == nil {
		return ""
	}
	parts := make([]string, 0, 4)
	for current := path; current != nil; current = current.Prev {
		parts = append(parts, fmt.Sprint(current.Key))
	}
	for i, j := 0, len(parts)-1; i < j; i, j = i+1, j-1 {
		parts[i], parts[j] = parts[j], parts[i]
	}
	return strings.Join(parts, ".")
}

func encodeCanonicalValue(value interface{}) string {
	switch v := value.(type) {
	case nil:
		return "null"
	case string:
		return strconv.Quote(v)
	case bool:
		return strconv.FormatBool(v)
	case int:
		return strconv.Itoa(v)
	case int8:
		return strconv.FormatInt(int64(v), 10)
	case int16:
		return strconv.FormatInt(int64(v), 10)
	case int32:
		return strconv.FormatInt(int64(v), 10)
	case int64:
		return strconv.FormatInt(v, 10)
	case uint:
		return strconv.FormatUint(uint64(v), 10)
	case uint8:
		return strconv.FormatUint(uint64(v), 10)
	case uint16:
		return strconv.FormatUint(uint64(v), 10)
	case uint32:
		return strconv.FormatUint(uint64(v), 10)
	case uint64:
		return strconv.FormatUint(v, 10)
	case float32:
		return strconv.FormatFloat(float64(v), 'g', -1, 32)
	case float64:
		return strconv.FormatFloat(v, 'g', -1, 64)
	case []interface{}:
		return encodeCanonicalSlice(v)
	case []string:
		items := make([]interface{}, len(v))
		for i, item := range v {
			items[i] = item
		}
		return encodeCanonicalSlice(items)
	case []int:
		items := make([]interface{}, len(v))
		for i, item := range v {
			items[i] = item
		}
		return encodeCanonicalSlice(items)
	case []bool:
		items := make([]interface{}, len(v))
		for i, item := range v {
			items[i] = item
		}
		return encodeCanonicalSlice(items)
	case map[string]interface{}:
		return encodeCanonicalMap(v)
	default:
		return fmt.Sprintf("%v", v)
	}
}

func encodeCanonicalSlice(values []interface{}) string {
	if len(values) == 0 {
		return "[]"
	}
	parts := make([]string, len(values))
	for i, item := range values {
		parts[i] = encodeCanonicalValue(item)
	}
	return "[" + strings.Join(parts, ",") + "]"
}

func encodeCanonicalMap(values map[string]interface{}) string {
	if len(values) == 0 {
		return "{}"
	}
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	parts := make([]string, len(keys))
	for i, key := range keys {
		parts[i] = key + ":" + encodeCanonicalValue(values[key])
	}
	return "{" + strings.Join(parts, ",") + "}"
}

var errAccessDenied = errors.New("access denied")

// MySQL/TiDB error codes for access control violations.
// See: https://dev.mysql.com/doc/mysql-errors/8.0/en/server-error-reference.html
const (
	mysqlErrDBAccessDenied     = 1044 // Access denied for user to database
	mysqlErrTableAccessDenied  = 1142 // SELECT command denied to user for table
	mysqlErrColumnAccessDenied = 1143 // SELECT command denied to user for column
)

func normalizeQueryError(err error) error {
	if err == nil {
		return nil
	}
	var mysqlErr *mysql.MySQLError
	if errors.As(err, &mysqlErr) {
		switch mysqlErr.Number {
		case mysqlErrDBAccessDenied, mysqlErrTableAccessDenied, mysqlErrColumnAccessDenied:
			return errAccessDenied
		}
	}
	return err
}

func scanRows(rows dbexec.Rows, columns []introspection.Column) ([]map[string]interface{}, error) {
	results := make([]map[string]interface{}, 0)

	for rows.Next() {
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))

		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, err
		}

		row := make(map[string]interface{})
		for i, col := range columns {
			fieldName := introspection.GraphQLFieldName(col)
			row[fieldName] = convertValue(values[i])
		}

		results = append(results, row)
	}

	return results, rows.Err()
}

func scanRowsWithExtras(rows dbexec.Rows, columns []introspection.Column, extras []string) ([]map[string]interface{}, error) {
	results := make([]map[string]interface{}, 0)

	totalColumns := len(columns) + len(extras)
	for rows.Next() {
		values := make([]interface{}, totalColumns)
		valuePtrs := make([]interface{}, totalColumns)

		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, err
		}

		row := make(map[string]interface{}, totalColumns)
		for i, col := range columns {
			fieldName := introspection.GraphQLFieldName(col)
			row[fieldName] = convertValue(values[i])
		}
		for i, name := range extras {
			row[name] = convertValue(values[len(columns)+i])
		}

		results = append(results, row)
	}

	return results, rows.Err()
}

func ensureNonNullRows(rows []map[string]interface{}) []map[string]interface{} {
	if rows == nil {
		return []map[string]interface{}{}
	}
	return rows
}

func graphQLFieldNameForColumn(table introspection.Table, columnName string) string {
	for _, col := range table.Columns {
		if col.Name == columnName {
			return introspection.GraphQLFieldName(col)
		}
	}
	return introspection.ToGraphQLFieldName(columnName)
}

func columnsForPlan(plan *planner.Plan) []introspection.Column {
	if plan == nil {
		return nil
	}
	if len(plan.Columns) > 0 {
		return plan.Columns
	}
	return plan.Table.Columns
}

func convertValue(val interface{}) interface{} {
	if val == nil {
		return nil
	}

	// Convert []byte to string
	if b, ok := val.([]byte); ok {
		return string(b)
	}

	return val
}

func (r *Resolver) mapColumnTypeToGraphQL(table introspection.Table, col *introspection.Column) graphql.Output {
	if enumType := r.enumTypeForColumn(table, col); enumType != nil {
		return enumType
	}
	switch sqltype.MapToGraphQL(col.DataType) {
	case sqltype.TypeJSON:
		return r.jsonScalar()
	case sqltype.TypeInt:
		return graphql.Int
	case sqltype.TypeFloat:
		return graphql.Float
	case sqltype.TypeBoolean:
		return graphql.Boolean
	case sqltype.TypeDate:
		return r.dateScalar()
	case sqltype.TypeDateTime:
		return graphql.DateTime
	default:
		return graphql.String
	}
}

func (r *Resolver) mapColumnTypeToGraphQLInput(table introspection.Table, col *introspection.Column) graphql.Input {
	if enumType := r.enumTypeForColumn(table, col); enumType != nil {
		return enumType
	}
	switch sqltype.MapToGraphQL(col.DataType) {
	case sqltype.TypeJSON:
		return r.jsonScalar()
	case sqltype.TypeInt:
		return graphql.Int
	case sqltype.TypeFloat:
		return graphql.Float
	case sqltype.TypeBoolean:
		return graphql.Boolean
	case sqltype.TypeDate:
		return r.dateScalar()
	case sqltype.TypeDateTime:
		return graphql.DateTime
	default:
		return graphql.String
	}
}

// makeManyToOneResolver creates a resolver for many-to-one relationships (e.g., verses.chapter)
func (r *Resolver) makeManyToOneResolver(table introspection.Table, rel introspection.Relationship) graphql.FieldResolveFn {
	return func(p graphql.ResolveParams) (interface{}, error) {
		// Get the foreign key value from parent object
		source, ok := p.Source.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("invalid source type")
		}

		// Get FK value (e.g., chapterId from verse)
		fkFieldName := graphQLFieldNameForColumn(table, rel.LocalColumn)
		fkValue := source[fkFieldName]

		if fkValue == nil {
			return nil, nil // Nullable FK
		}

		if result, ok, err := r.tryBatchManyToOne(p, table, rel, fkValue); ok || err != nil {
			return result, err
		}

		// Query the related table
		relatedTable, err := r.findTable(rel.RemoteTable)
		if err != nil {
			return nil, fmt.Errorf("failed to find related table %s: %w", rel.RemoteTable, err)
		}
		field := firstFieldAST(p.Info.FieldASTs)
		if field == nil {
			return nil, fmt.Errorf("missing field AST")
		}
		planned, err := planner.PlanQuery(r.dbSchema, field, p.Args, planner.WithFragments(p.Info.Fragments), planner.WithDefaultListLimit(r.defaultLimit), planner.WithRelationship(planner.RelationshipContext{
			RelatedTable: relatedTable,
			RemoteColumn: rel.RemoteColumn,
			Value:        fkValue,
			IsManyToOne:  true,
		}))
		if err != nil {
			return nil, fmt.Errorf("failed to build query: %w", err)
		}

		rows, err := r.executor.QueryContext(p.Context, planned.Root.SQL, planned.Root.Args...)
		if err != nil {
			return nil, normalizeQueryError(err)
		}
		defer func() {
			_ = rows.Close()
		}()

		results, err := scanRows(rows, columnsForPlan(planned))
		if err != nil {
			return nil, err
		}

		if len(results) == 0 {
			return nil, nil
		}

		return results[0], nil
	}
}

// makeOneToManyResolver creates a resolver for one-to-many relationships (e.g., chapter.verses)
func (r *Resolver) makeOneToManyResolver(table introspection.Table, rel introspection.Relationship) graphql.FieldResolveFn {
	return func(p graphql.ResolveParams) (interface{}, error) {
		// Get the primary key value from parent object
		source, ok := p.Source.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("invalid source type")
		}

		// Get PK value (e.g., id from chapter)
		pkFieldName := graphQLFieldNameForColumn(table, rel.LocalColumn)
		pkValue := source[pkFieldName]

		if pkValue == nil {
			return []map[string]interface{}{}, nil
		}

		if results, ok, err := r.tryBatchOneToMany(p, table, rel, pkValue); ok || err != nil {
			if err != nil {
				return nil, err
			}
			results = ensureNonNullRows(results)
			seedBatchRows(p, results)
			return results, nil
		}

		// Query the related table
		relatedTable, err := r.findTable(rel.RemoteTable)
		if err != nil {
			return nil, fmt.Errorf("failed to find related table %s: %w", rel.RemoteTable, err)
		}
		field := firstFieldAST(p.Info.FieldASTs)
		if field == nil {
			return nil, fmt.Errorf("missing field AST")
		}
		planned, err := planner.PlanQuery(r.dbSchema, field, p.Args, planner.WithFragments(p.Info.Fragments), planner.WithDefaultListLimit(r.defaultLimit), planner.WithRelationship(planner.RelationshipContext{
			RelatedTable: relatedTable,
			RemoteColumn: rel.RemoteColumn,
			Value:        pkValue,
			IsOneToMany:  true,
		}))
		if err != nil {
			return nil, fmt.Errorf("failed to build query: %w", err)
		}

		rows, err := r.executor.QueryContext(p.Context, planned.Root.SQL, planned.Root.Args...)
		if err != nil {
			return nil, normalizeQueryError(err)
		}
		defer func() {
			_ = rows.Close()
		}()

		results, err := scanRows(rows, columnsForPlan(planned))
		if err != nil {
			return nil, err
		}

		results = ensureNonNullRows(results)
		seedBatchRows(p, results)
		return results, nil
	}
}

// makeManyToManyResolver creates a resolver for direct M2M relationships through pure junctions.
// It queries the target table by joining through the junction table.
func (r *Resolver) makeManyToManyResolver(table introspection.Table, rel introspection.Relationship) graphql.FieldResolveFn {
	return func(p graphql.ResolveParams) (interface{}, error) {
		source, ok := p.Source.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("invalid source type")
		}

		// Get PK value from parent
		pkFieldName := graphQLFieldNameForColumn(table, rel.LocalColumn)
		pkValue := source[pkFieldName]

		if pkValue == nil {
			return []map[string]interface{}{}, nil
		}

		if results, ok, err := r.tryBatchManyToMany(p, table, rel, pkValue); ok || err != nil {
			if err != nil {
				return nil, err
			}
			results = ensureNonNullRows(results)
			seedBatchRows(p, results)
			return results, nil
		}

		// Find target table
		targetTable, err := r.findTable(rel.RemoteTable)
		if err != nil {
			return nil, fmt.Errorf("failed to find target table %s: %w", rel.RemoteTable, err)
		}

		field := firstFieldAST(p.Info.FieldASTs)
		if field == nil {
			return nil, fmt.Errorf("missing field AST")
		}
		selection := planner.SelectedColumns(targetTable, field, p.Info.Fragments)

		orderBy, err := planner.ParseOrderBy(targetTable, p.Args)
		if err != nil {
			return nil, err
		}

		// Get limit/offset from args
		limit := r.defaultLimit
		if v, ok := p.Args["limit"].(int); ok {
			limit = v
		}
		offset := 0
		if v, ok := p.Args["offset"].(int); ok {
			offset = v
		}

		// Build M2M query using junction table
		planned, err := planner.PlanManyToMany(
			rel.JunctionTable,
			targetTable,
			rel.JunctionLocalFK,
			rel.JunctionRemoteFK,
			rel.RemoteColumn,
			selection,
			pkValue,
			limit,
			offset,
			orderBy,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to plan M2M query: %w", err)
		}

		rows, err := r.executor.QueryContext(p.Context, planned.SQL, planned.Args...)
		if err != nil {
			return nil, normalizeQueryError(err)
		}
		defer func() {
			_ = rows.Close()
		}()

		results, err := scanRows(rows, selection)
		if err != nil {
			return nil, err
		}

		results = ensureNonNullRows(results)
		seedBatchRows(p, results)
		return results, nil
	}
}

// makeEdgeListResolver creates a resolver for edge list access through attribute junctions.
// It returns the junction table rows (edge objects) which include both FK references and attribute columns.
func (r *Resolver) makeEdgeListResolver(table introspection.Table, rel introspection.Relationship) graphql.FieldResolveFn {
	return func(p graphql.ResolveParams) (interface{}, error) {
		source, ok := p.Source.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("invalid source type")
		}

		// Get PK value from parent
		pkFieldName := graphQLFieldNameForColumn(table, rel.LocalColumn)
		pkValue := source[pkFieldName]

		if pkValue == nil {
			return []map[string]interface{}{}, nil
		}

		if results, ok, err := r.tryBatchEdgeList(p, table, rel, pkValue); ok || err != nil {
			if err != nil {
				return nil, err
			}
			results = ensureNonNullRows(results)
			seedBatchRows(p, results)
			return results, nil
		}

		// Find junction table
		junctionTable, err := r.findTable(rel.JunctionTable)
		if err != nil {
			return nil, fmt.Errorf("failed to find junction table %s: %w", rel.JunctionTable, err)
		}

		field := firstFieldAST(p.Info.FieldASTs)
		if field == nil {
			return nil, fmt.Errorf("missing field AST")
		}
		selection := planner.SelectedColumns(junctionTable, field, p.Info.Fragments)

		orderBy, err := planner.ParseOrderBy(junctionTable, p.Args)
		if err != nil {
			return nil, err
		}

		// Get limit/offset from args
		limit := r.defaultLimit
		if v, ok := p.Args["limit"].(int); ok {
			limit = v
		}
		offset := 0
		if v, ok := p.Args["offset"].(int); ok {
			offset = v
		}

		// Build edge list query - simple one-to-many through junction FK
		planned, err := planner.PlanEdgeList(
			junctionTable,
			rel.JunctionLocalFK,
			selection,
			pkValue,
			limit,
			offset,
			orderBy,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to plan edge list query: %w", err)
		}

		rows, err := r.executor.QueryContext(p.Context, planned.SQL, planned.Args...)
		if err != nil {
			return nil, normalizeQueryError(err)
		}
		defer func() {
			_ = rows.Close()
		}()

		results, err := scanRows(rows, selection)
		if err != nil {
			return nil, err
		}

		results = ensureNonNullRows(results)
		seedBatchRows(p, results)
		return results, nil
	}
}

// makeAggregateResolver creates a resolver for root-level aggregate queries.
func (r *Resolver) makeAggregateResolver(table introspection.Table) graphql.FieldResolveFn {
	return func(p graphql.ResolveParams) (interface{}, error) {
		field := firstFieldAST(p.Info.FieldASTs)
		if field == nil {
			return nil, fmt.Errorf("missing field AST")
		}

		// Parse aggregate selection from GraphQL query
		selection := planner.ParseAggregateSelection(table, field, p.Info.Fragments)
		columns := planner.BuildAggregateColumns(selection)

		filters, err := r.aggregateFiltersFromArgs(table, p.Args)
		if err != nil {
			return nil, err
		}

		// Plan and execute aggregate query
		planned, err := planner.PlanAggregate(table, selection, filters)
		if err != nil {
			return nil, fmt.Errorf("failed to plan aggregate: %w", err)
		}

		rows, err := r.executor.QueryContext(p.Context, planned.SQL, planned.Args...)
		if err != nil {
			return nil, normalizeQueryError(err)
		}
		defer func() {
			_ = rows.Close()
		}()

		// Scan aggregate results using columns list for correct ordering
		aggregateResult, err := scanAggregateRow(rows, columns, table)
		if err != nil {
			return nil, err
		}

		return aggregateResult, nil
	}
}

// makeRelationshipAggregateResolver creates a resolver for aggregate queries on relationships.
func (r *Resolver) makeRelationshipAggregateResolver(table introspection.Table, rel introspection.Relationship) graphql.FieldResolveFn {
	return func(p graphql.ResolveParams) (interface{}, error) {
		source, ok := p.Source.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("invalid source type")
		}

		// Get parent key value
		pkFieldName := graphQLFieldNameForColumn(table, rel.LocalColumn)
		pkValue := source[pkFieldName]
		if pkValue == nil {
			return map[string]interface{}{"count": int64(0)}, nil
		}

		relatedTable, err := r.findTable(rel.RemoteTable)
		if err != nil {
			return nil, err
		}

		field := firstFieldAST(p.Info.FieldASTs)
		if field == nil {
			return nil, fmt.Errorf("missing field AST")
		}

		selection := planner.ParseAggregateSelection(relatedTable, field, p.Info.Fragments)
		columns := planner.BuildAggregateColumns(selection)

		filters, err := r.aggregateFiltersFromArgs(relatedTable, p.Args, rel.RemoteColumn)
		if err != nil {
			return nil, err
		}

		if result, ok, err := r.tryBatchRelationshipAggregate(p, table, rel, pkValue, selection, filters); ok || err != nil {
			return result, err
		}

		// Execute single relationship aggregate query
		planned, err := planner.PlanRelationshipAggregate(relatedTable, selection, rel.RemoteColumn, pkValue, filters)
		if err != nil {
			return nil, err
		}

		rows, err := r.executor.QueryContext(p.Context, planned.SQL, planned.Args...)
		if err != nil {
			return nil, normalizeQueryError(err)
		}
		defer func() {
			_ = rows.Close()
		}()

		// Scan aggregate results using columns list for correct ordering
		aggregateResult, err := scanAggregateRow(rows, columns, relatedTable)
		if err != nil {
			return nil, err
		}

		return aggregateResult, nil
	}
}

// scanAggregateRow scans a single row of aggregate results into a map.
// It uses the columns list from BuildAggregateColumns to ensure scan order
// matches SQL SELECT clause order exactly.
func scanAggregateRow(rows dbexec.Rows, columns []planner.AggregateColumn, table introspection.Table) (map[string]interface{}, error) {
	if !rows.Next() {
		// No rows means count is 0
		return map[string]interface{}{"count": int64(0)}, rows.Err()
	}

	// Build scan destinations in the SAME order as columns (which matches SQL SELECT order).
	// This ordering is guaranteed by using BuildAggregateColumns as the single source of truth.
	scanDests := make([]interface{}, len(columns))
	intValues := make([]sql.NullInt64, len(columns))
	floatValues := make([]sql.NullFloat64, len(columns))
	anyValues := make([]interface{}, len(columns))

	for i, col := range columns {
		switch col.ValueType {
		case planner.AggregateInt:
			scanDests[i] = &intValues[i]
		case planner.AggregateFloat:
			scanDests[i] = &floatValues[i]
		case planner.AggregateAny:
			scanDests[i] = &anyValues[i]
		}
	}

	if err := rows.Scan(scanDests...); err != nil {
		return nil, err
	}

	result := buildAggregateResult(columns, table, intValues, floatValues, anyValues)
	return result, rows.Err()
}

func scanAggregateRows(rows dbexec.Rows, columns []planner.AggregateColumn, table introspection.Table) (map[string]map[string]interface{}, error) {
	grouped := make(map[string]map[string]interface{})

	for rows.Next() {
		var groupKey interface{}

		scanDests := make([]interface{}, len(columns)+1)
		intValues := make([]sql.NullInt64, len(columns))
		floatValues := make([]sql.NullFloat64, len(columns))
		anyValues := make([]interface{}, len(columns))

		scanDests[0] = &groupKey
		for i, col := range columns {
			switch col.ValueType {
			case planner.AggregateInt:
				scanDests[i+1] = &intValues[i]
			case planner.AggregateFloat:
				scanDests[i+1] = &floatValues[i]
			case planner.AggregateAny:
				scanDests[i+1] = &anyValues[i]
			}
		}

		if err := rows.Scan(scanDests...); err != nil {
			return nil, err
		}

		grouped[fmt.Sprint(groupKey)] = buildAggregateResult(columns, table, intValues, floatValues, anyValues)
	}

	return grouped, rows.Err()
}

func buildAggregateResult(columns []planner.AggregateColumn, table introspection.Table, intValues []sql.NullInt64, floatValues []sql.NullFloat64, anyValues []interface{}) map[string]interface{} {
	result := map[string]interface{}{}
	groupedResults := map[string]map[string]interface{}{}

	for i, col := range columns {
		var value interface{}
		var hasValue bool

		switch col.ValueType {
		case planner.AggregateInt:
			if intValues[i].Valid {
				value = intValues[i].Int64
				hasValue = true
			}
		case planner.AggregateFloat:
			if floatValues[i].Valid {
				value = floatValues[i].Float64
				hasValue = true
			}
		case planner.AggregateAny:
			value = convertValue(anyValues[i])
			hasValue = true
		}

		// Handle plain count specially (no column name, goes directly in result)
		if col.ResultKey == "count" && col.ColumnName == "" {
			if hasValue {
				result["count"] = value
			} else {
				result["count"] = int64(0)
			}
			continue
		}

		if hasValue {
			if groupedResults[col.ResultKey] == nil {
				groupedResults[col.ResultKey] = map[string]interface{}{}
			}
			fieldName := graphQLFieldNameForColumn(table, col.ColumnName)
			groupedResults[col.ResultKey][fieldName] = value
		}
	}

	for key, values := range groupedResults {
		if len(values) > 0 {
			result[key] = values
		}
	}

	if _, ok := result["count"]; !ok {
		result["count"] = int64(0)
	}

	return result
}
