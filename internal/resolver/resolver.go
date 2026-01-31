// Package resolver builds and executes GraphQL schemas from database introspection.
// It dynamically generates GraphQL types, queries, and resolvers based on the database schema,
// supporting filtering, ordering, pagination, and relationship resolution with N+1 query prevention.
package resolver

import (
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

	"github.com/go-sql-driver/mysql"
	"github.com/graphql-go/graphql"
	"github.com/graphql-go/graphql/language/ast"
	"tidb-graphql/internal/dbexec"
	"tidb-graphql/internal/introspection"
	"tidb-graphql/internal/naming"
	"tidb-graphql/internal/planner"
	"tidb-graphql/internal/schemafilter"
	"tidb-graphql/internal/sqltype"
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
	singularQueryCache map[string]string
	singularTypeCache  map[string]string
	singularNamer      *naming.Namer
	orderDirection     *graphql.Enum
	nonNegativeInt     *graphql.Scalar
	jsonType           *graphql.Scalar
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

	return graphql.NewSchema(schemaConfig)
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
		Type: graphql.NewList(tableType),
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
		r.addSingleRowQuery(fields, table, tableType, pkFieldName, pkCols)
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
		argType := r.mapColumnTypeToGraphQL(col)
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

	// Create type with FieldsThunk for lazy field initialization
	// This prevents circular reference issues
	objType := graphql.NewObject(graphql.ObjectConfig{
		Name: typeName,
		Fields: graphql.FieldsThunk(func() graphql.Fields {
			return r.buildFieldsForTable(table)
		}),
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
		fieldType := r.mapColumnTypeToGraphQL(&col)
		if !col.IsNullable {
			fieldType = graphql.NewNonNull(fieldType)
		}

		fields[introspection.GraphQLFieldName(col)] = &graphql.Field{
			Type: fieldType,
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
				Type: graphql.NewList(graphql.NewNonNull(relatedType)),
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
				Type: graphql.NewList(graphql.NewNonNull(relatedType)),
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
				Type: graphql.NewList(graphql.NewNonNull(edgeType)),
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
			Type: r.mapColumnTypeToGraphQL(&col),
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

		if state, ok := getBatchState(p.Context); ok {
			parentKey := parentKeyFromResolve(p)
			for _, row := range results {
				row[batchParentKeyField] = parentKey
			}
			state.setParentRows(parentKey, results)
		}

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
		filterType := r.getFilterInputType(col.DataType)
		if filterType != nil {
			fields[fieldName] = &graphql.InputObjectFieldConfig{
				Type: filterType,
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

func (r *Resolver) getFilterInputType(dataType string) *graphql.InputObject {
	filterName := sqltype.MapToGraphQL(dataType).FilterTypeName()

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
	state, ok := getBatchState(p.Context)
	if !ok {
		return nil, false, nil
	}

	parentKey, ok := parentKeyFromSource(p.Source)
	if !ok {
		return nil, false, nil
	}

	parentRows := state.getParentRows(parentKey)
	if len(parentRows) == 0 {
		return nil, false, nil
	}

	limit := planner.GetArgInt(p.Args, "limit", r.defaultLimit)
	offset := planner.GetArgInt(p.Args, "offset", 0)
	relKey := fmt.Sprintf("%s|%s|%s|%d|%d|%s", table.Name, rel.RemoteTable, rel.RemoteColumn, limit, offset, parentKey)

	if cached := state.getChildRows(relKey); cached != nil {
		state.IncrementCacheHit()
		return cached[fmt.Sprint(pkValue)], true, nil
	}
	state.IncrementCacheMiss()

	relatedTable, err := r.findTable(rel.RemoteTable)
	if err != nil {
		return nil, true, fmt.Errorf("failed to find related table %s: %w", rel.RemoteTable, err)
	}

	parentField := graphQLFieldNameForColumn(table, rel.LocalColumn)
	parentValues := uniqueParentValues(parentRows, parentField)
	if len(parentValues) == 0 {
		state.setChildRows(relKey, map[string][]map[string]interface{}{})
		return []map[string]interface{}{}, true, nil
	}

	orderBy, err := planner.ParseOrderBy(relatedTable, p.Args)
	if err != nil {
		return nil, true, err
	}

	selection := planner.SelectedColumns(relatedTable, firstFieldAST(p.Info.FieldASTs), p.Info.Fragments)
	selection = planner.EnsureColumns(relatedTable, selection, []string{rel.RemoteColumn})
	grouped := make(map[string][]map[string]interface{})
	for _, chunk := range chunkValues(parentValues, batchMaxInClause) {
		planned, err := planner.PlanOneToManyBatch(relatedTable, selection, rel.RemoteColumn, chunk, limit, offset, orderBy)
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
		results, err := scanRows(rows, selection)
		rows.Close()
		if err != nil {
			return nil, true, err
		}

		fkFieldName := graphQLFieldNameForColumn(relatedTable, rel.RemoteColumn)
		mergeGrouped(grouped, groupByField(results, fkFieldName))
	}
	if len(grouped) == 0 {
		state.setChildRows(relKey, map[string][]map[string]interface{}{})
		return []map[string]interface{}{}, true, nil
	}
	state.setChildRows(relKey, grouped)

	return grouped[fmt.Sprint(pkValue)], true, nil
}

func (r *Resolver) tryBatchManyToOne(p graphql.ResolveParams, table introspection.Table, rel introspection.Relationship, fkValue interface{}) (map[string]interface{}, bool, error) {
	state, ok := getBatchState(p.Context)
	if !ok {
		return nil, false, nil
	}

	parentKey, ok := parentKeyFromSource(p.Source)
	if !ok {
		return nil, false, nil
	}

	parentRows := state.getParentRows(parentKey)
	if len(parentRows) == 0 {
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
	selection = planner.EnsureColumns(relatedTable, selection, []string{rel.RemoteColumn})

	relKey := fmt.Sprintf("%s|%s|%s|%s", relatedTable.Name, rel.RemoteColumn, parentKey, columnsKey(selection))
	if cached := state.getChildRows(relKey); cached != nil {
		state.IncrementCacheHit()
		return firstGroupedRecord(cached, fkValue), true, nil
	}
	state.IncrementCacheMiss()

	parentField := graphQLFieldNameForColumn(table, rel.LocalColumn)
	parentValues := uniqueParentValues(parentRows, parentField)
	if len(parentValues) == 0 {
		state.setChildRows(relKey, map[string][]map[string]interface{}{})
		return nil, true, nil
	}

	grouped := make(map[string][]map[string]interface{})
	for _, chunk := range chunkValues(parentValues, batchMaxInClause) {
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
		results, err := scanRows(rows, selection)
		rows.Close()
		if err != nil {
			return nil, true, err
		}

		fkFieldName := graphQLFieldNameForColumn(relatedTable, rel.RemoteColumn)
		mergeGrouped(grouped, groupByField(results, fkFieldName))
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

	keys := make([]string, 0, len(args))
	for key := range args {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	parts := make([]string, len(keys))
	for i, key := range keys {
		parts[i] = fmt.Sprintf("%s=%v", key, args[key])
	}
	return strings.Join(parts, ",")
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
	var results []map[string]interface{}

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

func (r *Resolver) mapColumnTypeToGraphQL(col *introspection.Column) graphql.Output {
	switch sqltype.MapToGraphQL(col.DataType) {
	case sqltype.TypeJSON:
		return r.jsonScalar()
	case sqltype.TypeInt:
		return graphql.Int
	case sqltype.TypeFloat:
		return graphql.Float
	case sqltype.TypeBoolean:
		return graphql.Boolean
	default:
		return graphql.String
	}
}

func (r *Resolver) mapColumnTypeToGraphQLInput(col *introspection.Column) graphql.Input {
	switch sqltype.MapToGraphQL(col.DataType) {
	case sqltype.TypeJSON:
		return r.jsonScalar()
	case sqltype.TypeInt:
		return graphql.Int
	case sqltype.TypeFloat:
		return graphql.Float
	case sqltype.TypeBoolean:
		return graphql.Boolean
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
			return []interface{}{}, nil
		}

		if results, ok, err := r.tryBatchOneToMany(p, table, rel, pkValue); ok || err != nil {
			return results, err
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
			return []interface{}{}, nil
		}

		// Find target table
		targetTable, err := r.findTable(rel.RemoteTable)
		if err != nil {
			return nil, fmt.Errorf("failed to find target table %s: %w", rel.RemoteTable, err)
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
			pkValue,
			limit,
			offset,
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

		results, err := scanRows(rows, targetTable.Columns)
		if err != nil {
			return nil, err
		}

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
			return []interface{}{}, nil
		}

		// Find junction table
		junctionTable, err := r.findTable(rel.JunctionTable)
		if err != nil {
			return nil, fmt.Errorf("failed to find junction table %s: %w", rel.JunctionTable, err)
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
			pkValue,
			limit,
			offset,
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

		results, err := scanRows(rows, junctionTable.Columns)
		if err != nil {
			return nil, err
		}

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
	result := map[string]interface{}{}

	if !rows.Next() {
		// No rows means count is 0
		result["count"] = int64(0)
		return result, rows.Err()
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

	// Build result map, grouping columns by their ResultKey
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

		// For column-specific aggregates, group by ResultKey
		if hasValue {
			if groupedResults[col.ResultKey] == nil {
				groupedResults[col.ResultKey] = map[string]interface{}{}
			}
			fieldName := graphQLFieldNameForColumn(table, col.ColumnName)
			groupedResults[col.ResultKey][fieldName] = value
		}
	}

	// Add grouped results to main result
	for key, values := range groupedResults {
		if len(values) > 0 {
			result[key] = values
		}
	}

	return result, rows.Err()
}
