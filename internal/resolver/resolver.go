// Package resolver builds and executes GraphQL schemas from database introspection.
// It dynamically generates GraphQL types, queries, and resolvers based on the database schema,
// supporting filtering, ordering, pagination, and relationship resolution with N+1 query prevention.
package resolver

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"sync"

	"tidb-graphql/internal/cursor"
	"tidb-graphql/internal/dbexec"
	"tidb-graphql/internal/introspection"
	"tidb-graphql/internal/naming"
	"tidb-graphql/internal/nodeid"
	"tidb-graphql/internal/observability"
	"tidb-graphql/internal/planner"
	"tidb-graphql/internal/scalars"
	"tidb-graphql/internal/schemafilter"
	"tidb-graphql/internal/schemanaming"
	"tidb-graphql/internal/sqltype"
	"tidb-graphql/internal/uuidutil"

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
	orderByClauseCache map[string]*graphql.InputObject
	whereCache         map[string]*graphql.InputObject
	filterCache        map[string]*graphql.InputObject
	aggregateCache     map[string]*graphql.Object // Cache for aggregate types (XxxAggregate, XxxAvgFields, etc.)
	createInputCache   map[string]*graphql.InputObject
	updateInputCache   map[string]*graphql.InputObject
	deletePayloadCache map[string]*graphql.Object
	enumCache          map[string]*graphql.Enum
	enumFilterCache    map[string]*graphql.InputObject
	setFilterCache     map[string]*graphql.InputObject
	vectorEdgeCache    map[string]*graphql.Object
	vectorConnCache    map[string]*graphql.Object
	singularQueryCache map[string]string
	singularTypeCache  map[string]string
	singularNamer      *naming.Namer
	orderDirection     *graphql.Enum
	orderByPolicy      *graphql.Enum
	nonNegativeInt     *graphql.Scalar
	bigIntType         *graphql.Scalar
	decimalType        *graphql.Scalar
	jsonType           *graphql.Scalar
	dateType           *graphql.Scalar
	timeType           *graphql.Scalar
	yearType           *graphql.Scalar
	bytesType          *graphql.Scalar
	uuidType           *graphql.Scalar
	vectorType         *graphql.Scalar
	nodeInterface      *graphql.Interface
	pageInfoType       *graphql.Object
	vectorDistance     *graphql.Enum
	edgeCache          map[string]*graphql.Object
	connectionCache    map[string]*graphql.Object
	limits             *planner.PlanLimits
	defaultLimit       int
	filters            schemafilter.Config
	vectorSearch       VectorSearchConfig
	mu                 sync.RWMutex
}

// VectorSearchConfig controls generated vector-search fields.
type VectorSearchConfig struct {
	RequireIndex bool
	MaxTopK      int
	DefaultFirst int
}

func normalizeVectorSearchConfig(cfg VectorSearchConfig) VectorSearchConfig {
	if cfg.MaxTopK <= 0 {
		cfg.MaxTopK = 100
	}
	if cfg.DefaultFirst <= 0 {
		cfg.DefaultFirst = planner.DefaultVectorFirst
	}
	if cfg.DefaultFirst > cfg.MaxTopK {
		cfg.DefaultFirst = cfg.MaxTopK
	}
	return cfg
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
		orderByClauseCache: make(map[string]*graphql.InputObject),
		whereCache:         make(map[string]*graphql.InputObject),
		filterCache:        make(map[string]*graphql.InputObject),
		aggregateCache:     make(map[string]*graphql.Object),
		createInputCache:   make(map[string]*graphql.InputObject),
		updateInputCache:   make(map[string]*graphql.InputObject),
		deletePayloadCache: make(map[string]*graphql.Object),
		enumCache:          make(map[string]*graphql.Enum),
		enumFilterCache:    make(map[string]*graphql.InputObject),
		setFilterCache:     make(map[string]*graphql.InputObject),
		vectorEdgeCache:    make(map[string]*graphql.Object),
		vectorConnCache:    make(map[string]*graphql.Object),
		singularQueryCache: make(map[string]string),
		singularTypeCache:  make(map[string]string),
		singularNamer:      naming.New(namingConfig, nil),
		edgeCache:          make(map[string]*graphql.Object),
		connectionCache:    make(map[string]*graphql.Object),
		limits:             limits,
		defaultLimit:       defaultLimit,
		filters:            filters,
		vectorSearch: normalizeVectorSearchConfig(VectorSearchConfig{
			RequireIndex: true,
		}),
	}
}

// SetVectorSearchConfig overrides vector-search generation/runtime behavior.
func (r *Resolver) SetVectorSearchConfig(cfg VectorSearchConfig) {
	r.mu.Lock()
	r.vectorSearch = normalizeVectorSearchConfig(cfg)
	r.mu.Unlock()
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
		connArgs := graphql.FieldConfigArgument{
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
			connArgs["orderBy"] = &graphql.ArgumentConfig{
				Type: orderByArgType,
			}
			connArgs["orderByPolicy"] = &graphql.ArgumentConfig{
				Type: r.orderByPolicyEnum(),
			}
		}
		if whereInput := r.whereInput(table); whereInput != nil {
			connArgs["where"] = &graphql.ArgumentConfig{
				Type: whereInput,
			}
		}
		fields[fieldName] = &graphql.Field{
			Type:    graphql.NewNonNull(connectionType),
			Args:    connArgs,
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
				connArgs := graphql.FieldConfigArgument{
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
				if orderByArgType := r.orderByArgType(relatedTable); orderByArgType != nil {
					connArgs["orderBy"] = &graphql.ArgumentConfig{
						Type: orderByArgType,
					}
					connArgs["orderByPolicy"] = &graphql.ArgumentConfig{
						Type: r.orderByPolicyEnum(),
					}
				}
				if whereInput := r.whereInput(relatedTable); whereInput != nil {
					connArgs["where"] = &graphql.ArgumentConfig{
						Type: whereInput,
					}
				}
				addRelConnectionField(fields, rel.GraphQLFieldName, connType, connArgs, r.makeOneToManyConnectionResolver(table, rel))
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
				connArgs := graphql.FieldConfigArgument{
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
				if orderByArgType := r.orderByArgType(relatedTable); orderByArgType != nil {
					connArgs["orderBy"] = &graphql.ArgumentConfig{
						Type: orderByArgType,
					}
					connArgs["orderByPolicy"] = &graphql.ArgumentConfig{
						Type: r.orderByPolicyEnum(),
					}
				}
				if whereInput := r.whereInput(relatedTable); whereInput != nil {
					connArgs["where"] = &graphql.ArgumentConfig{
						Type: whereInput,
					}
				}
				addRelConnectionField(fields, rel.GraphQLFieldName, connType, connArgs, r.makeManyToManyConnectionResolver(table, rel))
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
				connArgs := graphql.FieldConfigArgument{
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
				if orderByArgType := r.orderByArgType(junctionTable); orderByArgType != nil {
					connArgs["orderBy"] = &graphql.ArgumentConfig{
						Type: orderByArgType,
					}
					connArgs["orderByPolicy"] = &graphql.ArgumentConfig{
						Type: r.orderByPolicyEnum(),
					}
				}
				if whereInput := r.whereInput(junctionTable); whereInput != nil {
					connArgs["where"] = &graphql.ArgumentConfig{
						Type: whereInput,
					}
				}
				addRelConnectionField(fields, rel.GraphQLFieldName, connType, connArgs, r.makeEdgeListConnectionResolver(table, rel))
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

		// Peek at type name to find the table dynamically
		typeName, values, err := nodeid.Decode(id)
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

		pkValues, err := r.pkValuesFromDecodedNodeID(table, pkCols, typeName, values)
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
	decodedType, decodedValues, err := nodeid.Decode(id)
	if err != nil {
		return nil, err
	}
	return r.pkValuesFromDecodedNodeID(table, pkCols, decodedType, decodedValues)
}

func (r *Resolver) pkValuesFromDecodedNodeID(table introspection.Table, pkCols []introspection.Column, decodedType string, decodedValues []interface{}) (map[string]interface{}, error) {
	expectedType := introspection.GraphQLTypeName(table)
	if decodedType != expectedType {
		return nil, fmt.Errorf("invalid id for %s", expectedType)
	}
	if len(decodedValues) != len(pkCols) {
		return nil, fmt.Errorf("invalid id for %s", expectedType)
	}
	pkValues := make(map[string]interface{}, len(pkCols))
	for i, col := range pkCols {
		parsed, err := nodeid.ParsePKValue(col, decodedValues[i])
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
	// Snapshot the cache under a single lock to avoid key/value drift between
	// separate read passes.
	snapshot := make(map[string]*graphql.Object, len(r.typeCache))
	keys := make([]string, 0, len(r.typeCache))
	for key, objType := range r.typeCache {
		snapshot[key] = objType
		keys = append(keys, key)
	}
	r.mu.RUnlock()

	sort.Strings(keys)

	types := make([]graphql.Type, 0, len(keys))
	for _, key := range keys {
		if objType, ok := snapshot[key]; ok {
			types = append(types, objType)
		}
	}

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

func firstFieldAST(fields []*ast.Field) *ast.Field {
	if len(fields) == 0 {
		return nil
	}
	return fields[0]
}

func (r *Resolver) orderByArgType(table introspection.Table) graphql.Input {
	clauseInput := r.orderByClauseInput(table)
	if clauseInput == nil {
		return nil
	}
	return graphql.NewList(graphql.NewNonNull(clauseInput))
}

func (r *Resolver) orderByClauseInput(table introspection.Table) *graphql.InputObject {
	fields := planner.OrderByIndexedFields(table)
	if len(fields) == 0 {
		return nil
	}
	typeName := introspection.GraphQLTypeName(table) + "OrderByClauseInput"
	r.mu.RLock()
	cached, ok := r.orderByClauseCache[typeName]
	r.mu.RUnlock()
	if ok {
		return cached
	}

	orderDirection := r.orderDirectionEnum()
	clauseFields := graphql.InputObjectConfigFieldMap{}
	for _, name := range sortedOrderByFieldNames(fields) {
		clauseFields[name] = &graphql.InputObjectFieldConfig{
			Type: orderDirection,
		}
	}

	input := graphql.NewInputObject(graphql.InputObjectConfig{
		Name:   typeName,
		Fields: clauseFields,
	})
	r.mu.Lock()
	if cached, ok := r.orderByClauseCache[typeName]; ok {
		r.mu.Unlock()
		return cached
	}
	r.orderByClauseCache[typeName] = input
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

func (r *Resolver) orderByPolicyEnum() *graphql.Enum {
	r.mu.RLock()
	cached := r.orderByPolicy
	r.mu.RUnlock()
	if cached != nil {
		return cached
	}

	enumValue := graphql.NewEnum(graphql.EnumConfig{
		Name: "OrderByPolicy",
		Values: graphql.EnumValueConfigMap{
			string(planner.OrderByPolicyIndexPrefixOnly): &graphql.EnumValueConfig{Value: string(planner.OrderByPolicyIndexPrefixOnly)},
			string(planner.OrderByPolicyAllowNonPrefix):  &graphql.EnumValueConfig{Value: string(planner.OrderByPolicyAllowNonPrefix)},
		},
	})

	r.mu.Lock()
	if r.orderByPolicy == nil {
		r.orderByPolicy = enumValue
	}
	cached = r.orderByPolicy
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

	scalar := scalars.NonNegativeInt()

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

	scalar := scalars.JSON()

	r.mu.Lock()
	if r.jsonType == nil {
		r.jsonType = scalar
	}
	cached = r.jsonType
	r.mu.Unlock()

	return cached
}

func (r *Resolver) bigIntScalar() *graphql.Scalar {
	r.mu.RLock()
	cached := r.bigIntType
	r.mu.RUnlock()
	if cached != nil {
		return cached
	}

	scalar := scalars.BigInt()

	r.mu.Lock()
	if r.bigIntType == nil {
		r.bigIntType = scalar
	}
	cached = r.bigIntType
	r.mu.Unlock()

	return cached
}

func (r *Resolver) decimalScalar() *graphql.Scalar {
	r.mu.RLock()
	cached := r.decimalType
	r.mu.RUnlock()
	if cached != nil {
		return cached
	}

	scalar := scalars.Decimal()

	r.mu.Lock()
	if r.decimalType == nil {
		r.decimalType = scalar
	}
	cached = r.decimalType
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

	scalar := scalars.Date()

	r.mu.Lock()
	if r.dateType == nil {
		r.dateType = scalar
	}
	cached = r.dateType
	r.mu.Unlock()

	return cached
}

func (r *Resolver) timeScalar() *graphql.Scalar {
	r.mu.RLock()
	cached := r.timeType
	r.mu.RUnlock()
	if cached != nil {
		return cached
	}

	scalar := scalars.Time()

	r.mu.Lock()
	if r.timeType == nil {
		r.timeType = scalar
	}
	cached = r.timeType
	r.mu.Unlock()

	return cached
}

func (r *Resolver) yearScalar() *graphql.Scalar {
	r.mu.RLock()
	cached := r.yearType
	r.mu.RUnlock()
	if cached != nil {
		return cached
	}

	scalar := scalars.Year()

	r.mu.Lock()
	if r.yearType == nil {
		r.yearType = scalar
	}
	cached = r.yearType
	r.mu.Unlock()

	return cached
}

func (r *Resolver) bytesScalar() *graphql.Scalar {
	r.mu.RLock()
	cached := r.bytesType
	r.mu.RUnlock()
	if cached != nil {
		return cached
	}

	scalar := scalars.Bytes()

	r.mu.Lock()
	if r.bytesType == nil {
		r.bytesType = scalar
	}
	cached = r.bytesType
	r.mu.Unlock()

	return cached
}

func (r *Resolver) uuidScalar() *graphql.Scalar {
	r.mu.RLock()
	cached := r.uuidType
	r.mu.RUnlock()
	if cached != nil {
		return cached
	}

	scalar := scalars.UUID()

	r.mu.Lock()
	if r.uuidType == nil {
		r.uuidType = scalar
	}
	cached = r.uuidType
	r.mu.Unlock()

	return cached
}

func (r *Resolver) vectorScalar() *graphql.Scalar {
	r.mu.RLock()
	cached := r.vectorType
	r.mu.RUnlock()
	if cached != nil {
		return cached
	}

	scalar := scalars.Vector()

	r.mu.Lock()
	if r.vectorType == nil {
		r.vectorType = scalar
	}
	cached = r.vectorType
	r.mu.Unlock()

	return cached
}

func (r *Resolver) vectorDistanceMetricEnum() *graphql.Enum {
	r.mu.RLock()
	cached := r.vectorDistance
	r.mu.RUnlock()
	if cached != nil {
		return cached
	}

	enumValue := graphql.NewEnum(graphql.EnumConfig{
		Name: "VectorDistanceMetric",
		Values: graphql.EnumValueConfigMap{
			string(planner.VectorDistanceMetricCosine): &graphql.EnumValueConfig{Value: string(planner.VectorDistanceMetricCosine)},
			string(planner.VectorDistanceMetricL2):     &graphql.EnumValueConfig{Value: string(planner.VectorDistanceMetricL2)},
		},
	})

	r.mu.Lock()
	if r.vectorDistance == nil {
		r.vectorDistance = enumValue
	}
	cached = r.vectorDistance
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

// pageInfoType returns the shared PageInfo GraphQL type (lazy-init).
func (r *Resolver) getPageInfoType() *graphql.Object {
	r.mu.RLock()
	cached := r.pageInfoType
	r.mu.RUnlock()
	if cached != nil {
		return cached
	}

	pageInfo := graphql.NewObject(graphql.ObjectConfig{
		Name: "PageInfo",
		Fields: graphql.Fields{
			"hasNextPage": &graphql.Field{
				Type: graphql.NewNonNull(graphql.Boolean),
			},
			"hasPreviousPage": &graphql.Field{
				Type: graphql.NewNonNull(graphql.Boolean),
			},
			"startCursor": &graphql.Field{
				Type: graphql.String,
			},
			"endCursor": &graphql.Field{
				Type: graphql.String,
			},
		},
	})

	r.mu.Lock()
	if r.pageInfoType == nil {
		r.pageInfoType = pageInfo
	}
	cached = r.pageInfoType
	r.mu.Unlock()

	return cached
}

// buildEdgeType builds the Edge type for a table (cached per table).
func (r *Resolver) buildEdgeType(table introspection.Table, tableType *graphql.Object) *graphql.Object {
	typeName := introspection.GraphQLTypeName(table) + "Edge"

	r.mu.RLock()
	if cached, ok := r.edgeCache[typeName]; ok {
		r.mu.RUnlock()
		return cached
	}
	r.mu.RUnlock()

	edgeType := graphql.NewObject(graphql.ObjectConfig{
		Name: typeName,
		Fields: graphql.Fields{
			"cursor": &graphql.Field{
				Type: graphql.NewNonNull(graphql.String),
			},
			"node": &graphql.Field{
				Type: graphql.NewNonNull(tableType),
			},
		},
	})

	r.mu.Lock()
	if cached, ok := r.edgeCache[typeName]; ok {
		r.mu.Unlock()
		return cached
	}
	r.edgeCache[typeName] = edgeType
	r.mu.Unlock()

	return edgeType
}

// buildConnectionType builds the Connection type for a table (cached per table).
func (r *Resolver) buildConnectionType(table introspection.Table, tableType *graphql.Object) *graphql.Object {
	typeName := introspection.GraphQLTypeName(table) + "Connection"

	r.mu.RLock()
	if cached, ok := r.connectionCache[typeName]; ok {
		r.mu.RUnlock()
		return cached
	}
	r.mu.RUnlock()

	edgeType := r.buildEdgeType(table, tableType)
	pageInfo := r.getPageInfoType()
	aggregateType := r.buildAggregateFieldsType(table)

	connType := graphql.NewObject(graphql.ObjectConfig{
		Name: typeName,
		Fields: graphql.Fields{
			"edges": &graphql.Field{
				Type: graphql.NewNonNull(graphql.NewList(graphql.NewNonNull(edgeType))),
			},
			"nodes": &graphql.Field{
				Type: graphql.NewNonNull(graphql.NewList(graphql.NewNonNull(tableType))),
			},
			"pageInfo": &graphql.Field{
				Type: graphql.NewNonNull(pageInfo),
			},
			"totalCount": &graphql.Field{
				Type: graphql.NewNonNull(graphql.Int),
				Resolve: func(p graphql.ResolveParams) (interface{}, error) {
					source, ok := p.Source.(map[string]interface{})
					if !ok {
						return 0, nil
					}
					cr, ok := source["__connectionResult"].(*connectionResult)
					if !ok || cr == nil || cr.plan == nil {
						return 0, nil
					}
					return cr.totalCount()
				},
			},
			"aggregate": &graphql.Field{
				Type: graphql.NewNonNull(aggregateType),
				Resolve: func(p graphql.ResolveParams) (interface{}, error) {
					source, ok := p.Source.(map[string]interface{})
					if !ok {
						return map[string]interface{}{"count": 0}, nil
					}
					cr, ok := source["__connectionResult"].(*connectionResult)
					if !ok || cr == nil {
						return map[string]interface{}{"count": 0}, nil
					}
					field := firstFieldAST(p.Info.FieldASTs)
					selection := planner.ParseAggregateSelection(table, field, p.Info.Fragments)
					return cr.aggregate(selection)
				},
			},
		},
	})

	r.mu.Lock()
	if cached, ok := r.connectionCache[typeName]; ok {
		r.mu.Unlock()
		return cached
	}
	r.connectionCache[typeName] = connType
	r.mu.Unlock()

	return connType
}

func (r *Resolver) vectorTypeSuffix(vectorCol introspection.Column) string {
	return r.singularNamer.ToGraphQLTypeName(introspection.GraphQLFieldName(vectorCol)) + "Vector"
}

func (r *Resolver) buildVectorEdgeType(table introspection.Table, vectorCol introspection.Column, tableType *graphql.Object) *graphql.Object {
	typeName := introspection.GraphQLTypeName(table) + r.vectorTypeSuffix(vectorCol) + "Edge"

	r.mu.RLock()
	if cached, ok := r.vectorEdgeCache[typeName]; ok {
		r.mu.RUnlock()
		return cached
	}
	r.mu.RUnlock()

	edgeType := graphql.NewObject(graphql.ObjectConfig{
		Name: typeName,
		Fields: graphql.Fields{
			"cursor": &graphql.Field{
				Type: graphql.NewNonNull(graphql.String),
			},
			"node": &graphql.Field{
				Type: graphql.NewNonNull(tableType),
			},
			"distance": &graphql.Field{
				Type: graphql.NewNonNull(graphql.Float),
			},
			"rank": &graphql.Field{
				Type: graphql.NewNonNull(graphql.Int),
			},
		},
	})

	r.mu.Lock()
	if cached, ok := r.vectorEdgeCache[typeName]; ok {
		r.mu.Unlock()
		return cached
	}
	r.vectorEdgeCache[typeName] = edgeType
	r.mu.Unlock()

	return edgeType
}

func (r *Resolver) buildVectorConnectionType(table introspection.Table, vectorCol introspection.Column, tableType *graphql.Object) *graphql.Object {
	typeName := introspection.GraphQLTypeName(table) + r.vectorTypeSuffix(vectorCol) + "Connection"

	r.mu.RLock()
	if cached, ok := r.vectorConnCache[typeName]; ok {
		r.mu.RUnlock()
		return cached
	}
	r.mu.RUnlock()

	edgeType := r.buildVectorEdgeType(table, vectorCol, tableType)
	pageInfo := r.getPageInfoType()

	connType := graphql.NewObject(graphql.ObjectConfig{
		Name: typeName,
		Fields: graphql.Fields{
			"edges": &graphql.Field{
				Type: graphql.NewNonNull(graphql.NewList(graphql.NewNonNull(edgeType))),
			},
			"nodes": &graphql.Field{
				Type: graphql.NewNonNull(graphql.NewList(graphql.NewNonNull(tableType))),
			},
			"pageInfo": &graphql.Field{
				Type: graphql.NewNonNull(pageInfo),
			},
		},
	})

	r.mu.Lock()
	if cached, ok := r.vectorConnCache[typeName]; ok {
		r.mu.Unlock()
		return cached
	}
	r.vectorConnCache[typeName] = connType
	r.mu.Unlock()

	return connType
}

// connectionResult holds the data needed to resolve connection fields.
type connectionResult struct {
	rows     []map[string]interface{}
	plan     *planner.ConnectionPlan
	hasNext  bool
	hasPrev  bool
	executor dbexec.QueryExecutor
	countCtx context.Context
	// totalCount is lazily computed
	totalCountVal *int
	totalCountMu  sync.Mutex
	// aggregate results are cached per selection shape.
	aggregateVals map[string]map[string]interface{}
	aggregateMu   sync.Mutex
}

func (cr *connectionResult) totalCount() (int, error) {
	cr.totalCountMu.Lock()
	defer cr.totalCountMu.Unlock()

	if cr.totalCountVal != nil {
		return *cr.totalCountVal, nil
	}
	if cr.plan == nil || cr.executor == nil || cr.plan.Count.SQL == "" {
		count := 0
		cr.totalCountVal = &count
		return count, nil
	}

	rows, err := cr.executor.QueryContext(cr.countCtx, cr.plan.Count.SQL, cr.plan.Count.Args...)
	if err != nil {
		return 0, err
	}
	defer func() { _ = rows.Close() }()

	var count int
	if rows.Next() {
		if err := rows.Scan(&count); err != nil {
			return 0, err
		}
	}
	if err := rows.Err(); err != nil {
		return 0, err
	}

	cr.totalCountVal = &count
	return count, nil
}

func (cr *connectionResult) aggregate(selection planner.AggregateSelection) (map[string]interface{}, error) {
	columns := planner.BuildAggregateColumns(selection)
	cacheKey := aggregateColumnsKey(columns)

	cr.aggregateMu.Lock()
	if cached, ok := cr.aggregateVals[cacheKey]; ok {
		cr.aggregateMu.Unlock()
		return cached, nil
	}
	cr.aggregateMu.Unlock()

	// COUNT-only aggregate can be served directly from totalCount.
	if len(columns) == 1 {
		count, err := cr.totalCount()
		if err != nil {
			return nil, err
		}
		result := map[string]interface{}{"count": count}
		cr.aggregateMu.Lock()
		if existing, ok := cr.aggregateVals[cacheKey]; ok {
			cr.aggregateMu.Unlock()
			return existing, nil
		}
		cr.aggregateVals[cacheKey] = result
		cr.aggregateMu.Unlock()
		return result, nil
	}

	if cr.plan == nil || cr.plan.AggregateBase.SQL == "" || cr.executor == nil {
		count, err := cr.totalCount()
		if err != nil {
			return nil, err
		}
		result := map[string]interface{}{"count": count}
		cr.aggregateMu.Lock()
		if existing, ok := cr.aggregateVals[cacheKey]; ok {
			cr.aggregateMu.Unlock()
			return existing, nil
		}
		cr.aggregateVals[cacheKey] = result
		cr.aggregateMu.Unlock()
		return result, nil
	}

	aggregateQuery := planner.BuildConnectionAggregateSQL(cr.plan.AggregateBase, selection)
	rows, err := cr.executor.QueryContext(cr.countCtx, aggregateQuery.SQL, aggregateQuery.Args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	result, err := scanAggregateRow(rows, columns, cr.plan.Table)
	if err != nil {
		return nil, err
	}

	if count, ok := aggregateCountAsInt(result["count"]); ok {
		cr.totalCountMu.Lock()
		if cr.totalCountVal == nil {
			c := count
			cr.totalCountVal = &c
		}
		cr.totalCountMu.Unlock()
		result["count"] = count
	} else {
		count, err := cr.totalCount()
		if err != nil {
			return nil, err
		}
		result["count"] = count
	}

	cr.aggregateMu.Lock()
	if existing, ok := cr.aggregateVals[cacheKey]; ok {
		cr.aggregateMu.Unlock()
		return existing, nil
	}
	cr.aggregateVals[cacheKey] = result
	cr.aggregateMu.Unlock()

	return result, nil
}

func aggregateCountAsInt(value interface{}) (int, bool) {
	switch v := value.(type) {
	case int:
		return v, true
	case int64:
		return int(v), true
	case float64:
		return int(v), true
	default:
		return 0, false
	}
}

func encodeCursorFromRow(row map[string]interface{}, plan *planner.ConnectionPlan) string {
	typeName := introspection.GraphQLTypeName(plan.Table)
	values := make([]interface{}, len(plan.CursorColumns))
	for i, col := range plan.CursorColumns {
		fieldName := introspection.GraphQLFieldName(col)
		values[i] = row[fieldName]
	}
	return cursor.EncodeCursor(typeName, plan.OrderByKey, plan.OrderBy.Directions, values...)
}

func shouldBatchForwardConnection(args map[string]interface{}) bool {
	if hasConnectionCursorArg(args, "after") || hasConnectionCursorArg(args, "before") {
		return false
	}
	if args == nil {
		return true
	}
	if last, ok := args["last"]; ok && last != nil {
		return false
	}
	return true
}

func hasConnectionCursorArg(args map[string]interface{}, key string) bool {
	if args == nil {
		return false
	}
	raw, ok := args[key]
	if !ok || raw == nil {
		return false
	}
	if str, ok := raw.(string); ok {
		return str != ""
	}
	return true
}

func reverseConnectionRows(rows []map[string]interface{}) {
	for i, j := 0, len(rows)-1; i < j; i, j = i+1, j-1 {
		rows[i], rows[j] = rows[j], rows[i]
	}
}

func shapeConnectionRows(rows []map[string]interface{}, plan *planner.ConnectionPlan) ([]map[string]interface{}, bool, bool) {
	if plan == nil {
		return rows, false, false
	}
	if plan.Mode == planner.PaginationModeBackward {
		hasPrevious := len(rows) > plan.First
		if hasPrevious {
			rows = rows[:plan.First]
		}
		reverseConnectionRows(rows)
		return rows, plan.HasBefore, hasPrevious
	}

	hasNext := len(rows) > plan.First
	if hasNext {
		rows = rows[:plan.First]
	}
	return rows, hasNext, plan.HasAfter
}

// makeConnectionResolver creates a resolver for root connection queries.
func (r *Resolver) makeConnectionResolver(table introspection.Table) graphql.FieldResolveFn {
	return func(p graphql.ResolveParams) (interface{}, error) {
		field := firstFieldAST(p.Info.FieldASTs)
		if field == nil {
			return nil, fmt.Errorf("missing field AST")
		}

		var opts []planner.PlanOption
		opts = append(opts, planner.WithFragments(p.Info.Fragments))
		opts = append(opts, planner.WithDefaultListLimit(r.defaultLimit))
		opts = append(opts, planner.WithSchema(r.dbSchema))
		if r.limits != nil {
			opts = append(opts, planner.WithLimits(*r.limits))
		}

		plan, err := planner.PlanConnection(r.dbSchema, table, field, p.Args, opts...)
		if err != nil {
			return nil, fmt.Errorf("failed to plan connection: %w", err)
		}

		rows, err := r.executor.QueryContext(p.Context, plan.Root.SQL, plan.Root.Args...)
		if err != nil {
			return nil, normalizeQueryError(err)
		}
		defer func() { _ = rows.Close() }()

		results, err := scanRows(rows, plan.Columns)
		if err != nil {
			return nil, err
		}

		results, hasNext, hasPrev := shapeConnectionRows(results, plan)

		seedBatchRows(p, results)
		return r.buildConnectionResult(p.Context, results, plan, hasNext, hasPrev), nil
	}
}

func (r *Resolver) makeVectorConnectionResolver(table introspection.Table, vectorCol introspection.Column) graphql.FieldResolveFn {
	return func(p graphql.ResolveParams) (interface{}, error) {
		field := firstFieldAST(p.Info.FieldASTs)
		if field == nil {
			return nil, fmt.Errorf("missing field AST")
		}

		r.mu.RLock()
		searchCfg := r.vectorSearch
		r.mu.RUnlock()

		var opts []planner.PlanOption
		opts = append(opts, planner.WithFragments(p.Info.Fragments))
		opts = append(opts, planner.WithSchema(r.dbSchema))
		if r.limits != nil {
			opts = append(opts, planner.WithLimits(*r.limits))
		}

		plan, err := planner.PlanVectorSearchConnection(
			r.dbSchema,
			table,
			vectorCol,
			field,
			p.Args,
			searchCfg.MaxTopK,
			searchCfg.DefaultFirst,
			opts...,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to plan vector search connection: %w", err)
		}

		rows, err := r.executor.QueryContext(p.Context, plan.Root.SQL, plan.Root.Args...)
		if err != nil {
			return nil, normalizeQueryError(err)
		}
		defer func() { _ = rows.Close() }()

		results, err := scanRowsWithExtras(rows, plan.Columns, []string{plan.DistanceAlias})
		if err != nil {
			return nil, err
		}

		hasNext := len(results) > plan.First
		if hasNext {
			results = results[:plan.First]
		}

		edges := make([]map[string]interface{}, len(results))
		nodes := make([]map[string]interface{}, len(results))
		for i, row := range results {
			distance, err := coerceDistanceValue(row[plan.DistanceAlias])
			if err != nil {
				return nil, err
			}

			node := make(map[string]interface{}, len(plan.Columns))
			for _, col := range plan.Columns {
				fieldName := introspection.GraphQLFieldName(col)
				node[fieldName] = row[fieldName]
			}
			rank := i + 1

			cursorValues := make([]interface{}, 0, len(plan.PKColumns)+1)
			cursorValues = append(cursorValues, distance)
			for _, pk := range plan.PKColumns {
				pkField := introspection.GraphQLFieldName(pk)
				cursorValues = append(cursorValues, node[pkField])
			}
			encodedCursor := cursor.EncodeCursor(introspection.GraphQLTypeName(plan.Table), plan.OrderByKey, plan.CursorDirections, cursorValues...)

			edges[i] = map[string]interface{}{
				"cursor":   encodedCursor,
				"node":     node,
				"distance": distance,
				"rank":     rank,
			}
			nodes[i] = node
		}

		var startCursor, endCursor interface{}
		if len(edges) > 0 {
			startCursor = edges[0]["cursor"]
			endCursor = edges[len(edges)-1]["cursor"]
		}

		return map[string]interface{}{
			"edges": edges,
			"nodes": nodes,
			"pageInfo": map[string]interface{}{
				"hasNextPage":     hasNext,
				"hasPreviousPage": plan.HasAfter,
				"startCursor":     startCursor,
				"endCursor":       endCursor,
			},
		}, nil
	}
}

func coerceDistanceValue(value interface{}) (float64, error) {
	switch v := value.(type) {
	case float64:
		return v, nil
	case float32:
		return float64(v), nil
	case int:
		return float64(v), nil
	case int8:
		return float64(v), nil
	case int16:
		return float64(v), nil
	case int32:
		return float64(v), nil
	case int64:
		return float64(v), nil
	case uint:
		return float64(v), nil
	case uint8:
		return float64(v), nil
	case uint16:
		return float64(v), nil
	case uint32:
		return float64(v), nil
	case uint64:
		return float64(v), nil
	case []byte:
		parsed, err := strconv.ParseFloat(string(v), 64)
		if err != nil {
			return 0, fmt.Errorf("invalid distance value: %w", err)
		}
		return parsed, nil
	case string:
		parsed, err := strconv.ParseFloat(v, 64)
		if err != nil {
			return 0, fmt.Errorf("invalid distance value: %w", err)
		}
		return parsed, nil
	default:
		return 0, fmt.Errorf("invalid distance value type %T", value)
	}
}

// makeOneToManyConnectionResolver creates a resolver for relationship connection queries.
func (r *Resolver) makeOneToManyConnectionResolver(parentTable introspection.Table, rel introspection.Relationship) graphql.FieldResolveFn {
	return func(p graphql.ResolveParams) (interface{}, error) {
		source, ok := p.Source.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("invalid source type")
		}
		localColumn, remoteColumn, err := oneToManyMappingColumns(rel)
		if err != nil {
			return nil, err
		}

		pkFieldName := graphQLFieldNameForColumn(parentTable, localColumn)
		pkValue := source[pkFieldName]
		if pkValue == nil {
			return r.buildConnectionResult(p.Context, nil, nil, false, false), nil
		}

		// Batch only for forward first-page connection requests.
		if shouldBatchForwardConnection(p.Args) {
			if result, ok, err := r.tryBatchOneToManyConnection(p, parentTable, rel, pkValue); ok || err != nil {
				return result, err
			}
		}

		relatedTable, err := r.findTable(rel.RemoteTable)
		if err != nil {
			return nil, fmt.Errorf("failed to find related table %s: %w", rel.RemoteTable, err)
		}

		field := firstFieldAST(p.Info.FieldASTs)
		if field == nil {
			return nil, fmt.Errorf("missing field AST")
		}

		var opts []planner.PlanOption
		opts = append(opts, planner.WithFragments(p.Info.Fragments))
		opts = append(opts, planner.WithDefaultListLimit(r.defaultLimit))
		opts = append(opts, planner.WithSchema(r.dbSchema))
		if r.limits != nil {
			opts = append(opts, planner.WithLimits(*r.limits))
		}

		plan, err := planner.PlanOneToManyConnection(relatedTable, remoteColumn, pkValue, field, p.Args, opts...)
		if err != nil {
			return nil, fmt.Errorf("failed to plan connection: %w", err)
		}

		rows, err := r.executor.QueryContext(p.Context, plan.Root.SQL, plan.Root.Args...)
		if err != nil {
			return nil, normalizeQueryError(err)
		}
		defer func() { _ = rows.Close() }()

		results, err := scanRows(rows, plan.Columns)
		if err != nil {
			return nil, err
		}

		results, hasNext, hasPrev := shapeConnectionRows(results, plan)

		seedBatchRows(p, results)
		return r.buildConnectionResult(p.Context, results, plan, hasNext, hasPrev), nil
	}
}

// makeManyToManyConnectionResolver creates a resolver for many-to-many connection queries.
func (r *Resolver) makeManyToManyConnectionResolver(parentTable introspection.Table, rel introspection.Relationship) graphql.FieldResolveFn {
	return func(p graphql.ResolveParams) (interface{}, error) {
		source, ok := p.Source.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("invalid source type")
		}

		localColumns := rel.EffectiveLocalColumns()
		pkValues, ok := sourceValuesForColumns(parentTable, source, localColumns)
		if !ok {
			return r.buildConnectionResult(p.Context, nil, nil, false, false), nil
		}

		// Batch only for forward first-page connection requests.
		if shouldBatchForwardConnection(p.Args) {
			if result, ok, err := r.tryBatchManyToManyConnection(p, parentTable, rel, pkValues); ok || err != nil {
				return result, err
			}
		}

		relatedTable, err := r.findTable(rel.RemoteTable)
		if err != nil {
			return nil, fmt.Errorf("failed to find related table %s: %w", rel.RemoteTable, err)
		}

		field := firstFieldAST(p.Info.FieldASTs)
		if field == nil {
			return nil, fmt.Errorf("missing field AST")
		}

		var opts []planner.PlanOption
		opts = append(opts, planner.WithFragments(p.Info.Fragments))
		opts = append(opts, planner.WithDefaultListLimit(r.defaultLimit))
		opts = append(opts, planner.WithSchema(r.dbSchema))
		if r.limits != nil {
			opts = append(opts, planner.WithLimits(*r.limits))
		}

		plan, err := planner.PlanManyToManyConnection(
			relatedTable,
			rel.JunctionTable,
			rel.EffectiveJunctionLocalFKColumns(),
			rel.EffectiveJunctionRemoteFKColumns(),
			rel.EffectiveRemoteColumns(),
			pkValues,
			field,
			p.Args,
			opts...,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to plan connection: %w", err)
		}

		rows, err := r.executor.QueryContext(p.Context, plan.Root.SQL, plan.Root.Args...)
		if err != nil {
			return nil, normalizeQueryError(err)
		}
		defer func() { _ = rows.Close() }()

		results, err := scanRows(rows, plan.Columns)
		if err != nil {
			return nil, err
		}

		results, hasNext, hasPrev := shapeConnectionRows(results, plan)

		seedBatchRows(p, results)
		return r.buildConnectionResult(p.Context, results, plan, hasNext, hasPrev), nil
	}
}

// makeEdgeListConnectionResolver creates a resolver for edge-list connection queries.
func (r *Resolver) makeEdgeListConnectionResolver(parentTable introspection.Table, rel introspection.Relationship) graphql.FieldResolveFn {
	return func(p graphql.ResolveParams) (interface{}, error) {
		source, ok := p.Source.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("invalid source type")
		}

		localColumns := rel.EffectiveLocalColumns()
		pkValues, ok := sourceValuesForColumns(parentTable, source, localColumns)
		if !ok {
			return r.buildConnectionResult(p.Context, nil, nil, false, false), nil
		}

		// Batch only for forward first-page connection requests.
		if shouldBatchForwardConnection(p.Args) {
			if result, ok, err := r.tryBatchEdgeListConnection(p, parentTable, rel, pkValues); ok || err != nil {
				return result, err
			}
		}

		junctionTable, err := r.findTable(rel.JunctionTable)
		if err != nil {
			return nil, fmt.Errorf("failed to find junction table %s: %w", rel.JunctionTable, err)
		}

		field := firstFieldAST(p.Info.FieldASTs)
		if field == nil {
			return nil, fmt.Errorf("missing field AST")
		}

		var opts []planner.PlanOption
		opts = append(opts, planner.WithFragments(p.Info.Fragments))
		opts = append(opts, planner.WithDefaultListLimit(r.defaultLimit))
		opts = append(opts, planner.WithSchema(r.dbSchema))
		if r.limits != nil {
			opts = append(opts, planner.WithLimits(*r.limits))
		}

		plan, err := planner.PlanEdgeListConnection(
			junctionTable,
			rel.EffectiveJunctionLocalFKColumns(),
			pkValues,
			field,
			p.Args,
			opts...,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to plan connection: %w", err)
		}

		rows, err := r.executor.QueryContext(p.Context, plan.Root.SQL, plan.Root.Args...)
		if err != nil {
			return nil, normalizeQueryError(err)
		}
		defer func() { _ = rows.Close() }()

		results, err := scanRows(rows, plan.Columns)
		if err != nil {
			return nil, err
		}

		results, hasNext, hasPrev := shapeConnectionRows(results, plan)

		return r.buildConnectionResult(p.Context, results, plan, hasNext, hasPrev), nil
	}
}

// buildConnectionResult constructs the map that connection field resolvers read from.
func (r *Resolver) buildConnectionResult(ctx context.Context, rows []map[string]interface{}, plan *planner.ConnectionPlan, hasNext bool, hasPrev bool) map[string]interface{} {
	if rows == nil {
		rows = []map[string]interface{}{}
	}
	countCtx := ctx
	if countCtx == nil {
		countCtx = context.Background()
	}
	// totalCount is lazy, so it should not fail just because the caller's
	// request context is canceled after rows have already been materialized.
	countCtx = context.WithoutCancel(countCtx)

	result := &connectionResult{
		rows:          rows,
		plan:          plan,
		hasNext:       hasNext,
		hasPrev:       hasPrev,
		executor:      r.executor,
		countCtx:      countCtx,
		aggregateVals: make(map[string]map[string]interface{}),
	}

	// Build edges
	edges := make([]map[string]interface{}, len(rows))
	for i, row := range rows {
		var c string
		if plan != nil {
			c = encodeCursorFromRow(row, plan)
		}
		edges[i] = map[string]interface{}{
			"cursor": c,
			"node":   row,
		}
	}

	// Build pageInfo
	var startCursor, endCursor interface{}
	if len(edges) > 0 {
		startCursor = edges[0]["cursor"]
		endCursor = edges[len(edges)-1]["cursor"]
	}

	connMap := map[string]interface{}{
		"edges": edges,
		"nodes": rows,
		"pageInfo": map[string]interface{}{
			"hasNextPage":     hasNext,
			"hasPreviousPage": hasPrev,
			"startCursor":     startCursor,
			"endCursor":       endCursor,
		},
		"__connectionResult": result,
	}

	return connMap
}

func (r *Resolver) whereInput(table introspection.Table) *graphql.InputObject {
	return r.whereInputForTable(table, true)
}

func (r *Resolver) scalarWhereInput(table introspection.Table) *graphql.InputObject {
	return r.whereInputForTable(table, false)
}

func (r *Resolver) whereInputForTable(table introspection.Table, includeRelations bool) *graphql.InputObject {
	if table.IsView {
		return nil
	}
	typeName := introspection.GraphQLTypeName(table) + "Where"
	if !includeRelations {
		typeName = introspection.GraphQLTypeName(table) + "ScalarWhere"
	}
	r.mu.RLock()
	cached, ok := r.whereCache[typeName]
	r.mu.RUnlock()
	if ok {
		return cached
	}

	// Create a lazy-initialized input object to handle recursive reference
	var inputObj *graphql.InputObject
	inputObj = graphql.NewInputObject(graphql.InputObjectConfig{
		Name: typeName,
		Fields: graphql.InputObjectConfigFieldMapThunk(func() graphql.InputObjectConfigFieldMap {
			fields := r.scalarWhereFields(table)
			if includeRelations {
				r.addRelationshipWhereFields(fields, table)
			}
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

func (r *Resolver) scalarWhereFields(table introspection.Table) graphql.InputObjectConfigFieldMap {
	fields := graphql.InputObjectConfigFieldMap{}
	for _, col := range table.Columns {
		// Skip JSON and VECTOR columns from generic where inputs.
		effectiveType := introspection.EffectiveGraphQLType(col)
		if effectiveType == sqltype.TypeJSON || effectiveType == sqltype.TypeVector {
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
	return fields
}

func (r *Resolver) addRelationshipWhereFields(fields graphql.InputObjectConfigFieldMap, table introspection.Table) {
	for _, rel := range table.Relationships {
		if _, exists := fields[rel.GraphQLFieldName]; exists {
			continue
		}

		var relatedTable introspection.Table
		var err error
		switch {
		case rel.IsEdgeList:
			relatedTable, err = r.findTable(rel.JunctionTable)
		default:
			relatedTable, err = r.findTable(rel.RemoteTable)
		}
		if err != nil {
			continue
		}

		var relationInput *graphql.InputObject
		switch {
		case rel.IsManyToOne:
			relationInput = r.toOneRelationshipWhereInput(relatedTable)
		case rel.IsOneToMany, rel.IsManyToMany, rel.IsEdgeList:
			relationInput = r.toManyRelationshipWhereInput(relatedTable)
		default:
			continue
		}
		if relationInput == nil {
			continue
		}
		fields[rel.GraphQLFieldName] = &graphql.InputObjectFieldConfig{
			Type: relationInput,
		}
	}
}

func (r *Resolver) toManyRelationshipWhereInput(relatedTable introspection.Table) *graphql.InputObject {
	nestedWhere := r.scalarWhereInput(relatedTable)
	if nestedWhere == nil {
		return nil
	}

	typeName := introspection.GraphQLTypeName(relatedTable) + "WhereToManyRelationFilter"
	r.mu.RLock()
	cached, ok := r.whereCache[typeName]
	r.mu.RUnlock()
	if ok {
		return cached
	}

	input := graphql.NewInputObject(graphql.InputObjectConfig{
		Name: typeName,
		Fields: graphql.InputObjectConfigFieldMap{
			"some": &graphql.InputObjectFieldConfig{Type: nestedWhere},
			"none": &graphql.InputObjectFieldConfig{Type: nestedWhere},
		},
	})

	r.mu.Lock()
	if cached, ok := r.whereCache[typeName]; ok {
		r.mu.Unlock()
		return cached
	}
	r.whereCache[typeName] = input
	r.mu.Unlock()

	return input
}

func (r *Resolver) toOneRelationshipWhereInput(relatedTable introspection.Table) *graphql.InputObject {
	nestedWhere := r.scalarWhereInput(relatedTable)
	if nestedWhere == nil {
		return nil
	}

	typeName := introspection.GraphQLTypeName(relatedTable) + "WhereToOneRelationFilter"
	r.mu.RLock()
	cached, ok := r.whereCache[typeName]
	r.mu.RUnlock()
	if ok {
		return cached
	}

	input := graphql.NewInputObject(graphql.InputObjectConfig{
		Name: typeName,
		Fields: graphql.InputObjectConfigFieldMap{
			"is":     &graphql.InputObjectFieldConfig{Type: nestedWhere},
			"isNull": &graphql.InputObjectFieldConfig{Type: graphql.Boolean},
		},
	})

	r.mu.Lock()
	if cached, ok := r.whereCache[typeName]; ok {
		r.mu.Unlock()
		return cached
	}
	r.whereCache[typeName] = input
	r.mu.Unlock()

	return input
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

func (r *Resolver) setFilterType(enumType *graphql.Enum) *graphql.InputObject {
	if enumType == nil {
		return nil
	}
	filterName := enumType.Name() + "SetFilter"

	r.mu.RLock()
	cached := r.setFilterCache[filterName]
	r.mu.RUnlock()
	if cached != nil {
		return cached
	}

	listEnumType := graphql.NewList(graphql.NewNonNull(enumType))
	filterType := graphql.NewInputObject(graphql.InputObjectConfig{
		Name: filterName,
		Fields: graphql.InputObjectConfigFieldMap{
			"has":       &graphql.InputObjectFieldConfig{Type: enumType},
			"hasAnyOf":  &graphql.InputObjectFieldConfig{Type: listEnumType},
			"hasAllOf":  &graphql.InputObjectFieldConfig{Type: listEnumType},
			"hasNoneOf": &graphql.InputObjectFieldConfig{Type: listEnumType},
			"eq":        &graphql.InputObjectFieldConfig{Type: listEnumType},
			"ne":        &graphql.InputObjectFieldConfig{Type: listEnumType},
			"isNull":    &graphql.InputObjectFieldConfig{Type: graphql.Boolean},
		},
	})

	r.mu.Lock()
	if cached := r.setFilterCache[filterName]; cached != nil {
		r.mu.Unlock()
		return cached
	}
	r.setFilterCache[filterName] = filterType
	r.mu.Unlock()

	return filterType
}

func (r *Resolver) getFilterInputType(table introspection.Table, col introspection.Column) *graphql.InputObject {
	effectiveType := introspection.EffectiveGraphQLType(col)
	if effectiveType == sqltype.TypeSet {
		if enumType := r.enumTypeForColumn(table, &col); enumType != nil {
			return r.setFilterType(enumType)
		}
		return nil
	}
	if enumType := r.enumTypeForColumn(table, &col); enumType != nil {
		return r.enumFilterType(enumType)
	}

	filterName := effectiveType.FilterTypeName()

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
	case "BigIntFilter":
		filterType = graphql.NewInputObject(graphql.InputObjectConfig{
			Name: "BigIntFilter",
			Fields: graphql.InputObjectConfigFieldMap{
				"eq":     &graphql.InputObjectFieldConfig{Type: r.bigIntScalar()},
				"ne":     &graphql.InputObjectFieldConfig{Type: r.bigIntScalar()},
				"lt":     &graphql.InputObjectFieldConfig{Type: r.bigIntScalar()},
				"lte":    &graphql.InputObjectFieldConfig{Type: r.bigIntScalar()},
				"gt":     &graphql.InputObjectFieldConfig{Type: r.bigIntScalar()},
				"gte":    &graphql.InputObjectFieldConfig{Type: r.bigIntScalar()},
				"in":     &graphql.InputObjectFieldConfig{Type: graphql.NewList(graphql.NewNonNull(r.bigIntScalar()))},
				"notIn":  &graphql.InputObjectFieldConfig{Type: graphql.NewList(graphql.NewNonNull(r.bigIntScalar()))},
				"isNull": &graphql.InputObjectFieldConfig{Type: graphql.Boolean},
			},
		})
	case "DecimalFilter":
		filterType = graphql.NewInputObject(graphql.InputObjectConfig{
			Name: "DecimalFilter",
			Fields: graphql.InputObjectConfigFieldMap{
				"eq":     &graphql.InputObjectFieldConfig{Type: r.decimalScalar()},
				"ne":     &graphql.InputObjectFieldConfig{Type: r.decimalScalar()},
				"lt":     &graphql.InputObjectFieldConfig{Type: r.decimalScalar()},
				"lte":    &graphql.InputObjectFieldConfig{Type: r.decimalScalar()},
				"gt":     &graphql.InputObjectFieldConfig{Type: r.decimalScalar()},
				"gte":    &graphql.InputObjectFieldConfig{Type: r.decimalScalar()},
				"in":     &graphql.InputObjectFieldConfig{Type: graphql.NewList(graphql.NewNonNull(r.decimalScalar()))},
				"notIn":  &graphql.InputObjectFieldConfig{Type: graphql.NewList(graphql.NewNonNull(r.decimalScalar()))},
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
	case "BytesFilter":
		filterType = graphql.NewInputObject(graphql.InputObjectConfig{
			Name: "BytesFilter",
			Fields: graphql.InputObjectConfigFieldMap{
				"eq":     &graphql.InputObjectFieldConfig{Type: r.bytesScalar()},
				"ne":     &graphql.InputObjectFieldConfig{Type: r.bytesScalar()},
				"in":     &graphql.InputObjectFieldConfig{Type: graphql.NewList(graphql.NewNonNull(r.bytesScalar()))},
				"notIn":  &graphql.InputObjectFieldConfig{Type: graphql.NewList(graphql.NewNonNull(r.bytesScalar()))},
				"isNull": &graphql.InputObjectFieldConfig{Type: graphql.Boolean},
			},
		})
	case "UUIDFilter":
		filterType = graphql.NewInputObject(graphql.InputObjectConfig{
			Name: "UUIDFilter",
			Fields: graphql.InputObjectConfigFieldMap{
				"eq":     &graphql.InputObjectFieldConfig{Type: r.uuidScalar()},
				"ne":     &graphql.InputObjectFieldConfig{Type: r.uuidScalar()},
				"in":     &graphql.InputObjectFieldConfig{Type: graphql.NewList(graphql.NewNonNull(r.uuidScalar()))},
				"notIn":  &graphql.InputObjectFieldConfig{Type: graphql.NewList(graphql.NewNonNull(r.uuidScalar()))},
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
	case "TimeFilter":
		filterType = graphql.NewInputObject(graphql.InputObjectConfig{
			Name: "TimeFilter",
			Fields: graphql.InputObjectConfigFieldMap{
				"eq":     &graphql.InputObjectFieldConfig{Type: r.timeScalar()},
				"ne":     &graphql.InputObjectFieldConfig{Type: r.timeScalar()},
				"lt":     &graphql.InputObjectFieldConfig{Type: r.timeScalar()},
				"lte":    &graphql.InputObjectFieldConfig{Type: r.timeScalar()},
				"gt":     &graphql.InputObjectFieldConfig{Type: r.timeScalar()},
				"gte":    &graphql.InputObjectFieldConfig{Type: r.timeScalar()},
				"in":     &graphql.InputObjectFieldConfig{Type: graphql.NewList(graphql.NewNonNull(r.timeScalar()))},
				"notIn":  &graphql.InputObjectFieldConfig{Type: graphql.NewList(graphql.NewNonNull(r.timeScalar()))},
				"isNull": &graphql.InputObjectFieldConfig{Type: graphql.Boolean},
			},
		})
	case "YearFilter":
		filterType = graphql.NewInputObject(graphql.InputObjectConfig{
			Name: "YearFilter",
			Fields: graphql.InputObjectConfigFieldMap{
				"eq":     &graphql.InputObjectFieldConfig{Type: r.yearScalar()},
				"ne":     &graphql.InputObjectFieldConfig{Type: r.yearScalar()},
				"lt":     &graphql.InputObjectFieldConfig{Type: r.yearScalar()},
				"lte":    &graphql.InputObjectFieldConfig{Type: r.yearScalar()},
				"gt":     &graphql.InputObjectFieldConfig{Type: r.yearScalar()},
				"gte":    &graphql.InputObjectFieldConfig{Type: r.yearScalar()},
				"in":     &graphql.InputObjectFieldConfig{Type: graphql.NewList(graphql.NewNonNull(r.yearScalar()))},
				"notIn":  &graphql.InputObjectFieldConfig{Type: graphql.NewList(graphql.NewNonNull(r.yearScalar()))},
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

func sortedOrderByFieldNames(options map[string]string) []string {
	names := make([]string, 0, len(options))
	for name := range options {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func oneToManyMappingColumns(rel introspection.Relationship) (localColumn string, remoteColumn string, err error) {
	localColumns := rel.EffectiveLocalColumns()
	remoteColumns := rel.EffectiveRemoteColumns()
	if len(localColumns) != 1 || len(remoteColumns) != 1 {
		return "", "", fmt.Errorf("invalid one-to-many mapping for %s", rel.GraphQLFieldName)
	}
	return localColumns[0], remoteColumns[0], nil
}

func (r *Resolver) tryBatchOneToManyConnection(p graphql.ResolveParams, table introspection.Table, rel introspection.Relationship, pkValue interface{}) (map[string]interface{}, bool, error) {
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

	relatedTable, err := r.findTable(rel.RemoteTable)
	if err != nil {
		return nil, true, fmt.Errorf("failed to find related table %s: %w", rel.RemoteTable, err)
	}

	pkCols := introspection.PrimaryKeyColumns(relatedTable)
	if len(pkCols) == 0 {
		if metrics != nil {
			metrics.RecordBatchSkipped(p.Context, relationOneToMany, "no_primary_key")
		}
		return nil, false, nil
	}

	field := firstFieldAST(p.Info.FieldASTs)
	if field == nil {
		return nil, true, fmt.Errorf("missing field AST")
	}
	localColumn, remoteColumn, err := oneToManyMappingColumns(rel)
	if err != nil {
		return nil, true, err
	}

	first, err := planner.ParseFirstWithDefault(p.Args, r.defaultLimit)
	if err != nil {
		return nil, true, err
	}

	orderBy, err := planner.ParseOrderBy(relatedTable, p.Args)
	if err != nil {
		return nil, true, err
	}
	if orderBy == nil {
		pkColNames := make([]string, len(pkCols))
		pkDirections := make([]string, len(pkCols))
		for i, col := range pkCols {
			pkColNames[i] = col.Name
			pkDirections[i] = "ASC"
		}
		orderBy = &planner.OrderBy{
			Columns:    pkColNames,
			Directions: pkDirections,
		}
	}

	var whereClause *planner.WhereClause
	if whereArg, ok := p.Args["where"]; ok {
		if whereMap, ok := whereArg.(map[string]interface{}); ok {
			whereClause, err = planner.BuildWhereClauseWithSchema(r.dbSchema, relatedTable, whereMap)
			if err != nil {
				return nil, true, err
			}
			if whereClause != nil {
				if err := planner.ValidateWhereClauseIndexes(r.dbSchema, relatedTable, whereClause); err != nil {
					return nil, true, err
				}
			}
		}
	}

	selection := planner.SelectedColumnsForConnection(relatedTable, field, p.Info.Fragments, orderBy)
	orderByKey := planner.OrderByKey(relatedTable, orderBy.Columns)
	cursorCols := planner.CursorColumns(relatedTable, orderBy)

	relKey := fmt.Sprintf(
		"%s|%s|%s|%s|%s|%s",
		table.Name,
		rel.RemoteTable,
		remoteColumn,
		orderByKey,
		columnsKey(selection),
		stableArgsKey(p.Args),
	)

	if cached := state.getConnectionRows(relKey); cached != nil {
		state.IncrementCacheHit()
		if metrics != nil {
			metrics.RecordBatchCacheHit(p.Context, relationOneToMany)
		}
		if result, ok := cached[fmt.Sprint(pkValue)]; ok {
			if nodes, ok := result["nodes"].([]map[string]interface{}); ok {
				seedBatchRows(p, nodes)
			}
			return result, true, nil
		}
		return r.buildConnectionResult(p.Context, nil, nil, false, false), true, nil
	}
	state.IncrementCacheMiss()
	if metrics != nil {
		metrics.RecordBatchCacheMiss(p.Context, relationOneToMany)
	}

	parentField := graphQLFieldNameForColumn(table, localColumn)
	parentValues := uniqueParentValues(parentRows, parentField)
	if len(parentValues) == 0 {
		state.setConnectionRows(relKey, map[string]map[string]interface{}{})
		return r.buildConnectionResult(p.Context, nil, nil, false, false), true, nil
	}
	// Keep original typed parent values so count queries can bind FK args correctly.
	parentValueByKey := make(map[string]interface{}, len(parentValues))
	for _, value := range parentValues {
		parentValueByKey[fmt.Sprint(value)] = value
	}

	chunks := chunkValues(parentValues, batchMaxInClause)
	if metrics != nil {
		metrics.RecordBatchQueriesSaved(p.Context, listBatchQueriesSaved(len(parentValues), len(chunks)), relationOneToMany)
	}

	bp := batchConnectionPlan{
		table:         relatedTable,
		selection:     selection,
		orderBy:       orderBy,
		orderByKey:    orderByKey,
		cursorCols:    cursorCols,
		first:         first,
		parentAliases: []string{planner.BatchParentAlias},
		relation:      relationOneToMany,
	}
	groupedConnections := make(map[string]map[string]interface{})
	for _, chunk := range chunks {
		partial, err := runBatchConnectionChunks(
			p.Context, r, bp, len(chunk), metrics,
			func() (planner.SQLQuery, error) {
				return planner.PlanOneToManyConnectionBatch(relatedTable, remoteColumn, selection, chunk, first, orderBy, whereClause)
			},
			func(results []map[string]interface{}) map[string][]map[string]interface{} {
				return groupByAlias(results, planner.BatchParentAlias)
			},
			func(parentID string) (planner.SQLQuery, planner.SQLQuery, error) {
				parentValue := parentValueByKey[parentID]
				count, err := planner.BuildOneToManyCountSQL(relatedTable, remoteColumn, parentValue, whereClause)
				if err != nil {
					return planner.SQLQuery{}, planner.SQLQuery{}, err
				}
				agg, err := planner.BuildOneToManyAggregateBaseSQL(relatedTable, remoteColumn, parentValue, whereClause)
				return count, agg, err
			},
		)
		if errors.Is(err, errBatchSkip) {
			return nil, false, nil
		}
		if err != nil {
			return nil, true, err
		}
		for k, v := range partial {
			groupedConnections[k] = v
		}
	}

	if len(groupedConnections) == 0 {
		state.setConnectionRows(relKey, map[string]map[string]interface{}{})
		return r.buildConnectionResult(p.Context, nil, nil, false, false), true, nil
	}
	state.setConnectionRows(relKey, groupedConnections)

	if result, ok := groupedConnections[fmt.Sprint(pkValue)]; ok {
		if nodes, ok := result["nodes"].([]map[string]interface{}); ok {
			seedBatchRows(p, nodes)
		}
		return result, true, nil
	}
	return r.buildConnectionResult(p.Context, nil, nil, false, false), true, nil
}

func (r *Resolver) tryBatchManyToManyConnection(p graphql.ResolveParams, table introspection.Table, rel introspection.Relationship, pkValues []interface{}) (map[string]interface{}, bool, error) {
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

	relatedTable, err := r.findTable(rel.RemoteTable)
	if err != nil {
		return nil, true, fmt.Errorf("failed to find related table %s: %w", rel.RemoteTable, err)
	}

	pkCols := introspection.PrimaryKeyColumns(relatedTable)
	if len(pkCols) == 0 {
		if metrics != nil {
			metrics.RecordBatchSkipped(p.Context, relationManyToMany, "no_primary_key")
		}
		return nil, false, nil
	}

	field := firstFieldAST(p.Info.FieldASTs)
	if field == nil {
		return nil, true, fmt.Errorf("missing field AST")
	}

	first, err := planner.ParseFirstWithDefault(p.Args, r.defaultLimit)
	if err != nil {
		return nil, true, err
	}

	orderBy, err := planner.ParseOrderBy(relatedTable, p.Args)
	if err != nil {
		return nil, true, err
	}
	if orderBy == nil {
		pkColNames := make([]string, len(pkCols))
		pkDirections := make([]string, len(pkCols))
		for i, col := range pkCols {
			pkColNames[i] = col.Name
			pkDirections[i] = "ASC"
		}
		orderBy = &planner.OrderBy{
			Columns:    pkColNames,
			Directions: pkDirections,
		}
	}

	var whereClause *planner.WhereClause
	if whereArg, ok := p.Args["where"]; ok {
		if whereMap, ok := whereArg.(map[string]interface{}); ok {
			whereClause, err = planner.BuildWhereClauseQualifiedWithSchema(r.dbSchema, relatedTable, relatedTable.Name, whereMap)
			if err != nil {
				return nil, true, err
			}
			if whereClause != nil {
				if err := planner.ValidateWhereClauseIndexes(r.dbSchema, relatedTable, whereClause); err != nil {
					return nil, true, err
				}
			}
		}
	}

	selection := planner.SelectedColumnsForConnection(relatedTable, field, p.Info.Fragments, orderBy)
	orderByKey := planner.OrderByKey(relatedTable, orderBy.Columns)
	cursorCols := planner.CursorColumns(relatedTable, orderBy)
	localColumns := rel.EffectiveLocalColumns()
	junctionLocalColumns := rel.EffectiveJunctionLocalFKColumns()
	junctionRemoteColumns := rel.EffectiveJunctionRemoteFKColumns()
	remoteColumns := rel.EffectiveRemoteColumns()
	if len(localColumns) == 0 || len(localColumns) != len(pkValues) {
		return nil, true, fmt.Errorf("invalid many-to-many local key mapping")
	}
	if len(junctionLocalColumns) != len(localColumns) {
		return nil, true, fmt.Errorf("invalid many-to-many junction local key mapping")
	}
	if len(junctionRemoteColumns) != len(remoteColumns) {
		return nil, true, fmt.Errorf("invalid many-to-many junction remote key mapping")
	}
	currentParentTupleKey := tupleKeyFromValues(pkValues)

	relKey := fmt.Sprintf(
		"%s|%s|%s|%s|%s|%s|%s",
		table.Name,
		rel.RemoteTable,
		rel.JunctionTable,
		strings.Join(remoteColumns, ","),
		orderByKey,
		columnsKey(selection),
		stableArgsKey(p.Args),
	)

	if cached := state.getConnectionRows(relKey); cached != nil {
		state.IncrementCacheHit()
		if metrics != nil {
			metrics.RecordBatchCacheHit(p.Context, relationManyToMany)
		}
		if result, ok := cached[currentParentTupleKey]; ok {
			if nodes, ok := result["nodes"].([]map[string]interface{}); ok {
				seedBatchRows(p, nodes)
			}
			return result, true, nil
		}
		return r.buildConnectionResult(p.Context, nil, nil, false, false), true, nil
	}
	state.IncrementCacheMiss()
	if metrics != nil {
		metrics.RecordBatchCacheMiss(p.Context, relationManyToMany)
	}

	parentFields := make([]string, len(localColumns))
	for i, colName := range localColumns {
		parentFields[i] = graphQLFieldNameForColumn(table, colName)
	}
	parentTuples := uniqueParentTuples(parentRows, parentFields)
	if len(parentTuples) == 0 {
		state.setConnectionRows(relKey, map[string]map[string]interface{}{})
		return r.buildConnectionResult(p.Context, nil, nil, false, false), true, nil
	}
	parentValueByKey := make(map[string]planner.ParentTuple, len(parentTuples))
	for _, tuple := range parentTuples {
		parentValueByKey[tupleKeyFromValues(tuple.Values)] = tuple
	}

	chunks := chunkParentTuples(parentTuples, batchMaxInClause)
	if metrics != nil {
		metrics.RecordBatchQueriesSaved(p.Context, listBatchQueriesSaved(len(parentTuples), len(chunks)), relationManyToMany)
	}

	parentAliases := planner.BatchParentAliases(len(junctionLocalColumns))
	bp := batchConnectionPlan{
		table:         relatedTable,
		selection:     selection,
		orderBy:       orderBy,
		orderByKey:    orderByKey,
		cursorCols:    cursorCols,
		first:         first,
		parentAliases: parentAliases,
		relation:      relationManyToMany,
	}
	groupedConnections := make(map[string]map[string]interface{})
	for _, chunk := range chunks {
		partial, err := runBatchConnectionChunks(
			p.Context, r, bp, len(chunk), metrics,
			func() (planner.SQLQuery, error) {
				return planner.PlanManyToManyConnectionBatch(relatedTable, rel.JunctionTable, junctionLocalColumns, junctionRemoteColumns, remoteColumns, selection, chunk, first, orderBy, whereClause)
			},
			func(results []map[string]interface{}) map[string][]map[string]interface{} {
				return groupByAliases(results, parentAliases)
			},
			func(parentID string) (planner.SQLQuery, planner.SQLQuery, error) {
				tuple := parentValueByKey[parentID]
				count, err := planner.BuildManyToManyCountSQL(relatedTable, rel.JunctionTable, junctionLocalColumns, junctionRemoteColumns, remoteColumns, tuple.Values, whereClause)
				if err != nil {
					return planner.SQLQuery{}, planner.SQLQuery{}, err
				}
				agg, err := planner.BuildManyToManyAggregateBaseSQL(relatedTable, rel.JunctionTable, junctionLocalColumns, junctionRemoteColumns, remoteColumns, tuple.Values, whereClause)
				return count, agg, err
			},
		)
		if errors.Is(err, errBatchSkip) {
			return nil, false, nil
		}
		if err != nil {
			return nil, true, err
		}
		for k, v := range partial {
			groupedConnections[k] = v
		}
	}

	if len(groupedConnections) == 0 {
		state.setConnectionRows(relKey, map[string]map[string]interface{}{})
		return r.buildConnectionResult(p.Context, nil, nil, false, false), true, nil
	}
	state.setConnectionRows(relKey, groupedConnections)

	if result, ok := groupedConnections[currentParentTupleKey]; ok {
		if nodes, ok := result["nodes"].([]map[string]interface{}); ok {
			seedBatchRows(p, nodes)
		}
		return result, true, nil
	}
	return r.buildConnectionResult(p.Context, nil, nil, false, false), true, nil
}

func (r *Resolver) tryBatchEdgeListConnection(p graphql.ResolveParams, table introspection.Table, rel introspection.Relationship, pkValues []interface{}) (map[string]interface{}, bool, error) {
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

	junctionTable, err := r.findTable(rel.JunctionTable)
	if err != nil {
		return nil, true, fmt.Errorf("failed to find junction table %s: %w", rel.JunctionTable, err)
	}

	pkCols := introspection.PrimaryKeyColumns(junctionTable)
	if len(pkCols) == 0 {
		if metrics != nil {
			metrics.RecordBatchSkipped(p.Context, relationEdgeList, "no_primary_key")
		}
		return nil, false, nil
	}

	field := firstFieldAST(p.Info.FieldASTs)
	if field == nil {
		return nil, true, fmt.Errorf("missing field AST")
	}

	first, err := planner.ParseFirstWithDefault(p.Args, r.defaultLimit)
	if err != nil {
		return nil, true, err
	}

	orderBy, err := planner.ParseOrderBy(junctionTable, p.Args)
	if err != nil {
		return nil, true, err
	}
	if orderBy == nil {
		pkColNames := make([]string, len(pkCols))
		pkDirections := make([]string, len(pkCols))
		for i, col := range pkCols {
			pkColNames[i] = col.Name
			pkDirections[i] = "ASC"
		}
		orderBy = &planner.OrderBy{
			Columns:    pkColNames,
			Directions: pkDirections,
		}
	}

	var whereClause *planner.WhereClause
	if whereArg, ok := p.Args["where"]; ok {
		if whereMap, ok := whereArg.(map[string]interface{}); ok {
			whereClause, err = planner.BuildWhereClauseWithSchema(r.dbSchema, junctionTable, whereMap)
			if err != nil {
				return nil, true, err
			}
			if whereClause != nil {
				if err := planner.ValidateWhereClauseIndexes(r.dbSchema, junctionTable, whereClause); err != nil {
					return nil, true, err
				}
			}
		}
	}

	selection := planner.SelectedColumnsForConnection(junctionTable, field, p.Info.Fragments, orderBy)
	orderByKey := planner.OrderByKey(junctionTable, orderBy.Columns)
	cursorCols := planner.CursorColumns(junctionTable, orderBy)
	localColumns := rel.EffectiveLocalColumns()
	junctionLocalColumns := rel.EffectiveJunctionLocalFKColumns()
	if len(localColumns) == 0 || len(localColumns) != len(pkValues) {
		return nil, true, fmt.Errorf("invalid edge-list local key mapping")
	}
	if len(junctionLocalColumns) != len(localColumns) {
		return nil, true, fmt.Errorf("invalid edge-list junction local key mapping")
	}
	currentParentTupleKey := tupleKeyFromValues(pkValues)

	relKey := fmt.Sprintf(
		"%s|%s|%s|%s|%s|%s",
		table.Name,
		rel.JunctionTable,
		strings.Join(junctionLocalColumns, ","),
		orderByKey,
		columnsKey(selection),
		stableArgsKey(p.Args),
	)

	if cached := state.getConnectionRows(relKey); cached != nil {
		state.IncrementCacheHit()
		if metrics != nil {
			metrics.RecordBatchCacheHit(p.Context, relationEdgeList)
		}
		if result, ok := cached[currentParentTupleKey]; ok {
			if nodes, ok := result["nodes"].([]map[string]interface{}); ok {
				seedBatchRows(p, nodes)
			}
			return result, true, nil
		}
		return r.buildConnectionResult(p.Context, nil, nil, false, false), true, nil
	}
	state.IncrementCacheMiss()
	if metrics != nil {
		metrics.RecordBatchCacheMiss(p.Context, relationEdgeList)
	}

	parentFields := make([]string, len(localColumns))
	for i, colName := range localColumns {
		parentFields[i] = graphQLFieldNameForColumn(table, colName)
	}
	parentTuples := uniqueParentTuples(parentRows, parentFields)
	if len(parentTuples) == 0 {
		state.setConnectionRows(relKey, map[string]map[string]interface{}{})
		return r.buildConnectionResult(p.Context, nil, nil, false, false), true, nil
	}
	parentValueByKey := make(map[string]planner.ParentTuple, len(parentTuples))
	for _, tuple := range parentTuples {
		parentValueByKey[tupleKeyFromValues(tuple.Values)] = tuple
	}

	chunks := chunkParentTuples(parentTuples, batchMaxInClause)
	if metrics != nil {
		metrics.RecordBatchQueriesSaved(p.Context, listBatchQueriesSaved(len(parentTuples), len(chunks)), relationEdgeList)
	}

	parentAliases := planner.BatchParentAliases(len(junctionLocalColumns))
	bp := batchConnectionPlan{
		table:         junctionTable,
		selection:     selection,
		orderBy:       orderBy,
		orderByKey:    orderByKey,
		cursorCols:    cursorCols,
		first:         first,
		parentAliases: parentAliases,
		relation:      relationEdgeList,
	}
	groupedConnections := make(map[string]map[string]interface{})
	for _, chunk := range chunks {
		partial, err := runBatchConnectionChunks(
			p.Context, r, bp, len(chunk), metrics,
			func() (planner.SQLQuery, error) {
				return planner.PlanEdgeListConnectionBatch(junctionTable, junctionLocalColumns, selection, chunk, first, orderBy, whereClause)
			},
			func(results []map[string]interface{}) map[string][]map[string]interface{} {
				return groupByAliases(results, parentAliases)
			},
			func(parentID string) (planner.SQLQuery, planner.SQLQuery, error) {
				tuple := parentValueByKey[parentID]
				count, err := planner.BuildEdgeListCountSQL(junctionTable, junctionLocalColumns, tuple.Values, whereClause)
				if err != nil {
					return planner.SQLQuery{}, planner.SQLQuery{}, err
				}
				agg, err := planner.BuildEdgeListAggregateBaseSQL(junctionTable, junctionLocalColumns, tuple.Values, whereClause)
				return count, agg, err
			},
		)
		if errors.Is(err, errBatchSkip) {
			return nil, false, nil
		}
		if err != nil {
			return nil, true, err
		}
		for k, v := range partial {
			groupedConnections[k] = v
		}
	}

	if len(groupedConnections) == 0 {
		state.setConnectionRows(relKey, map[string]map[string]interface{}{})
		return r.buildConnectionResult(p.Context, nil, nil, false, false), true, nil
	}
	state.setConnectionRows(relKey, groupedConnections)

	if result, ok := groupedConnections[currentParentTupleKey]; ok {
		if nodes, ok := result["nodes"].([]map[string]interface{}); ok {
			seedBatchRows(p, nodes)
		}
		return result, true, nil
	}
	return r.buildConnectionResult(p.Context, nil, nil, false, false), true, nil
}

func (r *Resolver) tryBatchManyToOne(p graphql.ResolveParams, table introspection.Table, rel introspection.Relationship, fkValues []interface{}) (map[string]interface{}, bool, error) {
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
	remoteColumns := rel.EffectiveRemoteColumns()
	localColumns := rel.EffectiveLocalColumns()
	if len(remoteColumns) == 0 || len(remoteColumns) != len(localColumns) || len(remoteColumns) != len(fkValues) {
		return nil, true, fmt.Errorf("invalid many-to-one batch mapping")
	}

	relKey := fmt.Sprintf("%s|%s|%s|%s", relatedTable.Name, strings.Join(remoteColumns, ","), parentKey, columnsKey(selection))
	if cached := state.getChildRows(relKey); cached != nil {
		state.IncrementCacheHit()
		if metrics != nil {
			metrics.RecordBatchCacheHit(p.Context, relationManyToOne)
		}
		return firstGroupedRecordByTuple(cached, fkValues), true, nil
	}
	state.IncrementCacheMiss()
	if metrics != nil {
		metrics.RecordBatchCacheMiss(p.Context, relationManyToOne)
	}

	parentFields := make([]string, len(localColumns))
	for i, colName := range localColumns {
		parentFields[i] = graphQLFieldNameForColumn(table, colName)
	}
	parentTuples := uniqueParentTuples(parentRows, parentFields)
	if len(parentTuples) == 0 {
		state.setChildRows(relKey, map[string][]map[string]interface{}{})
		return nil, true, nil
	}

	chunks := chunkParentTuples(parentTuples, batchMaxInClause)
	if metrics != nil {
		metrics.RecordBatchQueriesSaved(p.Context, listBatchQueriesSaved(len(parentTuples), len(chunks)), relationManyToOne)
	}

	grouped := make(map[string][]map[string]interface{})
	parentAliases := planner.BatchParentAliases(len(remoteColumns))
	for _, chunk := range chunks {
		if metrics != nil {
			metrics.RecordBatchParentCount(p.Context, int64(len(chunk)), relationManyToOne)
		}
		planned, err := planner.PlanManyToOneBatch(relatedTable, selection, remoteColumns, chunk)
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
		results, err := scanRowsWithExtras(rows, selection, parentAliases)
		rows.Close()
		if err != nil {
			return nil, true, err
		}
		if metrics != nil {
			metrics.RecordBatchResultRows(p.Context, int64(len(results)), relationManyToOne)
		}

		mergeGrouped(grouped, groupByAliases(results, parentAliases))
	}
	if len(grouped) == 0 {
		state.setChildRows(relKey, map[string][]map[string]interface{}{})
		return nil, true, nil
	}
	state.setChildRows(relKey, grouped)

	return firstGroupedRecordByTuple(grouped, fkValues), true, nil
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

func uniqueParentTuples(rows []map[string]interface{}, keys []string) []planner.ParentTuple {
	if len(keys) == 0 {
		return nil
	}
	seen := make(map[string]struct{})
	tuples := make([]planner.ParentTuple, 0, len(rows))
	for _, row := range rows {
		values := make([]interface{}, len(keys))
		missing := false
		for i, key := range keys {
			value := row[key]
			if value == nil {
				missing = true
				break
			}
			values[i] = value
		}
		if missing {
			continue
		}
		tupleKey := tupleKeyFromValues(values)
		if _, ok := seen[tupleKey]; ok {
			continue
		}
		seen[tupleKey] = struct{}{}
		tuples = append(tuples, planner.ParentTuple{Values: values})
	}
	return tuples
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

func groupByAliases(rows []map[string]interface{}, aliases []string) map[string][]map[string]interface{} {
	grouped := make(map[string][]map[string]interface{})
	for _, row := range rows {
		values := make([]interface{}, len(aliases))
		for i, alias := range aliases {
			values[i] = row[alias]
			delete(row, alias)
		}
		key := tupleKeyFromValues(values)
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

func chunkParentTuples(values []planner.ParentTuple, max int) [][]planner.ParentTuple {
	if len(values) == 0 {
		return nil
	}
	if max <= 0 || len(values) <= max {
		return [][]planner.ParentTuple{values}
	}
	chunks := make([][]planner.ParentTuple, 0, (len(values)+max-1)/max)
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
	return strings.Join(orderBy.Columns, ",") + ":" + strings.Join(orderBy.Directions, ",")
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
	return firstGroupedRecordByTuple(grouped, []interface{}{key})
}

func firstGroupedRecordByTuple(grouped map[string][]map[string]interface{}, values []interface{}) map[string]interface{} {
	if grouped == nil {
		return nil
	}
	rows := grouped[tupleKeyFromValues(values)]
	if len(rows) == 0 {
		return nil
	}
	return rows[0]
}

func tupleKeyFromValues(values []interface{}) string {
	return encodeCanonicalValue(values)
}

const batchParentKeyField = "__batch_parent_key"

var batchMaxInClause = 1000

const (
	relationOneToMany  = "one_to_many"
	relationManyToOne  = "many_to_one"
	relationManyToMany = "many_to_many"
	relationEdgeList   = "edge_list"
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
			row[fieldName] = convertColumnValue(col, values[i])
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
			row[fieldName] = convertColumnValue(col, values[i])
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

func sourceValuesForColumns(table introspection.Table, source map[string]interface{}, columns []string) ([]interface{}, bool) {
	if len(columns) == 0 {
		return nil, false
	}
	values := make([]interface{}, len(columns))
	for i, colName := range columns {
		fieldName := graphQLFieldNameForColumn(table, colName)
		value := source[fieldName]
		if value == nil {
			return nil, false
		}
		values[i] = value
	}
	return values, true
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

func (r *Resolver) uuidColumnResolver(col introspection.Column) graphql.FieldResolveFn {
	fieldName := introspection.GraphQLFieldName(col)
	return func(p graphql.ResolveParams) (interface{}, error) {
		source, ok := p.Source.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("invalid source type for UUID field %s", fieldName)
		}
		raw := source[fieldName]
		if raw == nil {
			return nil, nil
		}
		normalized, err := normalizeUUIDResultValue(raw)
		if err != nil {
			return nil, fmt.Errorf("invalid UUID value for field %s: %w", fieldName, err)
		}
		return normalized, nil
	}
}

func normalizeUUIDResultValue(raw interface{}) (string, error) {
	switch v := raw.(type) {
	case string:
		_, canonical, err := uuidutil.ParseString(v)
		if err != nil {
			return "", err
		}
		return canonical, nil
	case []byte:
		if len(v) == 16 {
			_, canonical, err := uuidutil.ParseBytes(v)
			if err != nil {
				return "", err
			}
			return canonical, nil
		}
		_, canonical, err := uuidutil.ParseString(string(v))
		if err != nil {
			return "", err
		}
		return canonical, nil
	default:
		return "", fmt.Errorf("unsupported UUID value type %T", raw)
	}
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

func convertColumnValue(col introspection.Column, val interface{}) interface{} {
	// TiFlash/vector-index execution can return ENUM values as ordinals (1-based)
	// instead of their string literals. Coerce ordinals back to enum strings so
	// GraphQL enum serialization remains stable across storage engines.
	if strings.EqualFold(col.DataType, "enum") {
		return coerceEnumColumnValue(col, val)
	}

	switch introspection.EffectiveGraphQLType(col) {
	case sqltype.TypeBoolean:
		return coerceBooleanColumnValue(val)
	case sqltype.TypeSet:
		return parseSetColumnValue(val)
	case sqltype.TypeBytes:
		return val
	case sqltype.TypeUUID:
		// UUID normalization/validation is enforced by uuidColumnResolver so it can
		// return field-level GraphQL errors on malformed stored values.
		return val
	case sqltype.TypeVector:
		return val
	default:
		return convertValue(val)
	}
}

func coerceEnumColumnValue(col introspection.Column, val interface{}) interface{} {
	if val == nil {
		return nil
	}

	if b, ok := val.([]byte); ok {
		s := string(b)
		if mapped, ok := enumStringOrOrdinalValue(col, s); ok {
			return mapped
		}
		return s
	}

	if s, ok := val.(string); ok {
		if mapped, ok := enumStringOrOrdinalValue(col, s); ok {
			return mapped
		}
		return s
	}

	if ord, ok := enumOrdinalValue(val); ok {
		if mapped, ok := enumValueByOrdinal(col, ord); ok {
			return mapped
		}
	}

	return convertValue(val)
}

func enumStringOrOrdinalValue(col introspection.Column, raw string) (string, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", true
	}
	ord, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return raw, true
	}
	return enumValueByOrdinal(col, ord)
}

func enumValueByOrdinal(col introspection.Column, ord int64) (string, bool) {
	if ord <= 0 {
		return "", false
	}
	idx := int(ord - 1)
	if idx < 0 || idx >= len(col.EnumValues) {
		return "", false
	}
	return col.EnumValues[idx], true
}

func enumOrdinalValue(raw interface{}) (int64, bool) {
	switch v := raw.(type) {
	case int:
		return int64(v), true
	case int8:
		return int64(v), true
	case int16:
		return int64(v), true
	case int32:
		return int64(v), true
	case int64:
		return v, true
	case uint:
		return int64(v), true
	case uint8:
		return int64(v), true
	case uint16:
		return int64(v), true
	case uint32:
		return int64(v), true
	case uint64:
		if v > uint64(^uint64(0)>>1) {
			return 0, false
		}
		return int64(v), true
	case float32:
		f := float64(v)
		if math.Trunc(f) != f {
			return 0, false
		}
		return int64(v), true
	case float64:
		if math.Trunc(v) != v {
			return 0, false
		}
		return int64(v), true
	default:
		return 0, false
	}
}

func coerceBooleanColumnValue(val interface{}) interface{} {
	if val == nil {
		return nil
	}

	switch v := val.(type) {
	case bool:
		return v
	case int:
		return v != 0
	case int8:
		return v != 0
	case int16:
		return v != 0
	case int32:
		return v != 0
	case int64:
		return v != 0
	case uint:
		return v != 0
	case uint8:
		return v != 0
	case uint16:
		return v != 0
	case uint32:
		return v != 0
	case uint64:
		return v != 0
	case float32:
		return v != 0
	case float64:
		return v != 0
	case []byte:
		return coerceBooleanColumnValue(string(v))
	case string:
		raw := strings.TrimSpace(v)
		if raw == "" {
			return false
		}
		if parsed, err := strconv.ParseFloat(raw, 64); err == nil {
			return parsed != 0
		}
		if parsed, err := strconv.ParseBool(raw); err == nil {
			return parsed
		}
		return convertValue(v)
	default:
		return convertValue(v)
	}
}

func parseSetColumnValue(val interface{}) interface{} {
	if val == nil {
		return nil
	}

	var raw string
	switch v := val.(type) {
	case []byte:
		raw = string(v)
	case string:
		raw = v
	default:
		return convertValue(val)
	}

	if raw == "" {
		return []string{}
	}

	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		trimmed := strings.TrimSpace(part)
		if trimmed == "" {
			continue
		}
		out = append(out, trimmed)
	}
	return out
}

func (r *Resolver) mapColumnTypeToGraphQL(table introspection.Table, col *introspection.Column) graphql.Output {
	effectiveType := introspection.EffectiveGraphQLType(*col)
	if effectiveType == sqltype.TypeUUID {
		return r.uuidScalar()
	}
	if effectiveType == sqltype.TypeSet {
		if enumType := r.enumTypeForColumn(table, col); enumType != nil {
			return graphql.NewList(graphql.NewNonNull(enumType))
		}
		return graphql.NewList(graphql.NewNonNull(graphql.String))
	}
	if enumType := r.enumTypeForColumn(table, col); enumType != nil {
		return enumType
	}
	switch effectiveType {
	case sqltype.TypeJSON:
		return r.jsonScalar()
	case sqltype.TypeVector:
		return r.vectorScalar()
	case sqltype.TypeInt:
		return graphql.Int
	case sqltype.TypeBigInt:
		return r.bigIntScalar()
	case sqltype.TypeFloat:
		return graphql.Float
	case sqltype.TypeDecimal:
		return r.decimalScalar()
	case sqltype.TypeBoolean:
		return graphql.Boolean
	case sqltype.TypeDate:
		return r.dateScalar()
	case sqltype.TypeDateTime:
		return graphql.DateTime
	case sqltype.TypeTime:
		return r.timeScalar()
	case sqltype.TypeYear:
		return r.yearScalar()
	case sqltype.TypeBytes:
		return r.bytesScalar()
	case sqltype.TypeSet:
		return graphql.NewList(graphql.NewNonNull(graphql.String))
	default:
		return graphql.String
	}
}

func (r *Resolver) mapColumnTypeToGraphQLInput(table introspection.Table, col *introspection.Column) graphql.Input {
	effectiveType := introspection.EffectiveGraphQLType(*col)
	if effectiveType == sqltype.TypeUUID {
		return r.uuidScalar()
	}
	if effectiveType == sqltype.TypeSet {
		if enumType := r.enumTypeForColumn(table, col); enumType != nil {
			return graphql.NewList(graphql.NewNonNull(enumType))
		}
		return graphql.NewList(graphql.NewNonNull(graphql.String))
	}
	if enumType := r.enumTypeForColumn(table, col); enumType != nil {
		return enumType
	}
	switch effectiveType {
	case sqltype.TypeJSON:
		return r.jsonScalar()
	case sqltype.TypeVector:
		return r.vectorScalar()
	case sqltype.TypeInt:
		return graphql.Int
	case sqltype.TypeBigInt:
		return r.bigIntScalar()
	case sqltype.TypeFloat:
		return graphql.Float
	case sqltype.TypeDecimal:
		return r.decimalScalar()
	case sqltype.TypeBoolean:
		return graphql.Boolean
	case sqltype.TypeDate:
		return r.dateScalar()
	case sqltype.TypeDateTime:
		return graphql.DateTime
	case sqltype.TypeTime:
		return r.timeScalar()
	case sqltype.TypeYear:
		return r.yearScalar()
	case sqltype.TypeBytes:
		return r.bytesScalar()
	case sqltype.TypeSet:
		return graphql.NewList(graphql.NewNonNull(graphql.String))
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

		localColumns := rel.EffectiveLocalColumns()
		remoteColumns := rel.EffectiveRemoteColumns()
		if len(localColumns) == 0 || len(localColumns) != len(remoteColumns) {
			return nil, fmt.Errorf("invalid many-to-one mapping for %s", rel.GraphQLFieldName)
		}
		fkValues, ok := sourceValuesForColumns(table, source, localColumns)
		if !ok {
			return nil, nil // Nullable FK
		}

		if result, ok, err := r.tryBatchManyToOne(p, table, rel, fkValues); ok || err != nil {
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
			RelatedTable:  relatedTable,
			RemoteColumns: remoteColumns,
			Values:        fkValues,
			IsManyToOne:   true,
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
