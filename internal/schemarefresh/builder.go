package schemarefresh

import (
	"context"
	"fmt"

	"tidb-graphql/internal/dbexec"
	"tidb-graphql/internal/introspection"
	"tidb-graphql/internal/junction"
	"tidb-graphql/internal/naming"
	"tidb-graphql/internal/planner"
	"tidb-graphql/internal/resolver"
	"tidb-graphql/internal/schemafilter"
	"tidb-graphql/internal/schemanaming"

	"github.com/graphql-go/graphql"
)

// DatabaseBuildEntry describes one database schema to include in a multi-database
// schema build. It contains schema-level behaviour only — no connection settings.
type DatabaseBuildEntry struct {
	// Name is the SQL TABLE_SCHEMA name (required).
	Name string
	// Namespace is the GraphQL namespace alias. Defaults to Name when empty.
	// When at least one entry sets a Namespace (or there are multiple entries),
	// namespace-prefixed type names are generated: e.g. Shop_Order.
	Namespace string
	// Filters are per-database schema filter overrides.
	// When nil the build-level GlobalFilters apply.
	Filters *schemafilter.Config
	// Naming holds per-database naming overrides (type_overrides, plural_overrides,
	// singular_overrides). When nil the build-level Naming config applies.
	Naming *naming.Config
}

// BuildSchemaConfig defines inputs for shared schema assembly.
type BuildSchemaConfig struct {
	Queryer                introspection.Queryer
	Executor               dbexec.QueryExecutor
	DatabaseName           string
	Filters                schemafilter.Config
	UUIDColumns            map[string][]string
	TinyInt1BooleanColumns map[string][]string
	TinyInt1IntColumns     map[string][]string
	Naming                 naming.Config
	Limits                 *planner.PlanLimits
	DefaultLimit           int
	VectorRequireIndex     bool
	VectorMaxTopK          int
}

// BuildSchemaResult contains schema artifacts produced by BuildSchema.
type BuildSchemaResult struct {
	DBSchema      *introspection.Schema
	GraphQLSchema graphql.Schema
}

// BuildSchema runs the canonical schema assembly pipeline used by runtime and tests.
func BuildSchema(ctx context.Context, cfg BuildSchemaConfig) (*BuildSchemaResult, error) {
	if cfg.Queryer == nil {
		return nil, fmt.Errorf("schema builder requires an introspection queryer")
	}
	if cfg.Executor == nil {
		return nil, fmt.Errorf("schema builder requires a query executor")
	}

	dbSchema, err := introspection.IntrospectDatabaseContext(ctx, cfg.Queryer, cfg.DatabaseName)
	if err != nil {
		return nil, fmt.Errorf("failed to introspect database: %w", err)
	}

	schemafilter.Apply(ctx, dbSchema, cfg.Filters)

	if err := introspection.ApplyTinyInt1TypeOverrides(dbSchema, cfg.TinyInt1BooleanColumns, cfg.TinyInt1IntColumns); err != nil {
		return nil, fmt.Errorf("failed to apply tinyint(1) type mappings: %w", err)
	}

	if err := introspection.ApplyUUIDTypeOverrides(dbSchema, cfg.UUIDColumns); err != nil {
		return nil, fmt.Errorf("failed to apply UUID type mappings: %w", err)
	}

	junctions := junction.ClassifyJunctions(dbSchema)
	dbSchema.Junctions = junctions.ToIntrospectionMap()

	namer := naming.New(cfg.Naming, nil)
	if err := introspection.RebuildRelationshipsWithJunctions(ctx, dbSchema, namer, dbSchema.Junctions); err != nil {
		return nil, fmt.Errorf("failed to rebuild relationships: %w", err)
	}
	schemanaming.Apply(dbSchema, namer)

	res := resolver.NewResolver(cfg.Executor, dbSchema, cfg.Limits, cfg.DefaultLimit, cfg.Filters, cfg.Naming)
	if cfg.VectorRequireIndex || cfg.VectorMaxTopK > 0 {
		res.SetVectorSearchConfig(resolver.VectorSearchConfig{
			RequireIndex: cfg.VectorRequireIndex,
			MaxTopK:      cfg.VectorMaxTopK,
		})
	}
	graphqlSchema, err := res.BuildGraphQLSchema()
	if err != nil {
		return nil, fmt.Errorf("failed to build GraphQL schema: %w", err)
	}

	return &BuildSchemaResult{
		DBSchema:      dbSchema,
		GraphQLSchema: graphqlSchema,
	}, nil
}

// BuildMultiDatabaseConfig defines inputs for multi-database schema assembly.
type BuildMultiDatabaseConfig struct {
	Queryer  introspection.Queryer
	Executor dbexec.QueryExecutor
	// Databases lists the SQL schemas to introspect (at least one required).
	Databases []DatabaseBuildEntry
	// GlobalFilters are the default schema filters applied to every database
	// that does not provide its own DatabaseBuildEntry.Filters override.
	GlobalFilters schemafilter.Config
	// Type mapping overrides applied to every database.
	UUIDColumns            map[string][]string
	TinyInt1BooleanColumns map[string][]string
	TinyInt1IntColumns     map[string][]string
	Naming                 naming.Config
	Limits                 *planner.PlanLimits
	DefaultLimit           int
	VectorRequireIndex     bool
	VectorMaxTopK          int
}

// BuildMultiDatabaseSchema introspects each database, merges the schemas, resolves
// cross-database relationships, and builds a unified GraphQL schema.
//
// Pipeline per database:
//  1. IntrospectDatabaseContext — tables get Key.Database set to entry.Name.
//  2. Per-db filter application (falls back to GlobalFilters).
//  3. Type override application (UUID, tinyint(1)).
//  4. Intra-db junction classification + relationship building.
//
// After all databases are processed:
//  5. Schemas are merged into a single introspection.Schema.
//  6. ResolveCrossDatabaseRelationships adds cross-db one-to-many relationships.
//  7. schemanaming.Apply names GraphQL types (namespace-aware naming in Phase 3).
//  8. A Resolver is built with per-db filter overrides and a GraphQL schema produced.
func BuildMultiDatabaseSchema(ctx context.Context, cfg BuildMultiDatabaseConfig) (*BuildSchemaResult, error) {
	if cfg.Queryer == nil {
		return nil, fmt.Errorf("schema builder requires an introspection queryer")
	}
	if cfg.Executor == nil {
		return nil, fmt.Errorf("schema builder requires a query executor")
	}
	if len(cfg.Databases) == 0 {
		return nil, fmt.Errorf("schema builder requires at least one database entry")
	}

	namer := naming.New(cfg.Naming, nil)
	filtersPerDB := make(map[string]schemafilter.Config, len(cfg.Databases))

	// Build namespace map and per-db naming map from the database entries.
	// Namespace-prefixed type names are generated when there are multiple databases
	// OR when any single database has an explicit Namespace set.
	namespaceMap := make(map[string]string, len(cfg.Databases))
	namingPerDB := make(map[string]naming.Config, len(cfg.Databases))
	normalizedNamespaceOwners := make(map[string]string, len(cfg.Databases))
	useNamespaces := len(cfg.Databases) > 1
	for _, entry := range cfg.Databases {
		ns := entry.Namespace
		if ns == "" {
			ns = entry.Name
		} else {
			useNamespaces = true // explicit namespace on a single-db entry triggers it too
		}
		normalizedNS := namer.ToGraphQLTypeName(ns)
		if prev, ok := normalizedNamespaceOwners[normalizedNS]; ok && prev != entry.Name {
			return nil, fmt.Errorf("databases %q and %q both normalize to the GraphQL namespace %q", prev, entry.Name, normalizedNS)
		}
		normalizedNamespaceOwners[normalizedNS] = entry.Name
		namespaceMap[entry.Name] = ns
		if entry.Naming != nil {
			namingPerDB[entry.Name] = *entry.Naming
		}
	}

	var allTables []introspection.Table
	mergedJunctions := make(introspection.JunctionMap)

	for _, entry := range cfg.Databases {
		// 1. Introspect — tables will have Key.Database = entry.Name.
		dbSchema, err := introspection.IntrospectDatabaseContext(ctx, cfg.Queryer, entry.Name)
		if err != nil {
			return nil, fmt.Errorf("failed to introspect database %q: %w", entry.Name, err)
		}

		// 2. Per-db filters, falling back to global.
		filters := cfg.GlobalFilters
		if entry.Filters != nil {
			filters = *entry.Filters
		}
		schemafilter.Apply(ctx, dbSchema, filters)
		filtersPerDB[entry.Name] = filters

		// 3. Type overrides.
		if err := introspection.ApplyTinyInt1TypeOverrides(dbSchema, cfg.TinyInt1BooleanColumns, cfg.TinyInt1IntColumns); err != nil {
			return nil, fmt.Errorf("failed to apply tinyint(1) type mappings for %q: %w", entry.Name, err)
		}
		if err := introspection.ApplyUUIDTypeOverrides(dbSchema, cfg.UUIDColumns); err != nil {
			return nil, fmt.Errorf("failed to apply UUID type mappings for %q: %w", entry.Name, err)
		}

		// 4. Intra-db junction classification + relationship building.
		// Cross-db FKs produce many-to-one relationships (IsCrossDatabase=true);
		// their reverse one-to-many relationships are added in step 6.
		junctions := junction.ClassifyJunctions(dbSchema)
		dbSchema.Junctions = junctions.ToIntrospectionMap()
		if err := introspection.RebuildRelationshipsWithJunctions(ctx, dbSchema, namer, dbSchema.Junctions); err != nil {
			return nil, fmt.Errorf("failed to rebuild relationships for %q: %w", entry.Name, err)
		}

		allTables = append(allTables, dbSchema.Tables...)
		for k, v := range dbSchema.Junctions {
			mergedJunctions[k] = v
		}
	}

	// 5. Merge.
	merged := &introspection.Schema{
		Tables:    allTables,
		Junctions: mergedJunctions,
	}

	// 6. Cross-database one-to-many relationships.
	if err := introspection.ResolveCrossDatabaseRelationships(merged, namer); err != nil {
		return nil, fmt.Errorf("failed to resolve cross-database relationships: %w", err)
	}

	// 7. GraphQL type naming — namespace-aware when multiple databases are configured
	// or when any entry carries an explicit Namespace alias.
	if useNamespaces {
		schemanaming.ApplyWithNamespaces(merged, namer, namespaceMap, namingPerDB)
	} else {
		schemanaming.Apply(merged, namer)
	}

	// 8. Build resolver + GraphQL schema.
	res := resolver.NewResolver(cfg.Executor, merged, cfg.Limits, cfg.DefaultLimit, cfg.GlobalFilters, cfg.Naming)
	res.SetFiltersPerDB(filtersPerDB)
	if useNamespaces {
		res.SetNamespaceMap(namespaceMap)
	}
	if cfg.VectorRequireIndex || cfg.VectorMaxTopK > 0 {
		res.SetVectorSearchConfig(resolver.VectorSearchConfig{
			RequireIndex: cfg.VectorRequireIndex,
			MaxTopK:      cfg.VectorMaxTopK,
		})
	}
	graphqlSchema, err := res.BuildGraphQLSchema()
	if err != nil {
		return nil, fmt.Errorf("failed to build GraphQL schema: %w", err)
	}

	return &BuildSchemaResult{
		DBSchema:      merged,
		GraphQLSchema: graphqlSchema,
	}, nil
}
