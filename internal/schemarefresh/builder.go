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

// EffectiveNamespace returns the GraphQL namespace alias, defaulting to Name.
func (d DatabaseBuildEntry) EffectiveNamespace() string {
	if d.Namespace != "" {
		return d.Namespace
	}
	return d.Name
}

// BuildSchemaConfig defines inputs for schema assembly across one or more databases.
type BuildSchemaConfig struct {
	Queryer                introspection.Queryer
	Executor               dbexec.QueryExecutor
	Databases              []DatabaseBuildEntry
	GlobalFilters          schemafilter.Config
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

func buildNamespaceConfig(entries []DatabaseBuildEntry, namer *naming.Namer) (map[string]string, map[string]naming.Config, bool, error) {
	namespaceMap := make(map[string]string, len(entries))
	namingPerDB := make(map[string]naming.Config, len(entries))
	normalizedNamespaceOwners := make(map[string]string, len(entries))
	namespacedRoot := len(entries) > 1
	for _, entry := range entries {
		ns := entry.EffectiveNamespace()
		if entry.Namespace != "" {
			namespacedRoot = true
		}
		normalizedNS := namer.ToGraphQLTypeName(ns)
		if prev, ok := normalizedNamespaceOwners[normalizedNS]; ok && prev != entry.Name {
			return nil, nil, false, fmt.Errorf("databases %q and %q both normalize to the GraphQL namespace %q", prev, entry.Name, normalizedNS)
		}
		normalizedNamespaceOwners[normalizedNS] = entry.Name
		namespaceMap[entry.Name] = ns
		if entry.Naming != nil {
			namingPerDB[entry.Name] = *entry.Naming
		}
	}
	return namespaceMap, namingPerDB, namespacedRoot, nil
}

// BuildSchema runs the canonical schema assembly pipeline used by runtime and tests.
func BuildSchema(ctx context.Context, cfg BuildSchemaConfig) (*BuildSchemaResult, error) {
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
	namespaceMap, namingPerDB, namespacedRoot, err := buildNamespaceConfig(cfg.Databases, namer)
	if err != nil {
		return nil, err
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

	// 7. GraphQL type naming.
	namingNamespaceMap := map[string]string(nil)
	if namespacedRoot {
		namingNamespaceMap = namespaceMap
	}
	schemanaming.ApplyWithNamespaces(merged, namer, namingNamespaceMap, namingPerDB)

	// 8. Build resolver + GraphQL schema.
	res := resolver.NewResolverWithConfig(cfg.Executor, merged, cfg.Limits, cfg.DefaultLimit, resolver.ResolverConfig{
		Filters:        cfg.GlobalFilters,
		FiltersPerDB:   filtersPerDB,
		NamespaceMap:   namespaceMap,
		NamespacedRoot: namespacedRoot,
		Naming:         cfg.Naming,
	})
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
