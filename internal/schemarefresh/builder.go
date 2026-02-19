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
