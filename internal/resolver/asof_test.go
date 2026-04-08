package resolver

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"tidb-graphql/internal/dbexec"
	"tidb-graphql/internal/introspection"
	"tidb-graphql/internal/naming"
	"tidb-graphql/internal/schemafilter"

	"github.com/graphql-go/graphql"
)

func TestBuildGraphQLSchema_IncludesAsOfDirective(t *testing.T) {
	users := introspection.Table{
		Name: "users",
		Columns: []introspection.Column{
			{Name: "id", IsPrimaryKey: true},
		},
	}
	renamePrimaryKeyID(&users)

	r := NewResolver(nil, &introspection.Schema{Tables: []introspection.Table{users}}, nil, 0, schemafilter.Config{}, naming.DefaultConfig())
	schema, err := r.BuildGraphQLSchema()
	if err != nil {
		t.Fatalf("BuildGraphQLSchema() error = %v", err)
	}

	directive := schema.Directive("asOf")
	if directive == nil {
		t.Fatalf("expected @asOf directive in schema")
	}
	if len(directive.Args) != 2 {
		t.Fatalf("directive arg count = %d, want 2", len(directive.Args))
	}
}

func TestAsOfRootAndNestedResolversPropagateSnapshot(t *testing.T) {
	executor := &snapshotRoutingExecutor{}
	r := NewResolver(executor, simpleAsOfSchema(), nil, 0, schemafilter.Config{}, naming.DefaultConfig())

	schema, err := r.BuildGraphQLSchema()
	if err != nil {
		t.Fatalf("BuildGraphQLSchema() error = %v", err)
	}

	result := graphql.Do(graphql.Params{
		Schema: schema,
		RequestString: `query {
			users @asOf(time: "2026-04-01T10:00:00Z") {
				nodes {
					databaseId
					posts {
						nodes {
							databaseId
						}
					}
				}
			}
		}`,
		Context: NewBatchingContext(context.Background()),
	})
	if len(result.Errors) > 0 {
		t.Fatalf("graphql errors: %+v", result.Errors)
	}

	if got := executor.snapshotQueries["current"]; got != 0 {
		t.Fatalf("current snapshot queries = %d, want 0", got)
	}
	if got := executor.snapshotQueries["snapshot"]; got != 2 {
		t.Fatalf("snapshot queries = %d, want 2", got)
	}
}

func TestAsOfMixedSnapshotsDoNotShareBatchCache(t *testing.T) {
	executor := &snapshotRoutingExecutor{}
	r := NewResolver(executor, simpleAsOfSchema(), nil, 0, schemafilter.Config{}, naming.DefaultConfig())

	schema, err := r.BuildGraphQLSchema()
	if err != nil {
		t.Fatalf("BuildGraphQLSchema() error = %v", err)
	}

	result := graphql.Do(graphql.Params{
		Schema: schema,
		RequestString: `query {
			current: users {
				nodes {
					databaseId
					posts {
						nodes {
							databaseId
						}
					}
				}
			}
			past: users @asOf(offsetSeconds: -600) {
				nodes {
					databaseId
					posts {
						nodes {
							databaseId
						}
					}
				}
			}
		}`,
		Context: NewBatchingContext(context.Background()),
	})
	if len(result.Errors) > 0 {
		t.Fatalf("graphql errors: %+v", result.Errors)
	}

	if got := executor.snapshotQueries["current"]; got != 2 {
		t.Fatalf("current queries = %d, want 2", got)
	}
	if got := executor.snapshotQueries["snapshot"]; got != 2 {
		t.Fatalf("snapshot queries = %d, want 2", got)
	}

	data, ok := result.Data.(map[string]any)
	if !ok {
		t.Fatalf("expected map data, got %T", result.Data)
	}
	current := nestedDatabaseID(t, data, "current")
	past := nestedDatabaseID(t, data, "past")
	if current == past {
		t.Fatalf("expected different nested post ids for current and past snapshots, both were %v", current)
	}
}

func TestAsOfTotalCountUsesSnapshotContext(t *testing.T) {
	executor := &fakeExecutor{
		responses: [][][]any{
			{{1}},
			{{1}},
		},
	}
	r := NewResolver(executor, simpleUsersOnlySchema(), nil, 0, schemafilter.Config{}, naming.DefaultConfig())

	schema, err := r.BuildGraphQLSchema()
	if err != nil {
		t.Fatalf("BuildGraphQLSchema() error = %v", err)
	}

	result := graphql.Do(graphql.Params{
		Schema: schema,
		RequestString: `query {
			users @asOf(time: "2026-04-01T10:00:00Z") {
				totalCount
				nodes {
					databaseId
				}
			}
		}`,
		Context: NewBatchingContext(context.Background()),
	})
	if len(result.Errors) > 0 {
		t.Fatalf("graphql errors: %+v", result.Errors)
	}
	if len(executor.ctxs) != 2 {
		t.Fatalf("query count = %d, want 2", len(executor.ctxs))
	}
	for i, ctx := range executor.ctxs {
		if _, ok := dbexec.SnapshotReadFromContext(ctx); !ok {
			t.Fatalf("query %d missing snapshot context", i)
		}
	}
}

func simpleAsOfSchema() *introspection.Schema {
	users := introspection.Table{
		Name: "users",
		Columns: []introspection.Column{
			{Name: "id", IsPrimaryKey: true},
		},
		Relationships: []introspection.Relationship{
			{
				IsOneToMany:      true,
				LocalColumns:     []string{"id"},
				RemoteTable:      "posts",
				RemoteColumns:    []string{"user_id"},
				GraphQLFieldName: "posts",
			},
		},
	}
	posts := introspection.Table{
		Name: "posts",
		Columns: []introspection.Column{
			{Name: "id", IsPrimaryKey: true},
			{Name: "user_id"},
		},
	}
	renamePrimaryKeyID(&users)
	renamePrimaryKeyID(&posts)
	return &introspection.Schema{Tables: []introspection.Table{users, posts}}
}

func simpleUsersOnlySchema() *introspection.Schema {
	users := introspection.Table{
		Name: "users",
		Columns: []introspection.Column{
			{Name: "id", IsPrimaryKey: true},
		},
	}
	renamePrimaryKeyID(&users)
	return &introspection.Schema{Tables: []introspection.Table{users}}
}

type snapshotRoutingExecutor struct {
	snapshotQueries map[string]int
}

func (e *snapshotRoutingExecutor) QueryContext(ctx context.Context, _ string, _ ...any) (dbexec.Rows, error) {
	if e.snapshotQueries == nil {
		e.snapshotQueries = map[string]int{}
	}
	key := "current"
	if _, ok := dbexec.SnapshotReadFromContext(ctx); ok {
		key = "snapshot"
	}
	e.snapshotQueries[key]++

	switch key {
	case "current":
		switch e.snapshotQueries[key] {
		case 1:
			return &fakeRows{rows: [][]any{{1}}}, nil
		case 2:
			return &fakeRows{rows: [][]any{{101, 1}}}, nil
		}
	case "snapshot":
		switch e.snapshotQueries[key] {
		case 1:
			return &fakeRows{rows: [][]any{{1}}}, nil
		case 2:
			return &fakeRows{rows: [][]any{{202, 1}}}, nil
		}
	}

	return &fakeRows{}, nil
}

func (e *snapshotRoutingExecutor) ExecContext(context.Context, string, ...any) (sql.Result, error) {
	return nil, nil
}

func (e *snapshotRoutingExecutor) BeginTx(context.Context) (dbexec.TxExecutor, error) {
	return nil, nil
}

func nestedDatabaseID(t *testing.T, data map[string]any, rootKey string) any {
	t.Helper()

	root, ok := data[rootKey].(map[string]any)
	if !ok {
		t.Fatalf("%s root result type = %T", rootKey, data[rootKey])
	}
	nodes, ok := root["nodes"].([]any)
	if !ok || len(nodes) == 0 {
		t.Fatalf("%s nodes = %#v", rootKey, root["nodes"])
	}
	node, ok := nodes[0].(map[string]any)
	if !ok {
		t.Fatalf("%s first node type = %T", rootKey, nodes[0])
	}
	posts, ok := node["posts"].(map[string]any)
	if !ok {
		t.Fatalf("%s posts type = %T", rootKey, node["posts"])
	}
	postNodes, ok := posts["nodes"].([]any)
	if !ok || len(postNodes) == 0 {
		t.Fatalf("%s post nodes = %#v", rootKey, posts["nodes"])
	}
	post, ok := postNodes[0].(map[string]any)
	if !ok {
		t.Fatalf("%s first post node type = %T", rootKey, postNodes[0])
	}
	return post["databaseId"]
}

func TestSnapshotReadIdentityStable(t *testing.T) {
	snapshot := dbexec.SnapshotRead{Time: time.Date(2026, 4, 1, 10, 0, 0, 0, time.UTC)}
	if got, want := snapshot.Identity(), "2026-04-01T10:00:00Z"; got != want {
		t.Fatalf("Identity() = %q, want %q", got, want)
	}
}
