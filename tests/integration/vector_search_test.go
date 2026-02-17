//go:build integration
// +build integration

package integration

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"tidb-graphql/internal/dbexec"
	"tidb-graphql/internal/naming"
	"tidb-graphql/internal/schemafilter"
	"tidb-graphql/internal/schemarefresh"
	"tidb-graphql/internal/testutil/tidbcloud"

	"github.com/graphql-go/graphql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestVectorSearchCursorPagination_NoDuplicatesOrGaps(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	testDB := tidbcloud.NewTestDB(t)
	loadVectorDocsFixture(t, testDB)

	schema := buildVectorGraphQLSchema(t, testDB)

	const query = `
		query($after: String) {
			searchDocsByEmbeddingVector(vector: [1, 0, 0], metric: L2, first: 2, after: $after) {
				edges {
					cursor
					node {
						databaseId
					}
				}
				pageInfo {
					hasNextPage
					endCursor
				}
			}
		}
	`

	page1 := graphql.Do(graphql.Params{
		Schema:        schema,
		RequestString: query,
		Context:       context.Background(),
	})
	require.Empty(t, page1.Errors, "page 1 should not return errors")

	edges1, pageInfo1 := requireVectorConnectionPayload(t, page1.Data)
	require.Len(t, edges1, 2)
	assert.Equal(t, []string{"1", "2"}, vectorNodeIDs(t, edges1))
	assert.True(t, requireBool(t, pageInfo1["hasNextPage"]))
	endCursor1 := requireString(t, pageInfo1["endCursor"])
	require.NotEmpty(t, endCursor1)

	page2 := graphql.Do(graphql.Params{
		Schema:        schema,
		RequestString: query,
		VariableValues: map[string]interface{}{
			"after": endCursor1,
		},
		Context: context.Background(),
	})
	require.Empty(t, page2.Errors, "page 2 should not return errors")

	edges2, pageInfo2 := requireVectorConnectionPayload(t, page2.Data)
	require.Len(t, edges2, 2)
	assert.Equal(t, []string{"3", "4"}, vectorNodeIDs(t, edges2))
	assert.False(t, requireBool(t, pageInfo2["hasNextPage"]))

	combined := append(vectorNodeIDs(t, edges1), vectorNodeIDs(t, edges2)...)
	assert.Equal(t, []string{"1", "2", "3", "4"}, combined)
}

func TestVectorSearchCursorPagination_WithWhere(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	testDB := tidbcloud.NewTestDB(t)
	loadVectorDocsFixture(t, testDB)

	schema := buildVectorGraphQLSchema(t, testDB)

	const query = `
		query($after: String) {
			searchDocsByEmbeddingVector(
				vector: [1, 0, 0]
				metric: L2
				where: { category: { eq: "alpha" } }
				first: 1
				after: $after
			) {
				edges {
					cursor
					node {
						databaseId
						category
					}
				}
				pageInfo {
					hasNextPage
					endCursor
				}
			}
		}
	`

	page1 := graphql.Do(graphql.Params{
		Schema:        schema,
		RequestString: query,
		Context:       context.Background(),
	})
	require.Empty(t, page1.Errors, "page 1 should not return errors")

	edges1, pageInfo1 := requireVectorConnectionPayload(t, page1.Data)
	require.Len(t, edges1, 1)
	assert.Equal(t, "1", vectorNodeID(t, edges1[0]))
	assert.Equal(t, "alpha", vectorNodeCategory(t, edges1[0]))
	assert.True(t, requireBool(t, pageInfo1["hasNextPage"]))
	endCursor1 := requireString(t, pageInfo1["endCursor"])
	require.NotEmpty(t, endCursor1)

	page2 := graphql.Do(graphql.Params{
		Schema:        schema,
		RequestString: query,
		VariableValues: map[string]interface{}{
			"after": endCursor1,
		},
		Context: context.Background(),
	})
	require.Empty(t, page2.Errors, "page 2 should not return errors")

	edges2, pageInfo2 := requireVectorConnectionPayload(t, page2.Data)
	require.Len(t, edges2, 1)
	assert.Equal(t, "2", vectorNodeID(t, edges2[0]))
	assert.Equal(t, "alpha", vectorNodeCategory(t, edges2[0]))
	assert.True(t, requireBool(t, pageInfo2["hasNextPage"]))
	assert.NotEqual(t, vectorNodeID(t, edges1[0]), vectorNodeID(t, edges2[0]))
}

func TestVectorSearchCursorPagination_InvalidCursor(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	testDB := tidbcloud.NewTestDB(t)
	loadVectorDocsFixture(t, testDB)

	schema := buildVectorGraphQLSchema(t, testDB)

	const query = `
		query($after: String) {
			searchDocsByEmbeddingVector(vector: [1, 0, 0], metric: L2, first: 2, after: $after) {
				edges {
					cursor
				}
			}
		}
	`

	result := graphql.Do(graphql.Params{
		Schema:        schema,
		RequestString: query,
		VariableValues: map[string]interface{}{
			"after": "not-a-valid-cursor",
		},
		Context: context.Background(),
	})

	require.NotEmpty(t, result.Errors, "invalid cursor should be rejected")
	require.Contains(t, strings.ToLower(result.Errors[0].Message), "invalid after cursor")
}

func loadVectorDocsFixture(t *testing.T, testDB *tidbcloud.TestDB) {
	t.Helper()

	schemaSQL := `
		CREATE TABLE docs (
			id BIGINT PRIMARY KEY,
			category VARCHAR(32) NOT NULL,
			embedding VECTOR(3) NOT NULL,
			KEY idx_docs_category (category)
		)
	`

	if _, err := testDB.DB.Exec(schemaSQL); err != nil {
		if vectorFeatureUnavailable(err) {
			t.Skipf("Skipping vector search test: %v", err)
		}
		require.NoError(t, err)
	}

	insertSQL := `
		INSERT INTO docs (id, category, embedding)
		VALUES
			(1, 'alpha', '[1,0,0]'),
			(2, 'alpha', '[0.9,0,0]'),
			(3, 'beta', '[0.8,0,0]'),
			(4, 'alpha', '[0.7,0,0]')
	`
	if _, err := testDB.DB.Exec(insertSQL); err != nil {
		if vectorFeatureUnavailable(err) {
			t.Skipf("Skipping vector search test: %v", err)
		}
		require.NoError(t, err)
	}

	var distance float64
	err := testDB.DB.QueryRow("SELECT VEC_L2_DISTANCE(embedding, '[1,0,0]') FROM docs LIMIT 1").Scan(&distance)
	if err != nil {
		if vectorFeatureUnavailable(err) {
			t.Skipf("Skipping vector search test: %v", err)
		}
		require.NoError(t, err)
	}
}

func buildVectorGraphQLSchema(t *testing.T, testDB *tidbcloud.TestDB) graphql.Schema {
	t.Helper()

	executor := dbexec.NewStandardExecutor(testDB.DB)
	result, err := schemarefresh.BuildSchema(context.Background(), schemarefresh.BuildSchemaConfig{
		Queryer:                testDB.DB,
		Executor:               executor,
		DatabaseName:           testDB.DatabaseName,
		Filters:                schemafilter.Config{},
		UUIDColumns:            nil,
		TinyInt1BooleanColumns: nil,
		TinyInt1IntColumns:     nil,
		Naming:                 naming.DefaultConfig(),
		VectorRequireIndex:     false,
		VectorMaxTopK:          100,
	})
	require.NoError(t, err)
	return result.GraphQLSchema
}

func requireVectorConnectionPayload(t *testing.T, data interface{}) ([]map[string]interface{}, map[string]interface{}) {
	t.Helper()

	root, ok := data.(map[string]interface{})
	require.True(t, ok, "expected response data map")

	rawConn, ok := root["searchDocsByEmbeddingVector"]
	require.True(t, ok, "expected searchDocsByEmbeddingVector in response")
	conn, ok := rawConn.(map[string]interface{})
	require.True(t, ok, "expected vector search connection map")

	rawEdges, ok := conn["edges"].([]interface{})
	require.True(t, ok, "expected edges list")
	edges := make([]map[string]interface{}, len(rawEdges))
	for i, raw := range rawEdges {
		edge, ok := raw.(map[string]interface{})
		require.True(t, ok, "expected edge object")
		edges[i] = edge
	}

	pageInfo, ok := conn["pageInfo"].(map[string]interface{})
	require.True(t, ok, "expected pageInfo object")

	return edges, pageInfo
}

func vectorNodeIDs(t *testing.T, edges []map[string]interface{}) []string {
	t.Helper()
	out := make([]string, len(edges))
	for i, edge := range edges {
		out[i] = vectorNodeID(t, edge)
	}
	return out
}

func vectorNodeID(t *testing.T, edge map[string]interface{}) string {
	t.Helper()
	node := requireVectorNode(t, edge)
	return fmt.Sprintf("%v", node["databaseId"])
}

func vectorNodeCategory(t *testing.T, edge map[string]interface{}) string {
	t.Helper()
	node := requireVectorNode(t, edge)
	return fmt.Sprintf("%v", node["category"])
}

func requireVectorNode(t *testing.T, edge map[string]interface{}) map[string]interface{} {
	t.Helper()
	node, ok := edge["node"].(map[string]interface{})
	require.True(t, ok, "expected edge.node object")
	return node
}

func requireString(t *testing.T, value interface{}) string {
	t.Helper()
	s, ok := value.(string)
	require.True(t, ok, "expected string value")
	return s
}

func requireBool(t *testing.T, value interface{}) bool {
	t.Helper()
	b, ok := value.(bool)
	require.True(t, ok, "expected bool value")
	return b
}

func vectorFeatureUnavailable(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	if strings.Contains(msg, "unknown data type") && strings.Contains(msg, "vector") {
		return true
	}
	if strings.Contains(msg, "unknown function") && strings.Contains(msg, "vec_") {
		return true
	}
	if strings.Contains(msg, "doesn't exist") && strings.Contains(msg, "vec_") {
		return true
	}
	if strings.Contains(msg, "unsupported") && strings.Contains(msg, "vector") {
		return true
	}
	return false
}
