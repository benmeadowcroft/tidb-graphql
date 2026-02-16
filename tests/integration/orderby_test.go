//go:build integration
// +build integration

package integration

import (
	"context"
	"testing"

	"tidb-graphql/internal/testutil/tidbcloud"

	"github.com/graphql-go/graphql"
	"github.com/stretchr/testify/require"
)

func TestOrderByIndexedPrefix(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	testDB := tidbcloud.NewTestDB(t)
	testDB.LoadSchema(t, "../fixtures/simple_schema.sql")
	testDB.LoadFixtures(t, "../fixtures/simple_seed.sql")

	_, err := testDB.DB.Exec(`
		UPDATE posts SET created_at = CASE id
			WHEN 1 THEN '2024-01-01 00:00:00'
			WHEN 2 THEN '2024-01-02 00:00:00'
			WHEN 3 THEN '2024-01-01 00:00:00'
			WHEN 4 THEN '2024-01-03 00:00:00'
		END
	`)
	require.NoError(t, err)

	schema := buildGraphQLSchema(t, testDB)

	query := `
		{
			posts(orderBy: { userId_createdAt: ASC }, first: 10) {
				nodes {
					databaseId
				}
			}
		}
	`
	result := graphql.Do(graphql.Params{
		Schema:        schema,
		RequestString: query,
		Context:       context.Background(),
	})
	require.Empty(t, result.Errors)

	data := result.Data.(map[string]interface{})
	posts := requireCollectionNodes(t, data, "posts")
	require.Len(t, posts, 4)

	require.EqualValues(t, 1, posts[0].(map[string]interface{})["databaseId"])
	require.EqualValues(t, 2, posts[1].(map[string]interface{})["databaseId"])
	require.EqualValues(t, 3, posts[2].(map[string]interface{})["databaseId"])
	require.EqualValues(t, 4, posts[3].(map[string]interface{})["databaseId"])
}

func TestOrderByNonLeftmostRejected(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	testDB := tidbcloud.NewTestDB(t)
	testDB.LoadSchema(t, "../fixtures/simple_schema.sql")
	testDB.LoadFixtures(t, "../fixtures/simple_seed.sql")

	schema := buildGraphQLSchema(t, testDB)

	query := `
		{
			posts(orderBy: { createdAt: ASC }, first: 10) {
				nodes {
					id
				}
			}
		}
	`
	result := graphql.Do(graphql.Params{
		Schema:        schema,
		RequestString: query,
		Context:       context.Background(),
	})
	require.NotEmpty(t, result.Errors)
}
