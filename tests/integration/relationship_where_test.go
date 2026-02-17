//go:build integration
// +build integration

package integration

import (
	"context"
	"testing"

	"tidb-graphql/internal/testutil/tidbcloud"

	"github.com/graphql-go/graphql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRelationshipWhere_OneToManySomeAndNone(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	testDB := tidbcloud.NewTestDB(t)
	testDB.LoadSchema(t, "../fixtures/simple_schema.sql")
	testDB.LoadFixtures(t, "../fixtures/simple_seed.sql")

	schema := buildGraphQLSchema(t, testDB)

	someQuery := `
		{
			users(
				where: { posts: { some: { published: { eq: true } } } }
				orderBy: [{ databaseId: ASC }]
			) {
				nodes {
					databaseId
					username
				}
			}
		}
	`
	someResult := graphql.Do(graphql.Params{
		Schema:        schema,
		RequestString: someQuery,
		Context:       context.Background(),
	})
	require.Empty(t, someResult.Errors)
	someUsers := requireCollectionNodes(t, someResult.Data.(map[string]interface{}), "users")
	require.Len(t, someUsers, 2)
	assert.EqualValues(t, 1, someUsers[0].(map[string]interface{})["databaseId"])
	assert.EqualValues(t, 3, someUsers[1].(map[string]interface{})["databaseId"])

	noneQuery := `
		{
			users(
				where: { posts: { none: { published: { eq: false } } } }
				orderBy: [{ databaseId: ASC }]
			) {
				nodes {
					databaseId
				}
			}
		}
	`
	noneResult := graphql.Do(graphql.Params{
		Schema:        schema,
		RequestString: noneQuery,
		Context:       context.Background(),
	})
	require.Empty(t, noneResult.Errors)
	noneUsers := requireCollectionNodes(t, noneResult.Data.(map[string]interface{}), "users")
	require.Len(t, noneUsers, 2)
	assert.EqualValues(t, 1, noneUsers[0].(map[string]interface{})["databaseId"])
	assert.EqualValues(t, 3, noneUsers[1].(map[string]interface{})["databaseId"])
}

func TestRelationshipWhere_ManyToOneIsAndIsNull(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	testDB := tidbcloud.NewTestDB(t)
	testDB.LoadSchema(t, "../fixtures/simple_schema.sql")
	testDB.LoadFixtures(t, "../fixtures/simple_seed.sql")

	schema := buildGraphQLSchema(t, testDB)

	isQuery := `
		{
			posts(
				where: { user: { is: { username: { eq: "alice" } } } }
				orderBy: [{ databaseId: ASC }]
			) {
				nodes {
					databaseId
				}
			}
		}
	`
	isResult := graphql.Do(graphql.Params{
		Schema:        schema,
		RequestString: isQuery,
		Context:       context.Background(),
	})
	require.Empty(t, isResult.Errors)
	posts := requireCollectionNodes(t, isResult.Data.(map[string]interface{}), "posts")
	require.Len(t, posts, 2)
	assert.EqualValues(t, 1, posts[0].(map[string]interface{})["databaseId"])
	assert.EqualValues(t, 2, posts[1].(map[string]interface{})["databaseId"])

	isNullTrueQuery := `
		{
			posts(where: { user: { isNull: true } }) {
				nodes {
					databaseId
				}
			}
		}
	`
	isNullTrueResult := graphql.Do(graphql.Params{
		Schema:        schema,
		RequestString: isNullTrueQuery,
		Context:       context.Background(),
	})
	require.Empty(t, isNullTrueResult.Errors)
	assert.Empty(t, requireCollectionNodes(t, isNullTrueResult.Data.(map[string]interface{}), "posts"))

	isNullFalseQuery := `
		{
			posts(where: { user: { isNull: false } }, orderBy: [{ databaseId: ASC }]) {
				nodes {
					databaseId
				}
			}
		}
	`
	isNullFalseResult := graphql.Do(graphql.Params{
		Schema:        schema,
		RequestString: isNullFalseQuery,
		Context:       context.Background(),
	})
	require.Empty(t, isNullFalseResult.Errors)
	assert.Len(t, requireCollectionNodes(t, isNullFalseResult.Data.(map[string]interface{}), "posts"), 4)
}

func TestRelationshipWhere_ManyToManyAndEdgeListSome(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	testDB := tidbcloud.NewTestDB(t)
	testDB.LoadSchema(t, "../fixtures/junction_schema.sql")

	schema := buildGraphQLSchema(t, testDB)

	query := `
		{
			users(
				where: {
					roles: { some: { name: { eq: "admin" } } }
					projectMembers: { some: { roleLevel: { eq: ADMIN } } }
				}
				orderBy: [{ databaseId: ASC }]
			) {
				nodes {
					databaseId
					name
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
	users := requireCollectionNodes(t, result.Data.(map[string]interface{}), "users")
	require.Len(t, users, 1)
	assert.Equal(t, "Alice", users[0].(map[string]interface{})["name"])
}

func TestRelationshipWhere_SingleHopEnforcedBySchema(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	testDB := tidbcloud.NewTestDB(t)
	testDB.LoadSchema(t, "../fixtures/simple_schema.sql")
	testDB.LoadFixtures(t, "../fixtures/simple_seed.sql")

	schema := buildGraphQLSchema(t, testDB)

	query := `
		{
			users(
				where: {
					posts: {
						some: {
							user: { isNull: false }
						}
					}
				}
			) {
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
	require.NotEmpty(t, result.Errors)
}

func TestRelationshipWhere_ManyToOneConnectionSelectionWithoutFKField(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	testDB := tidbcloud.NewTestDB(t)
	testDB.LoadSchema(t, "../fixtures/simple_schema.sql")
	testDB.LoadFixtures(t, "../fixtures/simple_seed.sql")

	schema := buildGraphQLSchema(t, testDB)

	query := `
		{
			posts(first: 4, orderBy: [{ databaseId: ASC }]) {
				nodes {
					databaseId
					user {
						username
					}
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

	posts := requireCollectionNodes(t, result.Data.(map[string]interface{}), "posts")
	require.Len(t, posts, 4)
	assert.Equal(t, "alice", posts[0].(map[string]interface{})["user"].(map[string]interface{})["username"])
	assert.Equal(t, "alice", posts[1].(map[string]interface{})["user"].(map[string]interface{})["username"])
	assert.Equal(t, "bob", posts[2].(map[string]interface{})["user"].(map[string]interface{})["username"])
	assert.Equal(t, "charlie", posts[3].(map[string]interface{})["user"].(map[string]interface{})["username"])
}
