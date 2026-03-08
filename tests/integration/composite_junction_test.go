//go:build integration
// +build integration

package integration

import (
	"context"
	"testing"

	"tidb-graphql/internal/dbexec"
	"tidb-graphql/internal/testutil/tidbcloud"

	"github.com/graphql-go/graphql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupCompositeJunctionSchema(t *testing.T) (*tidbcloud.TestDB, graphql.Schema) {
	t.Helper()

	testDB := tidbcloud.NewTestDB(t)
	testDB.LoadSchema(t, "../fixtures/composite_junction_schema.sql")
	testDB.LoadFixtures(t, "../fixtures/composite_junction_seed.sql")

	return testDB, buildGraphQLSchema(t, testDB)
}

func TestCompositeJunction_ManyToManyTraversal_UserToGroups(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	_, schema := setupCompositeJunctionSchema(t)
	userNodeID := nodeIDForTable("users", 1, 1)

	query := `
		{
			user(id: "` + userNodeID + `") {
				tenantId
				databaseId
				username
				groups(first: 10) {
					nodes {
						tenantId
						databaseId
						name
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
	require.Empty(t, result.Errors, "Query should not return errors: %v", result.Errors)

	data := result.Data.(map[string]interface{})
	user := data["user"].(map[string]interface{})
	groups := requireCollectionNodes(t, user, "groups")
	require.Len(t, groups, 1)

	group := groups[0].(map[string]interface{})
	assert.EqualValues(t, 1, group["tenantId"])
	assert.EqualValues(t, 10, group["databaseId"])
	assert.Equal(t, "admins_t1", group["name"])
}

func TestCompositeJunction_ManyToManyTraversal_GroupToUsers(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	_, schema := setupCompositeJunctionSchema(t)
	groupNodeID := nodeIDForTable("groups", 2, 10)

	query := `
		{
			group(id: "` + groupNodeID + `") {
				tenantId
				databaseId
				name
				users(first: 10) {
					nodes {
						tenantId
						databaseId
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
	require.Empty(t, result.Errors, "Query should not return errors: %v", result.Errors)

	data := result.Data.(map[string]interface{})
	group := data["group"].(map[string]interface{})
	users := requireCollectionNodes(t, group, "users")
	require.Len(t, users, 1)

	user := users[0].(map[string]interface{})
	assert.EqualValues(t, 2, user["tenantId"])
	assert.EqualValues(t, 1, user["databaseId"])
	assert.Equal(t, "alice_t2", user["username"])
}

func TestCompositeJunction_EdgeListTraversal_UserMemberships(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	_, schema := setupCompositeJunctionSchema(t)
	userNodeID := nodeIDForTable("users", 1, 1)

	query := `
		{
			user(id: "` + userNodeID + `") {
				projectMemberships(first: 10) {
					nodes {
						roleLevel
						project {
							tenantId
							databaseId
							name
						}
						user {
							tenantId
							databaseId
							username
						}
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
	require.Empty(t, result.Errors, "Query should not return errors: %v", result.Errors)

	data := result.Data.(map[string]interface{})
	user := data["user"].(map[string]interface{})
	memberships := requireCollectionNodes(t, user, "projectMemberships")
	require.Len(t, memberships, 1)

	edge := memberships[0].(map[string]interface{})
	assert.Equal(t, "owner", edge["roleLevel"])
	project := edge["project"].(map[string]interface{})
	assert.EqualValues(t, 1, project["tenantId"])
	assert.EqualValues(t, 100, project["databaseId"])
	assert.Equal(t, "apollo_t1", project["name"])
	edgeUser := edge["user"].(map[string]interface{})
	assert.EqualValues(t, 1, edgeUser["tenantId"])
	assert.EqualValues(t, 1, edgeUser["databaseId"])
	assert.Equal(t, "alice_t1", edgeUser["username"])
}

func TestCompositeJunction_EdgeListTraversal_ProjectMemberships(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	_, schema := setupCompositeJunctionSchema(t)
	projectNodeID := nodeIDForTable("projects", 2, 100)

	query := `
		{
			project(id: "` + projectNodeID + `") {
				projectMemberships(first: 10) {
					nodes {
						roleLevel
						user {
							tenantId
							databaseId
							username
						}
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
	require.Empty(t, result.Errors, "Query should not return errors: %v", result.Errors)

	data := result.Data.(map[string]interface{})
	project := data["project"].(map[string]interface{})
	memberships := requireCollectionNodes(t, project, "projectMemberships")
	require.Len(t, memberships, 1)

	edge := memberships[0].(map[string]interface{})
	assert.Equal(t, "viewer", edge["roleLevel"])
	edgeUser := edge["user"].(map[string]interface{})
	assert.EqualValues(t, 2, edgeUser["tenantId"])
	assert.EqualValues(t, 1, edgeUser["databaseId"])
	assert.Equal(t, "alice_t2", edgeUser["username"])
}

func TestCompositeJunction_CreateUserWithManyToManyConnect(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	testDB, schema := setupCompositeJunctionSchema(t)
	executor := dbexec.NewStandardExecutor(testDB.DB)

	groupNodeID := nodeIDForTable("groups", 1, 10)
	mutation := `
		mutation {
			createUser(input: {
				tenantId: 1
				databaseId: 99
				username: "new_user_t1"
				groupsConnect: [{ id: "` + groupNodeID + `" }]
			}) {
				__typename
				... on CreateUserSuccess {
					user {
						tenantId
						databaseId
						username
						groups(first: 10) {
							nodes {
								tenantId
								databaseId
								name
							}
						}
					}
				}
				... on MutationError {
					__typename
					message
				}
			}
		}
	`
	result := executeMutation(t, schema, executor, mutation, nil)
	wrapper := mutationResultField(t, result, "createUser")
	assert.Equal(t, "CreateUserSuccess", wrapper["__typename"])

	user := wrapper["user"].(map[string]interface{})
	assert.EqualValues(t, 1, user["tenantId"])
	assert.EqualValues(t, 99, user["databaseId"])
	assert.Equal(t, "new_user_t1", user["username"])

	groups := requireCollectionNodes(t, user, "groups")
	require.Len(t, groups, 1)
	group := groups[0].(map[string]interface{})
	assert.EqualValues(t, 1, group["tenantId"])
	assert.EqualValues(t, 10, group["databaseId"])
	assert.Equal(t, "admins_t1", group["name"])

	var exists bool
	err := testDB.DB.QueryRow(
		"SELECT EXISTS(SELECT 1 FROM user_groups WHERE tenant_id = 1 AND user_id = 99 AND group_tenant_id = 1 AND group_id = 10)",
	).Scan(&exists)
	require.NoError(t, err)
	assert.True(t, exists)
}
