//go:build integration
// +build integration

package integration

import (
	"context"
	"testing"

	"tidb-graphql/internal/schemarefresh"
	"tidb-graphql/internal/testutil/tidbcloud"

	"github.com/graphql-go/graphql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIntegrationHelperSchemaMatchesManagerSchemaForJunctionFields(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	testDB := tidbcloud.NewTestDB(t)
	testDB.LoadSchema(t, "../fixtures/junction_schema.sql")

	helperSchema := buildGraphQLSchema(t, testDB)

	manager, err := schemarefresh.NewManager(schemarefresh.Config{
		DB:           testDB.DB,
		DatabaseName: testDB.DatabaseName,
	})
	require.NoError(t, err)

	snapshot := manager.CurrentSnapshot()
	require.NotNil(t, snapshot)
	require.NotNil(t, snapshot.Schema)

	query := `
		{
			users(first: 2) {
				nodes {
					name
					roles(first: 5) {
						nodes {
							name
						}
					}
					projectMembers(first: 5) {
						nodes {
							roleLevel
							project {
								name
							}
						}
					}
				}
			}
		}
	`

	helperResult := graphql.Do(graphql.Params{
		Schema:        helperSchema,
		RequestString: query,
		Context:       context.Background(),
	})
	require.Empty(t, helperResult.Errors, "helper-built schema query should not error: %v", helperResult.Errors)

	runtimeResult := graphql.Do(graphql.Params{
		Schema:        *snapshot.Schema,
		RequestString: query,
		Context:       context.Background(),
	})
	require.Empty(t, runtimeResult.Errors, "manager-built schema query should not error: %v", runtimeResult.Errors)

	assert.Equal(t, runtimeResult.Data, helperResult.Data)
}
