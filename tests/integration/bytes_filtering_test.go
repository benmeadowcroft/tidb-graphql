//go:build integration
// +build integration

package integration

import (
	"context"
	"encoding/base64"
	"testing"

	"tidb-graphql/internal/testutil/tidbcloud"

	"github.com/graphql-go/graphql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBytesFiltering_QueryAndFilter(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	testDB := tidbcloud.NewTestDB(t)
	testDB.LoadSchema(t, "../fixtures/bytes_filtering_schema.sql")
	testDB.LoadFixtures(t, "../fixtures/bytes_filtering_seed.sql")

	schema := buildGraphQLSchema(t, testDB)
	query := `
		{
			files(where: { payload: { eq: "SGVsbG8=" } }) {
				name
				payload
			}
		}
	`

	result := graphql.Do(graphql.Params{Schema: schema, RequestString: query, Context: context.Background()})
	require.Empty(t, result.Errors)

	files := result.Data.(map[string]interface{})["files"].([]interface{})
	require.Len(t, files, 1)
	record := files[0].(map[string]interface{})
	assert.Equal(t, "alpha", record["name"])
	assert.Equal(t, "SGVsbG8=", record["payload"])
}

func TestBytesFiltering_MutationRoundTrip(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	testDB := tidbcloud.NewTestDB(t)
	testDB.LoadSchema(t, "../fixtures/bytes_filtering_schema.sql")
	testDB.LoadFixtures(t, "../fixtures/bytes_filtering_seed.sql")

	schema, executor := buildMutationSchema(t, testDB)
	payload := base64.StdEncoding.EncodeToString([]byte("binary-value"))
	hash := base64.StdEncoding.EncodeToString([]byte{0xAA, 0xBB, 0xCC, 0xDD})

	mutation := `
		mutation {
			createFile(input: { name: "gamma", payload: "` + payload + `", hash: "` + hash + `" }) {
				name
				payload
				hash
			}
		}
	`
	createResult := executeMutation(t, schema, executor, mutation, nil)
	require.Empty(t, createResult.Errors)

	record := createResult.Data.(map[string]interface{})["createFile"].(map[string]interface{})
	assert.Equal(t, "gamma", record["name"])
	assert.Equal(t, payload, record["payload"])
	assert.Equal(t, hash, record["hash"])
}

func TestBytesFiltering_InvalidBase64Rejected(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	testDB := tidbcloud.NewTestDB(t)
	testDB.LoadSchema(t, "../fixtures/bytes_filtering_schema.sql")
	testDB.LoadFixtures(t, "../fixtures/bytes_filtering_seed.sql")

	schema := buildGraphQLSchema(t, testDB)
	query := `
		{
			files(where: { payload: { eq: "%%%invalid%%%" } }) {
				name
			}
		}
	`

	result := graphql.Do(graphql.Params{Schema: schema, RequestString: query, Context: context.Background()})
	require.NotEmpty(t, result.Errors)
}
