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

func TestBytesFiltering_NullPayloadQuery(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	testDB := tidbcloud.NewTestDB(t)
	testDB.LoadSchema(t, "../fixtures/bytes_filtering_schema.sql")
	testDB.LoadFixtures(t, "../fixtures/bytes_filtering_seed.sql")
	_, err := testDB.DB.Exec("INSERT INTO files (name, payload, hash) VALUES ('null-payload', NULL, x'0102')")
	require.NoError(t, err)

	schema := buildGraphQLSchema(t, testDB)
	query := `
		{
			file_by_name(name: "null-payload") {
				name
				payload
			}
		}
	`

	result := graphql.Do(graphql.Params{Schema: schema, RequestString: query, Context: context.Background()})
	require.Empty(t, result.Errors)

	file := result.Data.(map[string]interface{})["file_by_name"].(map[string]interface{})
	assert.Equal(t, "null-payload", file["name"])
	assert.Nil(t, file["payload"])
}

func TestBytesFiltering_MutationUpdateAndDelete(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	testDB := tidbcloud.NewTestDB(t)
	testDB.LoadSchema(t, "../fixtures/bytes_filtering_schema.sql")
	testDB.LoadFixtures(t, "../fixtures/bytes_filtering_seed.sql")

	schema, executor := buildMutationSchema(t, testDB)

	lookup := `
		{
			file_by_name(name: "alpha") {
				id
			}
		}
	`
	lookupResult := graphql.Do(graphql.Params{Schema: schema, RequestString: lookup, Context: context.Background()})
	require.Empty(t, lookupResult.Errors)
	fileID := lookupResult.Data.(map[string]interface{})["file_by_name"].(map[string]interface{})["id"].(string)

	newPayload := base64.StdEncoding.EncodeToString([]byte("updated-bytes"))
	update := `
		mutation {
			updateFile(id: "` + fileID + `", set: { payload: "` + newPayload + `" }) {
				name
				payload
			}
		}
	`
	updateResult := executeMutation(t, schema, executor, update, nil)
	require.Empty(t, updateResult.Errors)
	updated := updateResult.Data.(map[string]interface{})["updateFile"].(map[string]interface{})
	assert.Equal(t, "alpha", updated["name"])
	assert.Equal(t, newPayload, updated["payload"])

	del := `
		mutation {
			deleteFile(id: "` + fileID + `") {
				id
			}
		}
	`
	deleteResult := executeMutation(t, schema, executor, del, nil)
	require.Empty(t, deleteResult.Errors)
}
