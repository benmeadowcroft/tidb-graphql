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

var uuidMappingPatterns = map[string][]string{
	"uuid_records": {"uuid_*"},
}

func TestUUIDMapping_QueryAndFilter(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	testDB := tidbcloud.NewTestDB(t)
	testDB.LoadSchema(t, "../fixtures/uuid_mapping_schema.sql")
	testDB.LoadFixtures(t, "../fixtures/uuid_mapping_seed.sql")

	schema := buildGraphQLSchemaWithUUIDMappings(t, testDB, uuidMappingPatterns)
	query := `
		{
			uuidRecords(where: { uuidBin: { eq: "550E8400-E29B-41D4-A716-446655440000" } }) {
				label
				uuidBin
				uuidText
			}
		}
	`

	result := graphql.Do(graphql.Params{Schema: schema, RequestString: query, Context: context.Background()})
	require.Empty(t, result.Errors)

	rows := result.Data.(map[string]interface{})["uuidRecords"].([]interface{})
	require.Len(t, rows, 1)
	row := rows[0].(map[string]interface{})
	assert.Equal(t, "alpha", row["label"])
	assert.Equal(t, "550e8400-e29b-41d4-a716-446655440000", row["uuidBin"])
	assert.Equal(t, "550e8400-e29b-41d4-a716-446655440000", row["uuidText"])
}

func TestUUIDMapping_MutationRoundTrip(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	testDB := tidbcloud.NewTestDB(t)
	testDB.LoadSchema(t, "../fixtures/uuid_mapping_schema.sql")
	testDB.LoadFixtures(t, "../fixtures/uuid_mapping_seed.sql")

	schema, executor := buildMutationSchemaWithUUIDMappings(t, testDB, uuidMappingPatterns)
	mutation := `
		mutation {
			createUuidRecord(input: {
				label: "gamma",
				uuidBin: "A0Eebc99-9c0b-4ef8-bb6d-6bb9bd380a11",
				uuidText: "A0Eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"
			}) {
				label
				uuidBin
				uuidText
			}
		}
	`
	result := executeMutation(t, schema, executor, mutation, nil)
	require.Empty(t, result.Errors)

	row := result.Data.(map[string]interface{})["createUuidRecord"].(map[string]interface{})
	assert.Equal(t, "gamma", row["label"])
	assert.Equal(t, "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11", row["uuidBin"])
	assert.Equal(t, "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11", row["uuidText"])
}

func TestUUIDMapping_InvalidStoredValueReturnsError(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	testDB := tidbcloud.NewTestDB(t)
	testDB.LoadSchema(t, "../fixtures/uuid_mapping_schema.sql")
	testDB.LoadFixtures(t, "../fixtures/uuid_mapping_seed.sql")
	_, err := testDB.DB.Exec("INSERT INTO uuid_records (uuid_bin, uuid_text, label) VALUES (UNHEX(REPLACE('00112233-4455-6677-8899-aabbccddeeff', '-', '')), 'not-a-uuid', 'invalid')")
	require.NoError(t, err)

	schema := buildGraphQLSchemaWithUUIDMappings(t, testDB, uuidMappingPatterns)
	query := `
		{
			uuidRecords(orderBy: { databaseId: ASC }) {
				label
				uuidText
			}
		}
	`

	result := graphql.Do(graphql.Params{Schema: schema, RequestString: query, Context: context.Background()})
	require.NotEmpty(t, result.Errors)
	require.NotNil(t, result.Data)

	rows := result.Data.(map[string]interface{})["uuidRecords"].([]interface{})
	require.Len(t, rows, 3)
	assert.Equal(t, "alpha", rows[0].(map[string]interface{})["label"])
	assert.Equal(t, "550e8400-e29b-41d4-a716-446655440000", rows[0].(map[string]interface{})["uuidText"])
	assert.Equal(t, "beta", rows[1].(map[string]interface{})["label"])
	assert.Equal(t, "123e4567-e89b-12d3-a456-426614174000", rows[1].(map[string]interface{})["uuidText"])
	assert.Equal(t, "invalid", rows[2].(map[string]interface{})["label"])
	assert.Nil(t, rows[2].(map[string]interface{})["uuidText"])
}
