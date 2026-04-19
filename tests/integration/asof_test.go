//go:build integration
// +build integration

package integration

import (
	"database/sql"
	"fmt"
	"testing"
	"time"

	"tidb-graphql/internal/introspection"
	"tidb-graphql/internal/testutil/tidbcloud"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	asOfHistoricalRootPort   = 18110
	asOfHistoricalNestedPort = 18111
	asOfMixedQueryPort       = 18112
	asOfNodeQueryPort        = 18113
	asOfFutureValidationPort = 18114
	asOfMutationValidation   = 18115
	asOfNestedValidationPort = 18116
	asOfMultipleArgsPort     = 18117

	asOfTemporalPostID    = 1001
	asOfTemporalUserID    = 1
	asOfTemporalPostTitle = "Temporal Snapshot Post"
)

type asOfTestFixture struct {
	port               int
	db                 *sql.DB
	markerTime         string
	temporalPostID     int
	temporalPostNodeID string
}

func TestAsOfHistoricalRootQuery(t *testing.T) {
	requireIntegrationEnv(t)

	fixture := setupAsOfHistoricalFixture(t, asOfHistoricalRootPort)
	result := executeGraphQLHTTPRaw(t, fixture.port, `
		query($postID: Int!, $asOf: DateTime!) {
			current: posts(where: { databaseId: { eq: $postID } }) {
				nodes {
					databaseId
					title
				}
			}
			past: posts(where: { databaseId: { eq: $postID } }) @asOf(time: $asOf) {
				nodes {
					databaseId
					title
				}
			}
		}
	`, map[string]interface{}{
		"postID": fixture.temporalPostID,
		"asOf":   fixture.markerTime,
	}, "")

	require.Equal(t, 200, result.StatusCode, "unexpected HTTP status: %s", result.RawBody)
	require.Empty(t, result.Errors, "GraphQL errors: %s", result.RawBody)

	currentNodes := requireCollectionNodes(t, result.Data, "current")
	require.Empty(t, currentNodes, "current query should not return deleted row")

	pastNodes := requireCollectionNodes(t, result.Data, "past")
	require.Len(t, pastNodes, 1, "historical query should return deleted row")
	post := pastNodes[0].(map[string]interface{})
	assert.EqualValues(t, fixture.temporalPostID, post["databaseId"])
	assert.Equal(t, asOfTemporalPostTitle, post["title"])
}

func TestAsOfHistoricalNestedRelationshipQuery(t *testing.T) {
	requireIntegrationEnv(t)

	fixture := setupAsOfHistoricalFixture(t, asOfHistoricalNestedPort)
	userNodeID := nodeIDForTable("users", asOfTemporalUserID)

	result := executeGraphQLHTTPRaw(t, fixture.port, `
		query($id: ID!, $postID: Int!, $asOf: DateTime!) {
			current: user(id: $id) {
				posts(where: { databaseId: { eq: $postID } }) {
					nodes {
						databaseId
						title
					}
				}
			}
			past: user(id: $id) @asOf(time: $asOf) {
				posts(where: { databaseId: { eq: $postID } }) {
					nodes {
						databaseId
						title
					}
				}
			}
		}
	`, map[string]interface{}{
		"id":     userNodeID,
		"postID": fixture.temporalPostID,
		"asOf":   fixture.markerTime,
	}, "")

	require.Equal(t, 200, result.StatusCode, "unexpected HTTP status: %s", result.RawBody)
	require.Empty(t, result.Errors, "GraphQL errors: %s", result.RawBody)

	currentUser := requireMapField(t, result.Data, "current")
	require.Empty(t, requireCollectionNodes(t, currentUser, "posts"))

	pastUser := requireMapField(t, result.Data, "past")
	pastPosts := requireCollectionNodes(t, pastUser, "posts")
	require.Len(t, pastPosts, 1, "historical nested query should return deleted row")
	post := pastPosts[0].(map[string]interface{})
	assert.EqualValues(t, fixture.temporalPostID, post["databaseId"])
	assert.Equal(t, asOfTemporalPostTitle, post["title"])
}

func TestAsOfMixedCurrentAndSnapshotInOneRequest(t *testing.T) {
	requireIntegrationEnv(t)

	fixture := setupAsOfHistoricalFixture(t, asOfMixedQueryPort)
	result := executeGraphQLHTTPRaw(t, fixture.port, `
		query($postID: Int!, $asOf: DateTime!) {
			current: posts(where: { databaseId: { eq: $postID } }) {
				nodes {
					databaseId
				}
			}
			past: posts(where: { databaseId: { eq: $postID } }) @asOf(time: $asOf) {
				nodes {
					databaseId
				}
			}
		}
	`, map[string]interface{}{
		"postID": fixture.temporalPostID,
		"asOf":   fixture.markerTime,
	}, "")

	require.Equal(t, 200, result.StatusCode, "unexpected HTTP status: %s", result.RawBody)
	require.Empty(t, result.Errors, "GraphQL errors: %s", result.RawBody)

	currentNodes := requireCollectionNodes(t, result.Data, "current")
	pastNodes := requireCollectionNodes(t, result.Data, "past")
	require.Empty(t, currentNodes, "current subtree should not see deleted row")
	require.Len(t, pastNodes, 1, "snapshot subtree should see deleted row")
	assert.EqualValues(t, fixture.temporalPostID, pastNodes[0].(map[string]interface{})["databaseId"])
}

func TestAsOfNodeQuery(t *testing.T) {
	requireIntegrationEnv(t)

	fixture := setupAsOfHistoricalFixture(t, asOfNodeQueryPort)
	postTypeName := introspection.ToGraphQLTypeName("posts")

	result := executeGraphQLHTTPRaw(t, fixture.port, fmt.Sprintf(`
		query($id: ID!, $asOf: DateTime!) {
			current: node(id: $id) {
				... on %s {
					databaseId
					title
				}
			}
			past: node(id: $id) @asOf(time: $asOf) {
				... on %s {
					databaseId
					title
				}
			}
		}
	`, postTypeName, postTypeName), map[string]interface{}{
		"id":   fixture.temporalPostNodeID,
		"asOf": fixture.markerTime,
	}, "")

	require.Equal(t, 200, result.StatusCode, "unexpected HTTP status: %s", result.RawBody)
	require.Empty(t, result.Errors, "GraphQL errors: %s", result.RawBody)

	assert.Nil(t, result.Data["current"], "current node query should return null for deleted row")

	past := requireMapField(t, result.Data, "past")
	assert.EqualValues(t, fixture.temporalPostID, past["databaseId"])
	assert.Equal(t, asOfTemporalPostTitle, past["title"])
}

func TestAsOfFutureTimestampReturns400(t *testing.T) {
	requireIntegrationEnv(t)

	port := setupAsOfValidationFixture(t, asOfFutureValidationPort)
	result := executeGraphQLHTTPRaw(t, port, `
		query($asOf: DateTime!) {
			posts @asOf(time: $asOf) {
				nodes {
					databaseId
				}
			}
		}
	`, map[string]interface{}{
		"asOf": "2999-01-01T00:00:00Z",
	}, "")

	requireGraphQLValidationError(t, result, "@asOf time must not be in the future")
}

func TestAsOfOnMutationReturns400(t *testing.T) {
	requireIntegrationEnv(t)

	port := setupAsOfValidationFixture(t, asOfMutationValidation)
	result := executeGraphQLHTTPRaw(t, port, `
		mutation {
			createPost(input: { userId: 1, title: "x" }) @asOf(offsetSeconds: -10) {
				__typename
			}
		}
	`, nil, "")

	requireGraphQLValidationError(t, result, "@asOf is only allowed on root query fields")
}

func TestAsOfOnNestedFieldReturns400(t *testing.T) {
	requireIntegrationEnv(t)

	port := setupAsOfValidationFixture(t, asOfNestedValidationPort)
	result := executeGraphQLHTTPRaw(t, port, `
		query {
			users {
				nodes {
					posts @asOf(offsetSeconds: -10) {
						nodes {
							databaseId
						}
					}
				}
			}
		}
	`, nil, "")

	requireGraphQLValidationError(t, result, "@asOf is only allowed on root query fields")
}

func TestAsOfWithMultipleArgsReturns400(t *testing.T) {
	requireIntegrationEnv(t)

	port := setupAsOfValidationFixture(t, asOfMultipleArgsPort)
	result := executeGraphQLHTTPRaw(t, port, `
		query {
			posts @asOf(time: "2026-04-01T10:00:00Z", offsetSeconds: -10) {
				nodes {
					databaseId
				}
			}
		}
	`, nil, "")

	requireGraphQLValidationError(t, result, "@asOf requires exactly one of: time, offsetSeconds")
}

func setupAsOfHistoricalFixture(t *testing.T, port int) asOfTestFixture {
	t.Helper()

	testDB := tidbcloud.NewTestDB(t)
	testDB.LoadSchema(t, "../fixtures/simple_schema.sql")
	testDB.LoadFixtures(t, "../fixtures/simple_seed.sql")
	requireSnapshotReadSupport(t, testDB.DB)

	cfg := buildBaseTestConfig(port)
	cfg.Database.Database = testDB.DatabaseName
	_, _, _ = startTestAppWithConfig(t, cfg)

	_, err := testDB.DB.Exec(`
		INSERT INTO posts (id, user_id, title, content, published)
		VALUES (?, ?, ?, ?, ?)
	`, asOfTemporalPostID, asOfTemporalUserID, asOfTemporalPostTitle, "visible only in the snapshot", true)
	require.NoError(t, err)

	var marker time.Time
	err = testDB.DB.QueryRow(`SELECT NOW(6)`).Scan(&marker)
	require.NoError(t, err)

	time.Sleep(time.Second)

	_, err = testDB.DB.Exec(`DELETE FROM posts WHERE id = ?`, asOfTemporalPostID)
	require.NoError(t, err)

	return asOfTestFixture{
		port:               port,
		db:                 testDB.DB,
		markerTime:         marker.UTC().Format(time.RFC3339Nano),
		temporalPostID:     asOfTemporalPostID,
		temporalPostNodeID: nodeIDForTable("posts", asOfTemporalPostID),
	}
}

func setupAsOfValidationFixture(t *testing.T, port int) int {
	t.Helper()

	testDB := tidbcloud.NewTestDB(t)
	testDB.LoadSchema(t, "../fixtures/simple_schema.sql")
	testDB.LoadFixtures(t, "../fixtures/simple_seed.sql")
	requireSnapshotReadSupport(t, testDB.DB)

	cfg := buildBaseTestConfig(port)
	cfg.Database.Database = testDB.DatabaseName
	_, _, _ = startTestAppWithConfig(t, cfg)
	return port
}

func requireGraphQLValidationError(t *testing.T, result graphQLHTTPResult, wantMessage string) {
	t.Helper()

	require.Equal(t, 400, result.StatusCode, "unexpected HTTP status: %s", result.RawBody)
	require.NotEmpty(t, result.Errors, "expected GraphQL errors: %s", result.RawBody)
	message, ok := result.Errors[0]["message"].(string)
	require.True(t, ok, "expected string error message, got %T", result.Errors[0]["message"])
	require.Equal(t, wantMessage, message)
}

func requireMapField(t *testing.T, data map[string]interface{}, field string) map[string]interface{} {
	t.Helper()

	value, ok := data[field]
	require.True(t, ok, "expected field %q in response data", field)
	out, ok := value.(map[string]interface{})
	require.True(t, ok, "expected map field %q, got %T", field, value)
	return out
}
