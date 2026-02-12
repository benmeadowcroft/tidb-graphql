//go:build integration
// +build integration

package integration

import (
	"context"
	"sort"
	"testing"

	"tidb-graphql/internal/testutil/tidbcloud"

	"github.com/graphql-go/graphql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSetFiltering_Has(t *testing.T) {
	testDB := tidbcloud.NewTestDB(t)
	testDB.LoadSchema(t, "../fixtures/set_filtering_schema.sql")
	testDB.LoadFixtures(t, "../fixtures/set_filtering_seed.sql")

	schema := buildGraphQLSchema(t, testDB)
	query := `
		{
			products(where: { tags: { has: FEATURED } }, orderBy: { databaseId: ASC }) {
				name
				tags
			}
		}
	`
	result := graphql.Do(graphql.Params{Schema: schema, RequestString: query, Context: context.Background()})
	require.Empty(t, result.Errors)

	products := result.Data.(map[string]interface{})["products"].([]interface{})
	require.Len(t, products, 2)
	names := []string{
		products[0].(map[string]interface{})["name"].(string),
		products[1].(map[string]interface{})["name"].(string),
	}
	sort.Strings(names)
	assert.Equal(t, []string{"Black Widget", "Blue Widget"}, names)
}

func TestSetFiltering_HasAnyAllNoneOf(t *testing.T) {
	testDB := tidbcloud.NewTestDB(t)
	testDB.LoadSchema(t, "../fixtures/set_filtering_schema.sql")
	testDB.LoadFixtures(t, "../fixtures/set_filtering_seed.sql")

	schema := buildGraphQLSchema(t, testDB)

	queryAny := `
		{
			products(where: { tags: { hasAnyOf: [FEATURED, LIMITED] } }, orderBy: { databaseId: ASC }) {
				name
			}
		}
	`
	resultAny := graphql.Do(graphql.Params{Schema: schema, RequestString: queryAny, Context: context.Background()})
	require.Empty(t, resultAny.Errors)
	productsAny := resultAny.Data.(map[string]interface{})["products"].([]interface{})
	require.Len(t, productsAny, 3)

	queryAll := `
		{
			products(where: { tags: { hasAllOf: [FEATURED, SEASONAL] } }) {
				name
			}
		}
	`
	resultAll := graphql.Do(graphql.Params{Schema: schema, RequestString: queryAll, Context: context.Background()})
	require.Empty(t, resultAll.Errors)
	productsAll := resultAll.Data.(map[string]interface{})["products"].([]interface{})
	require.Len(t, productsAll, 1)
	assert.Equal(t, "Black Widget", productsAll[0].(map[string]interface{})["name"])

	queryNone := `
		{
			products(where: { tags: { hasNoneOf: [FEATURED, CLEARANCE] } }, orderBy: { databaseId: ASC }) {
				name
			}
		}
	`
	resultNone := graphql.Do(graphql.Params{Schema: schema, RequestString: queryNone, Context: context.Background()})
	require.Empty(t, resultNone.Errors)
	productsNone := resultNone.Data.(map[string]interface{})["products"].([]interface{})
	require.Len(t, productsNone, 2)
	names := []string{
		productsNone[0].(map[string]interface{})["name"].(string),
		productsNone[1].(map[string]interface{})["name"].(string),
	}
	sort.Strings(names)
	assert.Equal(t, []string{"Green Widget", "White Widget"}, names)
}

func TestSetFiltering_ExactEqAndNe(t *testing.T) {
	testDB := tidbcloud.NewTestDB(t)
	testDB.LoadSchema(t, "../fixtures/set_filtering_schema.sql")
	testDB.LoadFixtures(t, "../fixtures/set_filtering_seed.sql")

	schema := buildGraphQLSchema(t, testDB)

	queryEq := `
		{
			products(where: { tags: { eq: [NEW, FEATURED] } }) {
				name
				tags
			}
		}
	`
	resultEq := graphql.Do(graphql.Params{Schema: schema, RequestString: queryEq, Context: context.Background()})
	require.Empty(t, resultEq.Errors)
	productsEq := resultEq.Data.(map[string]interface{})["products"].([]interface{})
	require.Len(t, productsEq, 1)
	assert.Equal(t, "Blue Widget", productsEq[0].(map[string]interface{})["name"])

	queryNe := `
		{
			products(where: { tags: { ne: [FEATURED, NEW] } }, orderBy: { databaseId: ASC }) {
				name
			}
		}
	`
	resultNe := graphql.Do(graphql.Params{Schema: schema, RequestString: queryNe, Context: context.Background()})
	require.Empty(t, resultNe.Errors)
	productsNe := resultNe.Data.(map[string]interface{})["products"].([]interface{})
	require.Len(t, productsNe, 4)
}

func TestSetFiltering_EmptyListSemantics(t *testing.T) {
	testDB := tidbcloud.NewTestDB(t)
	testDB.LoadSchema(t, "../fixtures/set_filtering_schema.sql")
	testDB.LoadFixtures(t, "../fixtures/set_filtering_seed.sql")

	schema := buildGraphQLSchema(t, testDB)

	cases := []struct {
		name     string
		filter   string
		expected int
	}{
		{name: "hasAnyOf empty", filter: "{ hasAnyOf: [] }", expected: 0},
		{name: "hasAllOf empty", filter: "{ hasAllOf: [] }", expected: 5},
		{name: "hasNoneOf empty", filter: "{ hasNoneOf: [] }", expected: 5},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			query := `
				{
					products(where: { tags: ` + tc.filter + ` }) {
						name
					}
				}
			`
			result := graphql.Do(graphql.Params{Schema: schema, RequestString: query, Context: context.Background()})
			require.Empty(t, result.Errors)
			products := result.Data.(map[string]interface{})["products"].([]interface{})
			assert.Len(t, products, tc.expected)
		})
	}
}

func TestSetFiltering_MutationRoundTrip(t *testing.T) {
	testDB := tidbcloud.NewTestDB(t)
	testDB.LoadSchema(t, "../fixtures/set_filtering_schema.sql")
	testDB.LoadFixtures(t, "../fixtures/set_filtering_seed.sql")

	schema, executor := buildMutationSchema(t, testDB)

	mutation := `
		mutation {
			createProduct(input: { name: "Purple Widget", price: "79.99", tags: [SEASONAL, FEATURED, SEASONAL] }) {
				name
				tags
			}
		}
	`
	createResult := executeMutation(t, schema, executor, mutation, nil)
	require.Empty(t, createResult.Errors)

	record := createResult.Data.(map[string]interface{})["createProduct"].(map[string]interface{})
	assert.Equal(t, "Purple Widget", record["name"])
	// GraphQL enum fields serialize as enum names, not underlying lowercase SQL values.
	assert.Equal(t, []interface{}{"FEATURED", "SEASONAL"}, record["tags"])
}
