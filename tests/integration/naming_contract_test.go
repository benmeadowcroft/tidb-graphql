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

func TestNamingContract_ListQueriesPluralized(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	testDB := tidbcloud.NewTestDB(t)
	testDB.LoadSchema(t, "../fixtures/generated_columns_schema.sql")

	schema := buildGraphQLSchema(t, testDB)

	query := `
		{
			__type(name: "Query") {
				fields {
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
	require.Empty(t, result.Errors, "Query should not return errors: %v", result.Errors)

	data := result.Data.(map[string]interface{})
	queryType := data["__type"].(map[string]interface{})
	fields := queryType["fields"].([]interface{})

	fieldNames := make(map[string]struct{}, len(fields))
	for _, f := range fields {
		name := f.(map[string]interface{})["name"].(string)
		fieldNames[name] = struct{}{}
	}

	if _, ok := fieldNames["people"]; !ok {
		t.Fatalf("expected list field 'people' to exist on Query")
	}
	// generated_columns_schema defines table `products_computeds`; the pluralized
	// GraphQL list field intentionally resolves to `productsComputeds`.
	if _, ok := fieldNames["productsComputeds"]; !ok {
		t.Fatalf("expected list field 'productsComputeds' to exist on Query")
	}
}

func TestNamingContract_DatabaseIdField(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	testDB := tidbcloud.NewTestDB(t)
	testDB.LoadSchema(t, "../fixtures/mutation_schema.sql")

	schema := buildGraphQLSchema(t, testDB)

	query := `
		{
			__type(name: "Categories") {
				fields {
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
	require.Empty(t, result.Errors, "Query should not return errors: %v", result.Errors)

	data := result.Data.(map[string]interface{})
	typeInfo := data["__type"].(map[string]interface{})
	fields := typeInfo["fields"].([]interface{})

	fieldNames := make(map[string]struct{}, len(fields))
	for _, f := range fields {
		name := f.(map[string]interface{})["name"].(string)
		fieldNames[name] = struct{}{}
	}

	if _, ok := fieldNames["databaseId"]; !ok {
		t.Fatalf("expected field 'databaseId' to exist on Categories type")
	}
	if _, ok := fieldNames["id"]; !ok {
		t.Fatalf("expected field 'id' to exist on Categories type")
	}
}

func TestNamingContract_SingularLookupUsesID(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	testDB := tidbcloud.NewTestDB(t)
	testDB.LoadSchema(t, "../fixtures/mutation_schema.sql")

	schema := buildGraphQLSchema(t, testDB)

	query := `
		{
			__type(name: "Query") {
				fields {
					name
					args {
						name
						type {
							kind
							name
							ofType {
								kind
								name
							}
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
	queryType := data["__type"].(map[string]interface{})
	fields := queryType["fields"].([]interface{})

	var categoryArgs []interface{}
	for _, f := range fields {
		field := f.(map[string]interface{})
		if field["name"].(string) == "category" {
			categoryArgs = field["args"].([]interface{})
			break
		}
	}
	if categoryArgs == nil {
		t.Fatalf("expected singular field 'category' to exist on Query")
	}

	for _, arg := range categoryArgs {
		argInfo := arg.(map[string]interface{})
		if argInfo["name"].(string) != "id" {
			continue
		}
		typeInfo := argInfo["type"].(map[string]interface{})
		if typeInfo["kind"] != "NON_NULL" {
			t.Fatalf("expected category.id to be NON_NULL, got %v", typeInfo["kind"])
		}
		ofType := typeInfo["ofType"].(map[string]interface{})
		if ofType["kind"] != "SCALAR" || ofType["name"] != "ID" {
			t.Fatalf("expected category.id to be ID!, got %v %v", ofType["kind"], ofType["name"])
		}
		return
	}

	t.Fatalf("expected category field to have an id argument")
}
