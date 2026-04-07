package resolver

import (
	"testing"

	"tidb-graphql/internal/introspection"
	"tidb-graphql/internal/naming"
	"tidb-graphql/internal/schemafilter"
	"tidb-graphql/internal/schemanaming"
	"tidb-graphql/internal/tablekey"

	"github.com/graphql-go/graphql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func makeNamespacedSchema() *introspection.Schema {
	schema := &introspection.Schema{
		Tables: []introspection.Table{
			{
				Name: "orders",
				Key:  tablekey.TableKey{Database: "ecommerce", Table: "orders"},
				Columns: []introspection.Column{
					{Name: "id", DataType: "int", IsPrimaryKey: true, IsAutoIncrement: true},
					{Name: "total", DataType: "decimal"},
				},
			},
			{
				Name: "users",
				Key:  tablekey.TableKey{Database: "identity", Table: "users"},
				Columns: []introspection.Column{
					{Name: "id", DataType: "int", IsPrimaryKey: true, IsAutoIncrement: true},
					{Name: "email", DataType: "varchar"},
				},
			},
		},
	}
	namespaceMap := map[string]string{
		"ecommerce": "shop",
		"identity":  "auth",
	}
	schemanaming.ApplyWithNamespaces(schema, naming.Default(), namespaceMap, nil)
	return schema
}

func TestBuildNamespacedGraphQLSchema_RootFields(t *testing.T) {
	schema := makeNamespacedSchema()
	namespaceMap := map[string]string{
		"ecommerce": "shop",
		"identity":  "auth",
	}

	r := NewResolver(nil, schema, nil, 0, schemafilter.Config{}, naming.DefaultConfig())
	r.SetNamespaceMap(namespaceMap)

	gqlSchema, err := r.BuildGraphQLSchema()
	require.NoError(t, err)

	queryType := gqlSchema.QueryType()
	require.NotNil(t, queryType)

	fields := queryType.Fields()
	// Each namespace gets a field on the root Query.
	assert.Contains(t, fields, "shop", "root Query should have a 'shop' field")
	assert.Contains(t, fields, "auth", "root Query should have an 'auth' field")
}

func TestBuildNamespacedGraphQLSchema_NamespaceQueryTypes(t *testing.T) {
	schema := makeNamespacedSchema()
	namespaceMap := map[string]string{
		"ecommerce": "shop",
		"identity":  "auth",
	}

	r := NewResolver(nil, schema, nil, 0, schemafilter.Config{}, naming.DefaultConfig())
	r.SetNamespaceMap(namespaceMap)

	gqlSchema, err := r.BuildGraphQLSchema()
	require.NoError(t, err)

	queryType := gqlSchema.QueryType()
	shopField := queryType.Fields()["shop"]
	require.NotNil(t, shopField)

	// The shop field's type should be the Shop_Query object.
	shopQueryObj, ok := unwrapGraphQLNonNull(shopField.Type).(*graphql.Object)
	require.True(t, ok, "shop field type should be an object, got %T", unwrapGraphQLNonNull(shopField.Type))
	assert.Equal(t, "Shop_Query", shopQueryObj.Name())

	// Shop_Query should have the orders query fields.
	shopFields := shopQueryObj.Fields()
	assert.Contains(t, shopFields, "orders", "Shop_Query should have 'orders' list field")
	assert.Contains(t, shopFields, "order", "Shop_Query should have 'order' single field")
}

func TestBuildNamespacedGraphQLSchema_MutationFields(t *testing.T) {
	schema := makeNamespacedSchema()
	namespaceMap := map[string]string{
		"ecommerce": "shop",
		"identity":  "auth",
	}

	r := NewResolver(nil, schema, nil, 0, schemafilter.Config{}, naming.DefaultConfig())
	r.SetNamespaceMap(namespaceMap)

	gqlSchema, err := r.BuildGraphQLSchema()
	require.NoError(t, err)

	mutationType := gqlSchema.MutationType()
	require.NotNil(t, mutationType)

	mutFields := mutationType.Fields()
	assert.Contains(t, mutFields, "shop", "root Mutation should have a 'shop' field")
	assert.Contains(t, mutFields, "auth", "root Mutation should have an 'auth' field")

	shopMut := mutFields["shop"]
	shopMutObj, ok := unwrapGraphQLNonNull(shopMut.Type).(*graphql.Object)
	require.True(t, ok)
	assert.Equal(t, "Shop_Mutation", shopMutObj.Name())

	// In multi-db mode the single type name is namespace-prefixed (Shop_Order),
	// so mutation field names become createShop_Order, updateShop_Order, etc.
	shopMutFields := shopMutObj.Fields()
	assert.Contains(t, shopMutFields, "createShop_Order")
	assert.Contains(t, shopMutFields, "updateShop_Order")
	assert.Contains(t, shopMutFields, "deleteShop_Order")
}

func TestBuildNamespacedGraphQLSchema_NodeFieldOnRoot(t *testing.T) {
	schema := makeNamespacedSchema()
	namespaceMap := map[string]string{
		"ecommerce": "shop",
		"identity":  "auth",
	}

	r := NewResolver(nil, schema, nil, 0, schemafilter.Config{}, naming.DefaultConfig())
	r.SetNamespaceMap(namespaceMap)

	gqlSchema, err := r.BuildGraphQLSchema()
	require.NoError(t, err)

	queryType := gqlSchema.QueryType()
	// node(id:) should be on the root, not inside a namespace.
	assert.Contains(t, queryType.Fields(), "node", "root Query should have the global node(id:) field")
}

func TestBuildGraphQLSchema_FlatWhenNoNamespaceMap(t *testing.T) {
	// Without SetNamespaceMap, even with Key.Database set, the schema should be flat.
	schema := &introspection.Schema{
		Tables: []introspection.Table{
			{
				Name: "users",
				Key:  tablekey.TableKey{Database: "myapp", Table: "users"},
				Columns: []introspection.Column{
					{Name: "id", DataType: "int", IsPrimaryKey: true, IsAutoIncrement: true},
				},
			},
		},
	}
	schemanaming.Apply(schema, naming.Default())

	r := NewResolver(nil, schema, nil, 0, schemafilter.Config{}, naming.DefaultConfig())
	// No SetNamespaceMap call.

	gqlSchema, err := r.BuildGraphQLSchema()
	require.NoError(t, err)

	queryType := gqlSchema.QueryType()
	fields := queryType.Fields()
	// Flat mode: table fields directly on root.
	assert.Contains(t, fields, "users", "flat schema should have 'users' on root Query")
	assert.NotContains(t, fields, "myapp", "flat schema should not have namespace wrapper")
}

func TestFindRelationshipRemoteTable_PrefersQualifiedKey(t *testing.T) {
	schema := &introspection.Schema{
		Tables: []introspection.Table{
			{Name: "users", Key: tablekey.TableKey{Database: "shop", Table: "users"}},
			{Name: "users", Key: tablekey.TableKey{Database: "auth", Table: "users"}},
		},
	}

	r := NewResolver(nil, schema, nil, 0, schemafilter.Config{}, naming.DefaultConfig())

	relatedTable, err := r.findRelationshipRemoteTable(introspection.Relationship{
		RemoteTable:    "users",
		RemoteTableKey: tablekey.TableKey{Database: "shop", Table: "users"},
	})
	require.NoError(t, err)
	assert.Equal(t, "shop", relatedTable.Key.Database)
}

func TestJunctionConfigForTable_UsesQualifiedKey(t *testing.T) {
	schema := &introspection.Schema{
		Junctions: introspection.JunctionMap{
			"shop.user_roles": {Table: "user_roles", Type: introspection.JunctionTypePure},
			"auth.user_roles": {Table: "user_roles", Type: introspection.JunctionTypeAttribute},
		},
	}

	r := NewResolver(nil, schema, nil, 0, schemafilter.Config{}, naming.DefaultConfig())

	shopConfig, ok := r.junctionConfigForTable(introspection.Table{
		Name: "user_roles",
		Key:  tablekey.TableKey{Database: "shop", Table: "user_roles"},
	})
	require.True(t, ok)
	assert.Equal(t, introspection.JunctionTypePure, shopConfig.Type)

	authConfig, ok := r.junctionConfigForTable(introspection.Table{
		Name: "user_roles",
		Key:  tablekey.TableKey{Database: "auth", Table: "user_roles"},
	})
	require.True(t, ok)
	assert.Equal(t, introspection.JunctionTypeAttribute, authConfig.Type)
}

func TestBuildNamespacedGraphQLSchema_RejectsNormalizedNamespaceCollision(t *testing.T) {
	schema := &introspection.Schema{
		Tables: []introspection.Table{
			{Name: "orders", Key: tablekey.TableKey{Database: "db_one", Table: "orders"}},
			{Name: "users", Key: tablekey.TableKey{Database: "db_two", Table: "users"}},
		},
	}

	r := NewResolver(nil, schema, nil, 0, schemafilter.Config{}, naming.DefaultConfig())
	r.SetNamespaceMap(map[string]string{
		"db_one": "my_app",
		"db_two": "MyApp",
	})

	_, err := r.BuildGraphQLSchema()
	require.Error(t, err)
	assert.Contains(t, err.Error(), `normalize to the GraphQL namespace`)
}

// unwrapGraphQLNonNull peels one layer of *graphql.NonNull if present.
func unwrapGraphQLNonNull(t graphql.Type) graphql.Type {
	if nn, ok := t.(*graphql.NonNull); ok {
		return nn.OfType
	}
	return t
}
