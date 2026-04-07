package schemanaming

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"tidb-graphql/internal/introspection"
	"tidb-graphql/internal/naming"
	"tidb-graphql/internal/tablekey"
)

func tk(database, table string) tablekey.TableKey {
	return tablekey.TableKey{Database: database, Table: table}
}

func TestApply_PrimaryKeyIDRenamed(t *testing.T) {
	schema := &introspection.Schema{
		Tables: []introspection.Table{
			{
				Name: "users",
				Columns: []introspection.Column{
					{Name: "id", IsPrimaryKey: true},
					{Name: "email"},
				},
			},
			{
				Name: "events",
				Columns: []introspection.Column{
					{Name: "id", IsPrimaryKey: false},
				},
			},
		},
	}

	Apply(schema, naming.Default())

	users := schema.Tables[0]
	events := schema.Tables[1]

	assert.Equal(t, "databaseId", users.Columns[0].GraphQLFieldName)
	assert.Equal(t, "email", users.Columns[1].GraphQLFieldName)
	assert.Equal(t, "id", events.Columns[0].GraphQLFieldName)
}

func TestApply_Idempotent(t *testing.T) {
	schema := &introspection.Schema{
		Tables: []introspection.Table{
			{
				Name: "users",
				Columns: []introspection.Column{
					{Name: "id", IsPrimaryKey: true},
					{Name: "email"},
				},
			},
		},
	}

	Apply(schema, naming.Default())
	first := schema.Tables[0]
	assert.True(t, schema.NamesApplied)

	Apply(schema, naming.Default())
	second := schema.Tables[0]

	assert.Equal(t, first.GraphQLQueryName, second.GraphQLQueryName)
	assert.Equal(t, first.GraphQLSingleQueryName, second.GraphQLSingleQueryName)
	assert.Equal(t, first.GraphQLTypeName, second.GraphQLTypeName)
	assert.Equal(t, first.Columns[0].GraphQLFieldName, second.Columns[0].GraphQLFieldName)
	assert.Equal(t, first.Columns[1].GraphQLFieldName, second.Columns[1].GraphQLFieldName)
}

func TestApply_PrimaryKeyIDCollision(t *testing.T) {
	schema := &introspection.Schema{
		Tables: []introspection.Table{
			{
				Name: "users",
				Columns: []introspection.Column{
					{Name: "id", IsPrimaryKey: true},
					{Name: "database_id"},
				},
			},
		},
	}

	Apply(schema, naming.Default())

	users := schema.Tables[0]
	assert.Equal(t, "databaseId_raw", users.Columns[0].GraphQLFieldName)
	assert.Equal(t, "databaseId", users.Columns[1].GraphQLFieldName)
}

func TestUniqueDatabaseIDName_MultipleFallbacks(t *testing.T) {
	columns := []introspection.Column{
		{GraphQLFieldName: "databaseId"},
		{GraphQLFieldName: "databaseId_raw"},
		{GraphQLFieldName: "databaseId_raw2"},
	}
	assert.Equal(t, "databaseId_raw3", uniqueDatabaseIDName(columns, -1))
}

func TestApply_TypeOverrides(t *testing.T) {
	schema := &introspection.Schema{
		Tables: []introspection.Table{
			{Name: "users"},
			{Name: "orders"},
		},
	}

	namer := naming.New(naming.Config{
		TypeOverrides: map[string]string{
			"users": "Account",
		},
	}, nil)

	Apply(schema, namer)

	assert.Equal(t, "Account", schema.Tables[0].GraphQLTypeName)
	assert.Equal(t, "Account", schema.Tables[0].GraphQLSingleTypeName)
	assert.Equal(t, "Order", schema.Tables[1].GraphQLSingleTypeName)
}

func TestApplyWithNamespaces_TypeNamesArePrefixed(t *testing.T) {
	schema := &introspection.Schema{
		Tables: []introspection.Table{
			{
				Name: "orders",
				Key:  tk("shop", "orders"),
				Columns: []introspection.Column{
					{Name: "id", IsPrimaryKey: true},
					{Name: "total"},
				},
			},
			{
				Name: "customers",
				Key:  tk("analytics", "customers"),
				Columns: []introspection.Column{
					{Name: "id", IsPrimaryKey: true},
				},
			},
		},
	}

	namespaceMap := map[string]string{
		"shop":      "shop",
		"analytics": "analytics",
	}

	ApplyWithNamespaces(schema, naming.Default(), namespaceMap, nil)

	orders := schema.Tables[0]
	customers := schema.Tables[1]

	// Type names get namespace prefix (singular base)
	assert.Equal(t, "Shop_Order", orders.GraphQLTypeName)
	assert.Equal(t, "Shop_Order", orders.GraphQLSingleTypeName)
	assert.Equal(t, "Analytics_Customer", customers.GraphQLTypeName)
	assert.Equal(t, "Analytics_Customer", customers.GraphQLSingleTypeName)

	// Query field names are not prefixed (mounted on namespace wrapper in Phase 4)
	assert.Equal(t, "orders", orders.GraphQLQueryName)
	assert.Equal(t, "order", orders.GraphQLSingleQueryName)
}

func TestApplyWithNamespaces_SingleDbNoNamespace_FlatBehaviour(t *testing.T) {
	// Single database with no explicit namespace → identical to Apply (flat root).
	schema := &introspection.Schema{
		Tables: []introspection.Table{
			{Name: "users", Key: tk("mydb", "users")},
		},
	}

	// Empty namespace map → no prefix
	ApplyWithNamespaces(schema, naming.Default(), nil, nil)

	assert.Equal(t, "Users", schema.Tables[0].GraphQLTypeName)
	assert.Equal(t, "User", schema.Tables[0].GraphQLSingleTypeName)
}

func TestApplyWithNamespaces_SameTableNameDifferentDBs_NoCollision(t *testing.T) {
	// Two databases each with a "users" table — types must be distinct.
	schema := &introspection.Schema{
		Tables: []introspection.Table{
			{Name: "users", Key: tk("ecommerce", "users")},
			{Name: "users", Key: tk("identity", "users")},
		},
	}

	namespaceMap := map[string]string{
		"ecommerce": "shop",
		"identity":  "auth",
	}

	ApplyWithNamespaces(schema, naming.Default(), namespaceMap, nil)

	assert.Equal(t, "Shop_User", schema.Tables[0].GraphQLTypeName)
	assert.Equal(t, "Auth_User", schema.Tables[1].GraphQLTypeName)
}

func TestApplyWithNamespaces_PerDBTypeOverride(t *testing.T) {
	schema := &introspection.Schema{
		Tables: []introspection.Table{
			{Name: "orders", Key: tk("shop", "orders")},
		},
	}

	namespaceMap := map[string]string{"shop": "shop"}
	namingPerDB := map[string]naming.Config{
		"shop": {
			TypeOverrides: map[string]string{"orders": "Purchase"},
		},
	}

	ApplyWithNamespaces(schema, naming.Default(), namespaceMap, namingPerDB)

	assert.Equal(t, "Shop_Purchase", schema.Tables[0].GraphQLTypeName)
}

func TestApplyWithNamespaces_QualifiedKeyTypeOverride(t *testing.T) {
	// Qualified key "shop.orders" in global TypeOverrides should work.
	schema := &introspection.Schema{
		Tables: []introspection.Table{
			{Name: "orders", Key: tk("shop", "orders")},
		},
	}

	namer := naming.New(naming.Config{
		TypeOverrides: map[string]string{"shop.orders": "Purchase"},
	}, nil)
	namespaceMap := map[string]string{"shop": "shop"}

	ApplyWithNamespaces(schema, namer, namespaceMap, nil)

	assert.Equal(t, "Shop_Purchase", schema.Tables[0].GraphQLTypeName)
}

func TestApplyWithNamespaces_PreservesGlobalInflectionOverrides(t *testing.T) {
	schema := &introspection.Schema{
		Tables: []introspection.Table{
			{Name: "staff", Key: tk("shop", "staff")},
		},
	}

	namer := naming.New(naming.Config{
		SingularOverrides: map[string]string{
			"staff": "staff_member",
		},
	}, nil)
	namespaceMap := map[string]string{"shop": "shop"}
	namingPerDB := map[string]naming.Config{
		"shop": naming.DefaultConfig(),
	}

	ApplyWithNamespaces(schema, namer, namespaceMap, namingPerDB)

	assert.Equal(t, "Shop_StaffMember", schema.Tables[0].GraphQLTypeName)
}

func TestApplyWithNamespaces_NilNamespaceMapFallsBackToApply(t *testing.T) {
	schema := &introspection.Schema{
		Tables: []introspection.Table{
			{Name: "orders"},
		},
	}
	ApplyWithNamespaces(schema, naming.Default(), nil, nil)
	assert.Equal(t, "Orders", schema.Tables[0].GraphQLTypeName)
	assert.Equal(t, "Order", schema.Tables[0].GraphQLSingleTypeName)
}

func TestApplyWithNamespaces_TypeOverridesDoNotReserveDefaultNames(t *testing.T) {
	schema := &introspection.Schema{
		Tables: []introspection.Table{
			{Name: "users"},
			{Name: "user"},
		},
	}

	namer := naming.New(naming.Config{
		TypeOverrides: map[string]string{
			"users": "Person",
		},
	}, nil)

	Apply(schema, namer)

	assert.Equal(t, "Person", schema.Tables[0].GraphQLTypeName)
	assert.Equal(t, "Person", schema.Tables[0].GraphQLSingleTypeName)
	assert.Equal(t, "User", schema.Tables[1].GraphQLTypeName)
	assert.Equal(t, "User", schema.Tables[1].GraphQLSingleTypeName)
}
