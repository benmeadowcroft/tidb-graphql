package schemanaming

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"tidb-graphql/internal/introspection"
	"tidb-graphql/internal/naming"
)

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

func TestApply_TypeOverridesDoNotReserveDefaultNames(t *testing.T) {
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
