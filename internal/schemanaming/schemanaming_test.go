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
