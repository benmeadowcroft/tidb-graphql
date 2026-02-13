package planner

import (
	"encoding/base64"
	"testing"

	"tidb-graphql/internal/introspection"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func bytesFilterTable() introspection.Table {
	return introspection.Table{
		Name: "files",
		Columns: []introspection.Column{
			{Name: "id", DataType: "bigint", IsPrimaryKey: true},
			{Name: "payload", DataType: "blob"},
		},
		Indexes: []introspection.Index{
			{Name: "PRIMARY", Columns: []string{"id"}},
			{Name: "idx_files_payload", Columns: []string{"payload"}},
		},
	}
}

func TestBuildWhereClause_BytesEqAndIn(t *testing.T) {
	table := bytesFilterTable()
	hello := base64.StdEncoding.EncodeToString([]byte("hello"))
	world := base64.StdEncoding.EncodeToString([]byte("world"))

	where, err := BuildWhereClause(table, map[string]interface{}{
		"payload": map[string]interface{}{
			"eq": hello,
			"in": []interface{}{hello, world},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, where)

	sql, args, err := where.Condition.ToSql()
	require.NoError(t, err)
	assert.Contains(t, sql, "`payload` = ?")
	assert.Contains(t, sql, "`payload` IN (?,?)")
	assert.Len(t, args, 3)
	assert.Equal(t, []byte("hello"), args[0])
}

func TestBuildWhereClause_BytesInvalidBase64(t *testing.T) {
	table := bytesFilterTable()
	_, err := BuildWhereClause(table, map[string]interface{}{
		"payload": map[string]interface{}{
			"eq": "%%%invalid%%%",
		},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid base64")
}

func TestBuildWhereClause_BytesUnsupportedOperator(t *testing.T) {
	table := bytesFilterTable()
	_, err := BuildWhereClause(table, map[string]interface{}{
		"payload": map[string]interface{}{
			"like": "abc",
		},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not supported for bytes")
}
