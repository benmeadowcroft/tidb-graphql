package planner

import (
	"testing"

	"tidb-graphql/internal/introspection"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setFilterTable() introspection.Table {
	return introspection.Table{
		Name: "products",
		Columns: []introspection.Column{
			{Name: "id", DataType: "bigint", IsPrimaryKey: true},
			{Name: "tags", DataType: "set", EnumValues: []string{"featured", "new", "clearance", "seasonal", "limited"}},
		},
		Indexes: []introspection.Index{
			{Name: "PRIMARY", Columns: []string{"id"}},
			{Name: "idx_products_tags", Columns: []string{"tags"}},
		},
	}
}

func TestBuildWhereClause_SetHas(t *testing.T) {
	table := setFilterTable()

	where, err := BuildWhereClause(table, map[string]interface{}{
		"tags": map[string]interface{}{"has": "featured"},
	})
	require.NoError(t, err)
	require.NotNil(t, where)
	require.NotNil(t, where.Condition)
	assert.Equal(t, []string{"tags"}, where.UsedColumns)

	sql, args, err := where.Condition.ToSql()
	require.NoError(t, err)
	assert.Contains(t, sql, "FIND_IN_SET(?, `tags`) > 0")
	assert.Equal(t, []interface{}{"featured"}, args)
}

func TestBuildWhereClause_SetHasAnyOfAndHasAllOf(t *testing.T) {
	table := setFilterTable()

	where, err := BuildWhereClause(table, map[string]interface{}{
		"tags": map[string]interface{}{
			"hasAnyOf": []interface{}{"featured", "seasonal"},
			"hasAllOf": []interface{}{"featured", "new"},
		},
	})
	require.NoError(t, err)

	sql, args, err := where.Condition.ToSql()
	require.NoError(t, err)
	assert.Contains(t, sql, "FIND_IN_SET(?, `tags`) > 0")
	assert.Len(t, args, 4)
	assert.Equal(t, []interface{}{"featured", "new", "featured", "seasonal"}, args)
}

func TestBuildWhereClause_SetHasNoneOf(t *testing.T) {
	table := setFilterTable()

	where, err := BuildWhereClause(table, map[string]interface{}{
		"tags": map[string]interface{}{"hasNoneOf": []interface{}{"clearance", "limited"}},
	})
	require.NoError(t, err)

	sql, args, err := where.Condition.ToSql()
	require.NoError(t, err)
	assert.Contains(t, sql, "FIND_IN_SET(?, `tags`) = 0")
	assert.Equal(t, []interface{}{"clearance", "limited"}, args)
}

func TestBuildWhereClause_SetExactCanonicalization(t *testing.T) {
	table := setFilterTable()

	where, err := BuildWhereClause(table, map[string]interface{}{
		"tags": map[string]interface{}{
			"eq": []interface{}{"new", "featured"},
			"ne": []interface{}{"limited", "clearance"},
		},
	})
	require.NoError(t, err)

	sql, args, err := where.Condition.ToSql()
	require.NoError(t, err)
	assert.Contains(t, sql, "`tags` = ?")
	assert.Contains(t, sql, "`tags` <> ?")
	assert.Equal(t, []interface{}{"featured,new", "clearance,limited"}, args)
}

func TestBuildWhereClause_SetEmptyListSemantics(t *testing.T) {
	table := setFilterTable()

	testCases := []struct {
		name     string
		filter   map[string]interface{}
		sqlMatch string
	}{
		{
			name:     "hasAnyOf empty is false",
			filter:   map[string]interface{}{"hasAnyOf": []interface{}{}},
			sqlMatch: "1=0",
		},
		{
			name:     "hasAllOf empty is true",
			filter:   map[string]interface{}{"hasAllOf": []interface{}{}},
			sqlMatch: "1=1",
		},
		{
			name:     "hasNoneOf empty is true",
			filter:   map[string]interface{}{"hasNoneOf": []interface{}{}},
			sqlMatch: "1=1",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			where, err := BuildWhereClause(table, map[string]interface{}{
				"tags": tc.filter,
			})
			require.NoError(t, err)
			sql, _, err := where.Condition.ToSql()
			require.NoError(t, err)
			assert.Contains(t, sql, tc.sqlMatch)
		})
	}
}

func TestBuildWhereClause_SetInvalidValue(t *testing.T) {
	table := setFilterTable()

	_, err := BuildWhereClause(table, map[string]interface{}{
		"tags": map[string]interface{}{
			"eq": []interface{}{"featured", "invalid"},
		},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid set value")
}
