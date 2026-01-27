package planner

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidb-graphql/internal/introspection"
)

func TestPlanInsert_Simple(t *testing.T) {
	table := introspection.Table{
		Name: "users",
		Columns: []introspection.Column{
			{Name: "id", IsPrimaryKey: true},
			{Name: "username"},
			{Name: "email"},
		},
	}

	columns := []string{"username", "email"}
	values := []interface{}{"alice", "alice@example.com"}

	planned, err := PlanInsert(table, columns, values)
	require.NoError(t, err)
	assert.Contains(t, planned.SQL, "INSERT INTO `users`")
	assert.Contains(t, planned.SQL, "`username`")
	assert.Contains(t, planned.SQL, "`email`")
	assert.Contains(t, planned.SQL, "VALUES")
	assert.Len(t, planned.Args, 2)
	assertArgsEqual(t, planned.Args, values)
}

func TestPlanInsert_WithExplicitPK(t *testing.T) {
	table := introspection.Table{
		Name: "users",
		Columns: []introspection.Column{
			{Name: "id", IsPrimaryKey: true},
			{Name: "username"},
		},
	}

	columns := []string{"id", "username"}
	values := []interface{}{100, "alice"}

	planned, err := PlanInsert(table, columns, values)
	require.NoError(t, err)
	assert.Contains(t, planned.SQL, "INSERT INTO `users`")
	assert.Contains(t, planned.SQL, "`id`")
	assert.Contains(t, planned.SQL, "`username`")
	assertArgsEqual(t, planned.Args, values)
}

func TestPlanInsert_EmptyColumns(t *testing.T) {
	table := introspection.Table{
		Name: "audit_log",
		Columns: []introspection.Column{
			{Name: "id", IsPrimaryKey: true, IsAutoIncrement: true},
		},
	}

	// Empty columns should generate INSERT INTO table () VALUES ()
	planned, err := PlanInsert(table, nil, nil)
	require.NoError(t, err)
	assert.Contains(t, planned.SQL, "INSERT INTO `audit_log` () VALUES ()")
	assert.Empty(t, planned.Args)
}

func TestPlanInsert_QuotesIdentifiers(t *testing.T) {
	table := introspection.Table{
		Name: "user data",
		Columns: []introspection.Column{
			{Name: "first name"},
			{Name: "select"},
		},
	}

	columns := []string{"first name", "select"}
	values := []interface{}{"John", "value"}

	planned, err := PlanInsert(table, columns, values)
	require.NoError(t, err)
	assert.Contains(t, planned.SQL, "`user data`")
	assert.Contains(t, planned.SQL, "`first name`")
	assert.Contains(t, planned.SQL, "`select`")
}

func TestPlanUpdate_Simple(t *testing.T) {
	table := introspection.Table{
		Name: "users",
		Columns: []introspection.Column{
			{Name: "id", IsPrimaryKey: true},
			{Name: "username"},
			{Name: "email"},
		},
	}

	set := map[string]interface{}{
		"email": "newemail@example.com",
	}
	pkValues := map[string]interface{}{
		"id": 1,
	}

	planned, err := PlanUpdate(table, set, pkValues)
	require.NoError(t, err)
	assert.Contains(t, planned.SQL, "UPDATE `users`")
	assert.Contains(t, planned.SQL, "SET")
	assert.Contains(t, planned.SQL, "`email` = ?")
	assert.Contains(t, planned.SQL, "WHERE")
	assert.Contains(t, planned.SQL, "`id` = ?")
}

func TestPlanUpdate_MultipleColumns(t *testing.T) {
	table := introspection.Table{
		Name: "users",
		Columns: []introspection.Column{
			{Name: "id", IsPrimaryKey: true},
			{Name: "username"},
			{Name: "email"},
			{Name: "active"},
		},
	}

	set := map[string]interface{}{
		"email":  "newemail@example.com",
		"active": false,
	}
	pkValues := map[string]interface{}{
		"id": 1,
	}

	planned, err := PlanUpdate(table, set, pkValues)
	require.NoError(t, err)
	assert.Contains(t, planned.SQL, "UPDATE `users`")
	assert.Contains(t, planned.SQL, "`email` = ?")
	assert.Contains(t, planned.SQL, "`active` = ?")
	assert.Contains(t, planned.SQL, "`id` = ?")
}

func TestPlanUpdate_CompositePK(t *testing.T) {
	table := introspection.Table{
		Name: "order_items",
		Columns: []introspection.Column{
			{Name: "order_id", IsPrimaryKey: true},
			{Name: "line_number", IsPrimaryKey: true},
			{Name: "quantity"},
			{Name: "unit_price"},
		},
	}

	set := map[string]interface{}{
		"quantity": 10,
	}
	pkValues := map[string]interface{}{
		"order_id":    100,
		"line_number": 1,
	}

	planned, err := PlanUpdate(table, set, pkValues)
	require.NoError(t, err)
	assert.Contains(t, planned.SQL, "UPDATE `order_items`")
	assert.Contains(t, planned.SQL, "`quantity` = ?")
	assert.Contains(t, planned.SQL, "`order_id` = ?")
	assert.Contains(t, planned.SQL, "`line_number` = ?")
}

func TestPlanUpdate_EmptySetReturnsError(t *testing.T) {
	table := introspection.Table{
		Name: "users",
		Columns: []introspection.Column{
			{Name: "id", IsPrimaryKey: true},
		},
	}

	set := map[string]interface{}{}
	pkValues := map[string]interface{}{"id": 1}

	_, err := PlanUpdate(table, set, pkValues)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "update set cannot be empty")
}

func TestPlanUpdate_MissingPKColumnReturnsError(t *testing.T) {
	table := introspection.Table{
		Name: "order_items",
		Columns: []introspection.Column{
			{Name: "order_id", IsPrimaryKey: true},
			{Name: "line_number", IsPrimaryKey: true},
			{Name: "quantity"},
		},
	}

	set := map[string]interface{}{"quantity": 10}
	pkValues := map[string]interface{}{
		"order_id": 100,
		// Missing line_number
	}

	_, err := PlanUpdate(table, set, pkValues)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "pkValues count")
}

func TestPlanUpdate_ExtraPKValueReturnsError(t *testing.T) {
	table := introspection.Table{
		Name: "users",
		Columns: []introspection.Column{
			{Name: "id", IsPrimaryKey: true},
			{Name: "username"},
		},
	}

	set := map[string]interface{}{"username": "bob"}
	pkValues := map[string]interface{}{
		"id":    1,
		"extra": 2, // Extra PK value not in table
	}

	_, err := PlanUpdate(table, set, pkValues)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "pkValues count")
}

func TestPlanDelete_Simple(t *testing.T) {
	table := introspection.Table{
		Name: "users",
		Columns: []introspection.Column{
			{Name: "id", IsPrimaryKey: true},
			{Name: "username"},
		},
	}

	pkValues := map[string]interface{}{
		"id": 1,
	}

	planned, err := PlanDelete(table, pkValues)
	require.NoError(t, err)
	assert.Contains(t, planned.SQL, "DELETE FROM `users`")
	assert.Contains(t, planned.SQL, "WHERE")
	assert.Contains(t, planned.SQL, "`id` = ?")
	assertArgsEqual(t, planned.Args, []interface{}{1})
}

func TestPlanDelete_CompositePK(t *testing.T) {
	table := introspection.Table{
		Name: "order_items",
		Columns: []introspection.Column{
			{Name: "order_id", IsPrimaryKey: true},
			{Name: "line_number", IsPrimaryKey: true},
			{Name: "quantity"},
		},
	}

	pkValues := map[string]interface{}{
		"order_id":    100,
		"line_number": 1,
	}

	planned, err := PlanDelete(table, pkValues)
	require.NoError(t, err)
	assert.Contains(t, planned.SQL, "DELETE FROM `order_items`")
	assert.Contains(t, planned.SQL, "`order_id` = ?")
	assert.Contains(t, planned.SQL, "`line_number` = ?")
	assert.Len(t, planned.Args, 2)
}

func TestPlanDelete_MissingPKColumnReturnsError(t *testing.T) {
	table := introspection.Table{
		Name: "order_items",
		Columns: []introspection.Column{
			{Name: "order_id", IsPrimaryKey: true},
			{Name: "line_number", IsPrimaryKey: true},
			{Name: "quantity"},
		},
	}

	pkValues := map[string]interface{}{
		"order_id": 100,
		// Missing line_number
	}

	_, err := PlanDelete(table, pkValues)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "pkValues count")
}

func TestPlanDelete_ExtraPKValueReturnsError(t *testing.T) {
	table := introspection.Table{
		Name: "users",
		Columns: []introspection.Column{
			{Name: "id", IsPrimaryKey: true},
			{Name: "username"},
		},
	}

	pkValues := map[string]interface{}{
		"id":    1,
		"extra": 2,
	}

	_, err := PlanDelete(table, pkValues)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "pkValues count")
}

func TestPlanDelete_QuotesIdentifiers(t *testing.T) {
	table := introspection.Table{
		Name: "user data",
		Columns: []introspection.Column{
			{Name: "user id", IsPrimaryKey: true},
		},
	}

	pkValues := map[string]interface{}{
		"user id": 1,
	}

	planned, err := PlanDelete(table, pkValues)
	require.NoError(t, err)
	assert.Contains(t, planned.SQL, "DELETE FROM `user data`")
	assert.Contains(t, planned.SQL, "`user id` = ?")
}

func TestPlanInsert_NullValues(t *testing.T) {
	table := introspection.Table{
		Name: "users",
		Columns: []introspection.Column{
			{Name: "id", IsPrimaryKey: true},
			{Name: "username"},
			{Name: "bio"},
		},
	}

	columns := []string{"username", "bio"}
	values := []interface{}{"alice", nil}

	planned, err := PlanInsert(table, columns, values)
	require.NoError(t, err)
	assert.Contains(t, planned.SQL, "`bio`")
	// nil should be passed as a value
	assert.Contains(t, planned.Args, nil)
}

func TestPlanUpdate_NullValue(t *testing.T) {
	table := introspection.Table{
		Name: "users",
		Columns: []introspection.Column{
			{Name: "id", IsPrimaryKey: true},
			{Name: "bio"},
		},
	}

	set := map[string]interface{}{
		"bio": nil,
	}
	pkValues := map[string]interface{}{
		"id": 1,
	}

	planned, err := PlanUpdate(table, set, pkValues)
	require.NoError(t, err)
	assert.Contains(t, planned.SQL, "`bio` = ?")
}
