package planner

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"tidb-graphql/internal/introspection"
)

func TestPlanTableList(t *testing.T) {
	table := introspection.Table{
		Name: "users",
		Columns: []introspection.Column{
			{Name: "id"},
			{Name: "username"},
		},
	}

	planned, err := PlanTableList(table, nil, 10, 5, nil, nil)
	require.NoError(t, err)
	assertSQLMatches(t, planned.SQL,
		"SELECT `id`, `username` FROM `users` LIMIT ? OFFSET ?",
		"SELECT `id`, `username` FROM `users` LIMIT 10 OFFSET 5",
	)
	assertLimitOffsetArgs(t, planned.SQL, planned.Args, 10, 5)
}

func TestPlanTableByPK(t *testing.T) {
	table := introspection.Table{
		Name: "users",
		Columns: []introspection.Column{
			{Name: "id"},
			{Name: "username"},
		},
	}

	planned, err := PlanTableByPK(table, nil, &introspection.Column{Name: "id"}, 42)
	require.NoError(t, err)
	assertSQLMatches(t, planned.SQL, "SELECT `id`, `username` FROM `users` WHERE `id` = ?")
	assertArgsEqual(t, planned.Args, []interface{}{42})
}

func TestPlanTableByPKColumns(t *testing.T) {
	t.Run("two column composite key", func(t *testing.T) {
		table := introspection.Table{
			Name: "order_items",
			Columns: []introspection.Column{
				{Name: "order_id", IsPrimaryKey: true},
				{Name: "product_id", IsPrimaryKey: true},
				{Name: "quantity"},
			},
		}

		pkCols := []introspection.Column{
			{Name: "order_id"},
			{Name: "product_id"},
		}
		values := map[string]interface{}{
			"order_id":   100,
			"product_id": 5,
		}

		planned, err := PlanTableByPKColumns(table, nil, pkCols, values)
		require.NoError(t, err)
		// Note: squirrel may order WHERE clauses differently, so we check for both columns
		assert.Contains(t, planned.SQL, "FROM `order_items`")
		assert.Contains(t, planned.SQL, "`order_id` = ?")
		assert.Contains(t, planned.SQL, "`product_id` = ?")
		assert.Len(t, planned.Args, 2)
	})

	t.Run("three column composite key", func(t *testing.T) {
		table := introspection.Table{
			Name: "inventory_locations",
			Columns: []introspection.Column{
				{Name: "warehouse_id", IsPrimaryKey: true},
				{Name: "aisle", IsPrimaryKey: true},
				{Name: "shelf", IsPrimaryKey: true},
				{Name: "product_id"},
			},
		}

		pkCols := []introspection.Column{
			{Name: "warehouse_id"},
			{Name: "aisle"},
			{Name: "shelf"},
		}
		values := map[string]interface{}{
			"warehouse_id": 1,
			"aisle":        "A",
			"shelf":        3,
		}

		planned, err := PlanTableByPKColumns(table, nil, pkCols, values)
		require.NoError(t, err)
		assert.Contains(t, planned.SQL, "FROM `inventory_locations`")
		assert.Contains(t, planned.SQL, "`warehouse_id` = ?")
		assert.Contains(t, planned.SQL, "`aisle` = ?")
		assert.Contains(t, planned.SQL, "`shelf` = ?")
		assert.Len(t, planned.Args, 3)
	})

	t.Run("missing pk column value returns error", func(t *testing.T) {
		table := introspection.Table{
			Name: "order_items",
			Columns: []introspection.Column{
				{Name: "order_id", IsPrimaryKey: true},
				{Name: "product_id", IsPrimaryKey: true},
			},
		}

		pkCols := []introspection.Column{
			{Name: "order_id"},
			{Name: "product_id"},
		}
		values := map[string]interface{}{
			"order_id": 100,
			// Missing product_id
		}

		_, err := PlanTableByPKColumns(table, nil, pkCols, values)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "missing value for primary key column product_id")
	})
}

func TestPlanManyToOne(t *testing.T) {
	table := introspection.Table{
		Name: "accounts",
		Columns: []introspection.Column{
			{Name: "id"},
			{Name: "email"},
		},
	}

	planned, err := PlanManyToOne(table, nil, "id", 7)
	require.NoError(t, err)
	assertSQLMatches(t, planned.SQL, "SELECT `id`, `email` FROM `accounts` WHERE `id` = ?")
	assertArgsEqual(t, planned.Args, []interface{}{7})
}

func TestPlanManyToOneBatch(t *testing.T) {
	table := introspection.Table{
		Name: "accounts",
		Columns: []introspection.Column{
			{Name: "id"},
			{Name: "email"},
		},
	}

	planned, err := PlanManyToOneBatch(table, nil, "id", []interface{}{7, 9})
	require.NoError(t, err)
	assertSQLMatches(t, planned.SQL, "SELECT `id`, `email`, `id` AS __batch_parent_id FROM `accounts` WHERE `id` IN (?,?)")
	assertArgsEqual(t, planned.Args, []interface{}{7, 9})
}

func TestPlanOneToMany(t *testing.T) {
	table := introspection.Table{
		Name: "posts",
		Columns: []introspection.Column{
			{Name: "id"},
			{Name: "user_id"},
			{Name: "title"},
		},
	}

	planned, err := PlanOneToMany(table, nil, "user_id", 3, 25, 10, nil)
	require.NoError(t, err)
	assertSQLMatches(t, planned.SQL,
		"SELECT `id`, `user_id`, `title` FROM `posts` WHERE `user_id` = ? LIMIT ? OFFSET ?",
		"SELECT `id`, `user_id`, `title` FROM `posts` WHERE `user_id` = ? LIMIT 25 OFFSET 10",
	)
	assertWhereLimitOffsetArgs(t, planned.SQL, planned.Args, []interface{}{3}, 25, 10)
}

func TestPlanOneToManyBatch(t *testing.T) {
	table := introspection.Table{
		Name: "posts",
		Columns: []introspection.Column{
			{Name: "id", IsPrimaryKey: true},
			{Name: "user_id"},
			{Name: "title"},
		},
	}

	planned, err := PlanOneToManyBatch(table, nil, "user_id", []interface{}{1, 2}, 10, 0, nil)
	require.NoError(t, err)
	assertSQLMatches(t, planned.SQL,
		"SELECT `id`, `user_id`, `title`, __batch_parent_id FROM (SELECT `id`, `user_id`, `title`, `user_id` AS __batch_parent_id, ROW_NUMBER() OVER (PARTITION BY `user_id` ORDER BY `id`) AS __rn FROM `posts` WHERE `user_id` IN (?,?)) AS __batch WHERE __rn > ? AND __rn <= ? ORDER BY __batch_parent_id, __rn",
	)
	assertArgsEqual(t, planned.Args, []interface{}{1, 2, 0, 10})
}

func TestPlanOneToManyBatch_OrdersByBatchAliasWhenFKNotSelected(t *testing.T) {
	table := introspection.Table{
		Name: "posts",
		Columns: []introspection.Column{
			{Name: "id", IsPrimaryKey: true},
			{Name: "user_id"},
			{Name: "title"},
		},
	}

	selection := []introspection.Column{
		{Name: "id"},
		{Name: "title"},
	}

	planned, err := PlanOneToManyBatch(table, selection, "user_id", []interface{}{1, 2}, 5, 0, nil)
	require.NoError(t, err)
	assertSQLMatches(t, planned.SQL,
		"SELECT `id`, `title`, __batch_parent_id FROM (SELECT `id`, `title`, `user_id` AS __batch_parent_id, ROW_NUMBER() OVER (PARTITION BY `user_id` ORDER BY `id`) AS __rn FROM `posts` WHERE `user_id` IN (?,?)) AS __batch WHERE __rn > ? AND __rn <= ? ORDER BY __batch_parent_id, __rn",
	)
	assertArgsEqual(t, planned.Args, []interface{}{1, 2, 0, 5})
}

func TestPlanManyToMany_OrderBy(t *testing.T) {
	table := introspection.Table{
		Name: "tags",
		Columns: []introspection.Column{
			{Name: "id"},
			{Name: "name"},
		},
	}

	orderBy := &OrderBy{Columns: []string{"name"}, Direction: "ASC"}
	planned, err := PlanManyToMany("user_tags", table, "user_id", "tag_id", "id", nil, 42, 5, 0, orderBy)
	require.NoError(t, err)
	assertSQLMatches(t, planned.SQL,
		"SELECT `id`, `name` FROM `tags` INNER JOIN `user_tags` ON `user_tags`.`tag_id` = `tags`.`id` WHERE `user_tags`.`user_id` = ? ORDER BY `name` ASC LIMIT ? OFFSET ?",
	)
	assertArgsEqual(t, planned.Args, []interface{}{42, 5, 0})
}

func TestPlanEdgeList_OrderBy(t *testing.T) {
	table := introspection.Table{
		Name: "user_tags",
		Columns: []introspection.Column{
			{Name: "user_id"},
			{Name: "tag_id"},
		},
	}

	orderBy := &OrderBy{Columns: []string{"tag_id"}, Direction: "DESC"}
	planned, err := PlanEdgeList(table, "user_id", nil, 7, 3, 1, orderBy)
	require.NoError(t, err)
	assertSQLMatches(t, planned.SQL,
		"SELECT `user_id`, `tag_id` FROM `user_tags` WHERE `user_id` = ? ORDER BY `tag_id` DESC LIMIT ? OFFSET ?",
		"SELECT `user_id`, `tag_id` FROM `user_tags` WHERE `user_id` = ? ORDER BY `tag_id` DESC LIMIT 3 OFFSET 1",
	)
	assertWhereLimitOffsetArgs(t, planned.SQL, planned.Args, []interface{}{7}, 3, 1)
}

func TestPlanManyToManyBatch(t *testing.T) {
	table := introspection.Table{
		Name: "tags",
		Columns: []introspection.Column{
			{Name: "id", IsPrimaryKey: true},
		},
	}

	planned, err := PlanManyToManyBatch(
		"user_tags",
		table,
		"user_id",
		"tag_id",
		"id",
		[]introspection.Column{{Name: "id"}},
		[]interface{}{1, 2},
		10,
		0,
		nil,
	)
	require.NoError(t, err)
	assertSQLMatches(t, planned.SQL,
		"SELECT `id`, __batch_parent_id FROM (SELECT `id`, `user_tags`.`user_id` AS __batch_parent_id, ROW_NUMBER() OVER (PARTITION BY `user_tags`.`user_id` ORDER BY `id`) AS __rn FROM `tags` INNER JOIN `user_tags` ON `user_tags`.`tag_id` = `tags`.`id` WHERE `user_tags`.`user_id` IN (?,?)) AS __batch WHERE __rn > ? AND __rn <= ? ORDER BY __batch_parent_id, __rn",
	)
	assertArgsEqual(t, planned.Args, []interface{}{1, 2, 0, 10})
}

func TestPlanEdgeListBatch_CompositePKOrder(t *testing.T) {
	table := introspection.Table{
		Name: "user_tags",
		Columns: []introspection.Column{
			{Name: "user_id", IsPrimaryKey: true},
			{Name: "tag_id", IsPrimaryKey: true},
		},
	}

	planned, err := PlanEdgeListBatch(table, "user_id", nil, []interface{}{1, 2}, 10, 0, nil)
	require.NoError(t, err)
	assertSQLMatches(t, planned.SQL,
		"SELECT `user_id`, `tag_id`, __batch_parent_id FROM (SELECT `user_id`, `tag_id`, `user_id` AS __batch_parent_id, ROW_NUMBER() OVER (PARTITION BY `user_id` ORDER BY `user_id`, `tag_id`) AS __rn FROM `user_tags` WHERE `user_id` IN (?,?)) AS __batch WHERE __rn > ? AND __rn <= ? ORDER BY __batch_parent_id, __rn",
	)
	assertArgsEqual(t, planned.Args, []interface{}{1, 2, 0, 10})
}

func TestPlanTableList_QuotesIdentifiers(t *testing.T) {
	table := introspection.Table{
		Name: "user`data",
		Columns: []introspection.Column{
			{Name: "first name"},
			{Name: "select"},
		},
	}

	planned, err := PlanTableList(table, nil, 1, 0, nil, nil)
	require.NoError(t, err)
	assertSQLMatches(t, planned.SQL,
		"SELECT `first name`, `select` FROM `user``data` LIMIT ? OFFSET ?",
		"SELECT `first name`, `select` FROM `user``data` LIMIT 1 OFFSET 0",
	)
	assertLimitOffsetArgs(t, planned.SQL, planned.Args, 1, 0)
}

func assertSQLMatches(t *testing.T, got string, candidates ...string) {
	t.Helper()

	gotNorm := normalizeSQL(got)
	for _, candidate := range candidates {
		if gotNorm == normalizeSQL(candidate) {
			return
		}
	}

	assert.Fail(t, "SQL did not match any expected form", "got: %q candidates: %v", gotNorm, candidates)
}

// Accept either bound args or literal LIMIT/OFFSET in SQL.
func assertLimitOffsetArgs(t *testing.T, sql string, args []interface{}, limit, offset int) {
	t.Helper()

	if len(args) == 0 {
		normalized := normalizeSQL(sql)
		assert.Contains(t, normalized, fmt.Sprintf("LIMIT %d OFFSET %d", limit, offset))
		return
	}

	expected := []interface{}{limit, offset}
	assertArgsEqual(t, args, expected)
}

// Accept either fully bound args or bound WHERE args with literal LIMIT/OFFSET.
func assertWhereLimitOffsetArgs(t *testing.T, sql string, args []interface{}, whereArgs []interface{}, limit, offset int) {
	t.Helper()

	if len(args) == len(whereArgs) {
		assertArgsEqual(t, args, whereArgs)
		assertLimitOffsetArgs(t, sql, nil, limit, offset)
		return
	}

	expected := append(append([]interface{}{}, whereArgs...), limit, offset)
	assertArgsEqual(t, args, expected)
}

// Compare args by string form to avoid int vs int64 differences.
func assertArgsEqual(t *testing.T, got []interface{}, expected []interface{}) {
	t.Helper()

	if len(got) != len(expected) {
		assert.Equal(t, len(expected), len(got))
		return
	}

	gotNorm := normalizeArgs(got)
	expectedNorm := normalizeArgs(expected)
	assert.Equal(t, expectedNorm, gotNorm)
}

// Normalize args to strings so numeric types compare consistently.
func normalizeArgs(args []interface{}) []string {
	normalized := make([]string, len(args))
	for i, arg := range args {
		normalized[i] = fmt.Sprintf("%v", arg)
	}
	return normalized
}

// Normalize SQL for stable comparisons across whitespace differences.
func normalizeSQL(sql string) string {
	return strings.Join(strings.Fields(sql), " ")
}
