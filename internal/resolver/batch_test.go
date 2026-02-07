package resolver

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"tidb-graphql/internal/introspection"
	"tidb-graphql/internal/planner"
)

func TestNewBatchingContext(t *testing.T) {
	t.Run("creates context with batch state", func(t *testing.T) {
		ctx := NewBatchingContext(context.Background())
		require.NotNil(t, ctx)

		state, ok := GetBatchState(ctx)
		require.True(t, ok)
		require.NotNil(t, state)
	})

	t.Run("handles nil context", func(t *testing.T) {
		//nolint:staticcheck // intentionally testing nil handling
		//lint:ignore SA1012 intentionally testing nil handling
		ctx := NewBatchingContext(nil)
		require.NotNil(t, ctx)

		state, ok := GetBatchState(ctx)
		require.True(t, ok)
		require.NotNil(t, state)
	})

	t.Run("initializes empty caches", func(t *testing.T) {
		ctx := NewBatchingContext(context.Background())
		state, _ := GetBatchState(ctx)

		assert.EqualValues(t, 0, state.GetCacheHits())
		assert.EqualValues(t, 0, state.GetCacheMisses())
	})
}

func TestGetBatchState(t *testing.T) {
	t.Run("returns false for context without batch state", func(t *testing.T) {
		state, ok := GetBatchState(context.Background())
		assert.False(t, ok)
		assert.Nil(t, state)
	})

	t.Run("returns false for nil context", func(t *testing.T) {
		//nolint:staticcheck // intentionally testing nil handling
		//lint:ignore SA1012 intentionally testing nil handling
		state, ok := GetBatchState(nil)
		assert.False(t, ok)
		assert.Nil(t, state)
	})

	t.Run("returns true for context with batch state", func(t *testing.T) {
		ctx := NewBatchingContext(context.Background())
		state, ok := GetBatchState(ctx)
		assert.True(t, ok)
		assert.NotNil(t, state)
	})
}

func TestBatchStateCacheCounters(t *testing.T) {
	ctx := NewBatchingContext(context.Background())
	state, _ := GetBatchState(ctx)

	// Initial values
	assert.EqualValues(t, 0, state.GetCacheHits())
	assert.EqualValues(t, 0, state.GetCacheMisses())

	// Increment hits
	state.IncrementCacheHit()
	state.IncrementCacheHit()
	assert.EqualValues(t, 2, state.GetCacheHits())
	assert.EqualValues(t, 0, state.GetCacheMisses())

	// Increment misses
	state.IncrementCacheMiss()
	assert.EqualValues(t, 2, state.GetCacheHits())
	assert.EqualValues(t, 1, state.GetCacheMisses())
}

func TestBatchStateParentRows(t *testing.T) {
	ctx := NewBatchingContext(context.Background())
	state, _ := GetBatchState(ctx)

	t.Run("returns nil for missing key", func(t *testing.T) {
		rows := state.getParentRows("nonexistent")
		assert.Nil(t, rows)
	})

	t.Run("stores and retrieves parent rows", func(t *testing.T) {
		parentRows := []map[string]interface{}{
			{"id": 1, "name": "alice"},
			{"id": 2, "name": "bob"},
		}
		state.setParentRows("users|list", parentRows)

		retrieved := state.getParentRows("users|list")
		require.Len(t, retrieved, 2)
		assert.Equal(t, 1, retrieved[0]["id"])
		assert.Equal(t, 2, retrieved[1]["id"])
	})
}

func TestBatchStateChildRows(t *testing.T) {
	ctx := NewBatchingContext(context.Background())
	state, _ := GetBatchState(ctx)

	t.Run("returns nil for missing key", func(t *testing.T) {
		rows := state.getChildRows("nonexistent")
		assert.Nil(t, rows)
	})

	t.Run("stores and retrieves child rows", func(t *testing.T) {
		childRows := map[string][]map[string]interface{}{
			"1": {{"id": 101, "userId": 1}},
			"2": {{"id": 102, "userId": 2}, {"id": 103, "userId": 2}},
		}
		state.setChildRows("posts|user_id", childRows)

		retrieved := state.getChildRows("posts|user_id")
		require.NotNil(t, retrieved)
		assert.Len(t, retrieved["1"], 1)
		assert.Len(t, retrieved["2"], 2)
	})
}

func TestUniqueParentValues(t *testing.T) {
	t.Run("extracts unique values", func(t *testing.T) {
		rows := []map[string]interface{}{
			{"id": 1},
			{"id": 2},
			{"id": 1}, // duplicate
			{"id": 3},
		}
		values := uniqueParentValues(rows, "id")
		assert.Len(t, values, 3)
		assert.Contains(t, values, 1)
		assert.Contains(t, values, 2)
		assert.Contains(t, values, 3)
	})

	t.Run("skips nil values", func(t *testing.T) {
		rows := []map[string]interface{}{
			{"id": 1},
			{"id": nil},
			{"id": 2},
		}
		values := uniqueParentValues(rows, "id")
		assert.Len(t, values, 2)
	})

	t.Run("handles empty rows", func(t *testing.T) {
		values := uniqueParentValues([]map[string]interface{}{}, "id")
		assert.Empty(t, values)
	})

	t.Run("handles missing key", func(t *testing.T) {
		rows := []map[string]interface{}{
			{"other": 1},
			{"other": 2},
		}
		values := uniqueParentValues(rows, "id")
		assert.Empty(t, values)
	})

	t.Run("normalizes different numeric types", func(t *testing.T) {
		rows := []map[string]interface{}{
			{"id": int(1)},
			{"id": int64(1)}, // same value, different type
			{"id": int(2)},
		}
		values := uniqueParentValues(rows, "id")
		// Both int(1) and int64(1) should be deduplicated as "1"
		assert.Len(t, values, 2)
	})
}

func TestGroupByField(t *testing.T) {
	t.Run("groups rows by field value", func(t *testing.T) {
		rows := []map[string]interface{}{
			{"id": 1, "userId": 10},
			{"id": 2, "userId": 10},
			{"id": 3, "userId": 20},
		}
		grouped := groupByField(rows, "userId")

		assert.Len(t, grouped, 2)
		assert.Len(t, grouped["10"], 2)
		assert.Len(t, grouped["20"], 1)
	})

	t.Run("handles empty rows", func(t *testing.T) {
		grouped := groupByField([]map[string]interface{}{}, "userId")
		assert.Empty(t, grouped)
	})

	t.Run("handles nil field values", func(t *testing.T) {
		rows := []map[string]interface{}{
			{"id": 1, "userId": nil},
			{"id": 2, "userId": 10},
		}
		grouped := groupByField(rows, "userId")

		// nil is converted to "<nil>" string
		assert.Len(t, grouped, 2)
		assert.Len(t, grouped["<nil>"], 1)
		assert.Len(t, grouped["10"], 1)
	})
}

func TestMergeGrouped(t *testing.T) {
	t.Run("merges two maps", func(t *testing.T) {
		target := map[string][]map[string]interface{}{
			"1": {{"id": 101}},
		}
		src := map[string][]map[string]interface{}{
			"1": {{"id": 102}}, // same key
			"2": {{"id": 201}}, // new key
		}
		mergeGrouped(target, src)

		assert.Len(t, target, 2)
		assert.Len(t, target["1"], 2) // merged
		assert.Len(t, target["2"], 1) // added
	})

	t.Run("handles empty source", func(t *testing.T) {
		target := map[string][]map[string]interface{}{
			"1": {{"id": 101}},
		}
		mergeGrouped(target, map[string][]map[string]interface{}{})
		assert.Len(t, target, 1)
	})

	t.Run("handles empty target", func(t *testing.T) {
		target := map[string][]map[string]interface{}{}
		src := map[string][]map[string]interface{}{
			"1": {{"id": 101}},
		}
		mergeGrouped(target, src)
		assert.Len(t, target, 1)
	})
}

func TestChunkValues(t *testing.T) {
	t.Run("returns nil for empty values", func(t *testing.T) {
		chunks := chunkValues([]interface{}{}, 10)
		assert.Nil(t, chunks)
	})

	t.Run("returns single chunk when values fit", func(t *testing.T) {
		values := []interface{}{1, 2, 3}
		chunks := chunkValues(values, 10)

		require.Len(t, chunks, 1)
		assert.Equal(t, values, chunks[0])
	})

	t.Run("returns single chunk when max is zero", func(t *testing.T) {
		values := []interface{}{1, 2, 3}
		chunks := chunkValues(values, 0)

		require.Len(t, chunks, 1)
		assert.Equal(t, values, chunks[0])
	})

	t.Run("returns single chunk when max is negative", func(t *testing.T) {
		values := []interface{}{1, 2, 3}
		chunks := chunkValues(values, -5)

		require.Len(t, chunks, 1)
		assert.Equal(t, values, chunks[0])
	})

	t.Run("splits into multiple chunks", func(t *testing.T) {
		values := []interface{}{1, 2, 3, 4, 5}
		chunks := chunkValues(values, 2)

		require.Len(t, chunks, 3)
		assert.Equal(t, []interface{}{1, 2}, chunks[0])
		assert.Equal(t, []interface{}{3, 4}, chunks[1])
		assert.Equal(t, []interface{}{5}, chunks[2])
	})

	t.Run("handles exact division", func(t *testing.T) {
		values := []interface{}{1, 2, 3, 4}
		chunks := chunkValues(values, 2)

		require.Len(t, chunks, 2)
		assert.Equal(t, []interface{}{1, 2}, chunks[0])
		assert.Equal(t, []interface{}{3, 4}, chunks[1])
	})

	t.Run("handles single element chunks", func(t *testing.T) {
		values := []interface{}{1, 2, 3}
		chunks := chunkValues(values, 1)

		require.Len(t, chunks, 3)
		assert.Equal(t, []interface{}{1}, chunks[0])
		assert.Equal(t, []interface{}{2}, chunks[1])
		assert.Equal(t, []interface{}{3}, chunks[2])
	})
}

func TestStableArgsKey_Deterministic(t *testing.T) {
	args1 := map[string]interface{}{
		"limit": 10,
		"where": map[string]interface{}{
			"name": map[string]interface{}{"eq": "alice"},
			"age":  map[string]interface{}{"gte": 21},
		},
	}
	args2 := map[string]interface{}{
		"where": map[string]interface{}{
			"age":  map[string]interface{}{"gte": 21},
			"name": map[string]interface{}{"eq": "alice"},
		},
		"limit": 10,
	}

	assert.Equal(t, stableArgsKey(args1), stableArgsKey(args2))
}

func TestGroupByAlias(t *testing.T) {
	rows := []map[string]interface{}{
		{"id": 1, "name": "a", "alias": 10},
		{"id": 2, "name": "b", "alias": 10},
		{"id": 3, "name": "c", "alias": 20},
	}

	grouped := groupByAlias(rows, "alias")
	assert.Len(t, grouped, 2)
	assert.Len(t, grouped["10"], 2)
	assert.Len(t, grouped["20"], 1)

	for _, row := range grouped["10"] {
		_, ok := row["alias"]
		assert.False(t, ok)
	}
}

func TestScanRowsWithExtras(t *testing.T) {
	columns := []introspection.Column{
		{Name: "id"},
		{Name: "name"},
	}
	extras := []string{"__batch_parent_id"}

	rows := &fakeRows{rows: [][]any{
		{1, "alice", 10},
		{2, "bob", 20},
	}}

	results, err := scanRowsWithExtras(rows, columns, extras)
	require.NoError(t, err)
	require.Len(t, results, 2)
	assert.EqualValues(t, 1, results[0]["id"])
	assert.Equal(t, "alice", results[0]["name"])
	assert.EqualValues(t, 10, results[0]["__batch_parent_id"])
}

func TestScanAggregateRows(t *testing.T) {
	table := introspection.Table{
		Name: "posts",
		Columns: []introspection.Column{
			{Name: "rating"},
		},
	}
	columns := []planner.AggregateColumn{
		{
			SQLClause:  "COUNT(*) AS __count",
			ResultKey:  "count",
			ColumnName: "",
			ValueType:  planner.AggregateInt,
		},
		{
			SQLClause:  "AVG(`rating`) AS __avg_rating",
			ResultKey:  "avg",
			ColumnName: "rating",
			ValueType:  planner.AggregateFloat,
		},
	}

	rows := &fakeRows{rows: [][]any{
		{int64(10), int64(5), float64(3.5)},
		{int64(11), int64(2), float64(4.0)},
	}}

	grouped, err := scanAggregateRows(rows, columns, table)
	require.NoError(t, err)
	require.Len(t, grouped, 2)

	result := grouped["10"]
	require.NotNil(t, result)
	assert.EqualValues(t, int64(5), result["count"])
	avgValues, ok := result["avg"].(map[string]interface{})
	require.True(t, ok)
	assert.EqualValues(t, 3.5, avgValues["rating"])
}
