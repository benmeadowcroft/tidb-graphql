package planner

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidb-graphql/internal/introspection"
)

func TestPlanAggregate(t *testing.T) {
	table := introspection.Table{
		Name: "employees",
		Columns: []introspection.Column{
			{Name: "id", DataType: "int"},
			{Name: "name", DataType: "varchar"},
			{Name: "salary", DataType: "decimal"},
			{Name: "age", DataType: "int"},
		},
	}

	t.Run("count only", func(t *testing.T) {
		selection := AggregateSelection{Count: true}
		planned, err := PlanAggregate(table, selection, nil)
		require.NoError(t, err)
		assert.Contains(t, planned.SQL, "COUNT(*)")
		assert.Contains(t, planned.SQL, "FROM (SELECT * FROM `employees`)")
	})

	t.Run("avg columns", func(t *testing.T) {
		selection := AggregateSelection{
			Count:      true,
			AvgColumns: []string{"salary", "age"},
		}
		planned, err := PlanAggregate(table, selection, nil)
		require.NoError(t, err)
		assert.Contains(t, planned.SQL, "AVG(`salary`)")
		assert.Contains(t, planned.SQL, "AVG(`age`)")
	})

	t.Run("sum columns", func(t *testing.T) {
		selection := AggregateSelection{
			Count:      true,
			SumColumns: []string{"salary"},
		}
		planned, err := PlanAggregate(table, selection, nil)
		require.NoError(t, err)
		assert.Contains(t, planned.SQL, "SUM(`salary`)")
	})

	t.Run("min max columns", func(t *testing.T) {
		selection := AggregateSelection{
			Count:      true,
			MinColumns: []string{"salary"},
			MaxColumns: []string{"age"},
		}
		planned, err := PlanAggregate(table, selection, nil)
		require.NoError(t, err)
		assert.Contains(t, planned.SQL, "MIN(`salary`)")
		assert.Contains(t, planned.SQL, "MAX(`age`)")
	})

	t.Run("with where clause", func(t *testing.T) {
		selection := AggregateSelection{Count: true}
		whereClause, err := BuildWhereClause(table, map[string]interface{}{
			"salary": map[string]interface{}{"gte": 50000},
		})
		require.NoError(t, err)

		planned, err := PlanAggregate(table, selection, &AggregateFilters{Where: whereClause})
		require.NoError(t, err)
		assert.Contains(t, planned.SQL, "WHERE")
		assert.Contains(t, planned.SQL, "`salary`")
	})

	t.Run("empty selection defaults to count", func(t *testing.T) {
		selection := AggregateSelection{}
		planned, err := PlanAggregate(table, selection, nil)
		require.NoError(t, err)
		assert.Contains(t, planned.SQL, "COUNT(*)")
	})

	t.Run("count included even when not selected", func(t *testing.T) {
		selection := AggregateSelection{
			SumColumns: []string{"salary"},
		}
		planned, err := PlanAggregate(table, selection, nil)
		require.NoError(t, err)
		assert.Contains(t, planned.SQL, "COUNT(*)")
		assert.Contains(t, planned.SQL, "SUM(`salary`)")
	})

	t.Run("count distinct columns", func(t *testing.T) {
		selection := AggregateSelection{
			CountDistinctColumns: []string{"name"},
		}
		planned, err := PlanAggregate(table, selection, nil)
		require.NoError(t, err)
		assert.Contains(t, planned.SQL, "COUNT(DISTINCT `name`)")
	})

	t.Run("with orderBy and limit", func(t *testing.T) {
		selection := AggregateSelection{Count: true}
		limit := 10
		offset := 5
		filters := &AggregateFilters{
			OrderBy: &OrderBy{Columns: []string{"salary"}, Directions: []string{"DESC"}},
			Limit:   &limit,
			Offset:  &offset,
		}
		planned, err := PlanAggregate(table, selection, filters)
		require.NoError(t, err)
		assert.Contains(t, planned.SQL, "ORDER BY `salary` DESC")
		assert.Contains(t, planned.SQL, "LIMIT 10")
		assert.Contains(t, planned.SQL, "OFFSET 5")
	})
}

func TestPlanRelationshipAggregate(t *testing.T) {
	table := introspection.Table{
		Name: "orders",
		Columns: []introspection.Column{
			{Name: "id", DataType: "int"},
			{Name: "customer_id", DataType: "int"},
			{Name: "total", DataType: "decimal"},
		},
	}

	t.Run("basic relationship aggregate", func(t *testing.T) {
		selection := AggregateSelection{Count: true}
		planned, err := PlanRelationshipAggregate(table, selection, "customer_id", 123, nil)
		require.NoError(t, err)
		assert.Contains(t, planned.SQL, "COUNT(*)")
		assert.Contains(t, planned.SQL, "WHERE")
		assert.Contains(t, planned.SQL, "`customer_id`")
		assert.Contains(t, planned.Args, 123)
	})

	t.Run("with additional where clause", func(t *testing.T) {
		selection := AggregateSelection{
			Count:      true,
			SumColumns: []string{"total"},
		}
		whereClause, err := BuildWhereClause(table, map[string]interface{}{
			"total": map[string]interface{}{"gte": 100},
		})
		require.NoError(t, err)

		planned, err := PlanRelationshipAggregate(table, selection, "customer_id", 123, &AggregateFilters{Where: whereClause})
		require.NoError(t, err)
		assert.Contains(t, planned.SQL, "SUM(`total`)")
		// Should have both the FK condition and the where condition
		assert.True(t, strings.Count(planned.SQL, "?") >= 2)
	})
}

func TestPlanRelationshipAggregateBatch(t *testing.T) {
	table := introspection.Table{
		Name: "orders",
		Columns: []introspection.Column{
			{Name: "id", DataType: "int"},
			{Name: "customer_id", DataType: "int"},
			{Name: "total", DataType: "decimal"},
		},
	}

	t.Run("batch aggregate with GROUP BY", func(t *testing.T) {
		selection := AggregateSelection{Count: true}
		planned, err := PlanRelationshipAggregateBatch(table, selection, "customer_id", []interface{}{1, 2, 3}, nil)
		require.NoError(t, err)
		assert.Contains(t, planned.SQL, "GROUP BY")
		assert.Contains(t, planned.SQL, "__group_key")
		assert.Contains(t, planned.SQL, "COUNT(*)")
		assert.Len(t, planned.Args, 3)
	})

	t.Run("empty values returns empty query", func(t *testing.T) {
		selection := AggregateSelection{Count: true}
		planned, err := PlanRelationshipAggregateBatch(table, selection, "customer_id", []interface{}{}, nil)
		require.NoError(t, err)
		assert.Empty(t, planned.SQL)
	})
}

func TestBuildAggregateSelectClauses(t *testing.T) {
	t.Run("all aggregate types", func(t *testing.T) {
		selection := AggregateSelection{
			Count:                true,
			CountDistinctColumns: []string{"col0"},
			AvgColumns:           []string{"col1"},
			SumColumns:           []string{"col2"},
			MinColumns:           []string{"col3"},
			MaxColumns:           []string{"col4"},
		}
		clauses := SQLClauses(BuildAggregateColumns(selection))
		assert.Len(t, clauses, 6)
		assert.Contains(t, clauses[0], "COUNT(*)")
		assert.Contains(t, clauses[1], "COUNT(DISTINCT")
		assert.Contains(t, clauses[2], "AVG")
		assert.Contains(t, clauses[3], "SUM")
		assert.Contains(t, clauses[4], "MIN")
		assert.Contains(t, clauses[5], "MAX")
	})

	t.Run("empty selection still includes count", func(t *testing.T) {
		// COUNT(*) is always included to match PlanAggregate SQL generation
		selection := AggregateSelection{}
		clauses := SQLClauses(BuildAggregateColumns(selection))
		assert.Len(t, clauses, 1)
		assert.Contains(t, clauses[0], "COUNT(*)")
	})
}
