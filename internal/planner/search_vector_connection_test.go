package planner

import (
	"strings"
	"testing"

	"tidb-graphql/internal/cursor"
	"tidb-graphql/internal/introspection"

	"github.com/graphql-go/graphql/language/ast"
)

func vectorConnectionTestField() *ast.Field {
	return &ast.Field{
		Name: &ast.Name{Value: "searchDocsByEmbeddingVector"},
		SelectionSet: &ast.SelectionSet{Selections: []ast.Selection{
			&ast.Field{
				Name: &ast.Name{Value: "edges"},
				SelectionSet: &ast.SelectionSet{Selections: []ast.Selection{
					&ast.Field{Name: &ast.Name{Value: "distance"}},
					&ast.Field{
						Name: &ast.Name{Value: "node"},
						SelectionSet: &ast.SelectionSet{Selections: []ast.Selection{
							&ast.Field{Name: &ast.Name{Value: "databaseId"}},
						}},
					},
				}},
			},
		}},
	}
}

func vectorConnectionTestTable() introspection.Table {
	return introspection.Table{
		Name: "docs",
		Columns: []introspection.Column{
			{Name: "id", DataType: "bigint", IsPrimaryKey: true},
			{Name: "embedding", DataType: "vector", ColumnType: "vector(3)", VectorDimension: 3},
			{Name: "title", DataType: "varchar"},
		},
	}
}

func TestPlanVectorSearchConnection_Basic(t *testing.T) {
	table := vectorConnectionTestTable()
	field := vectorConnectionTestField()
	vectorCol := table.Columns[1]

	plan, err := PlanVectorSearchConnection(
		&introspection.Schema{Tables: []introspection.Table{table}},
		table,
		vectorCol,
		field,
		map[string]interface{}{
			"vector": []interface{}{0.1, 0.2, 0.3},
			"first":  2,
		},
		100,
		20,
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !strings.Contains(plan.Root.SQL, "VEC_COSINE_DISTANCE") {
		t.Fatalf("expected cosine distance SQL, got: %s", plan.Root.SQL)
	}
	if !strings.Contains(plan.Root.SQL, "ORDER BY `__vector_distance` ASC, `id` ASC") {
		t.Fatalf("expected deterministic order by distance + pk, got: %s", plan.Root.SQL)
	}
	if !strings.Contains(plan.Root.SQL, "LIMIT 3") {
		t.Fatalf("expected first+1 limit (3), got: %s", plan.Root.SQL)
	}
	if got := plan.First; got != 2 {
		t.Fatalf("plan.First = %d, want 2", got)
	}
	if got := len(plan.Root.Args); got == 0 {
		t.Fatalf("expected query args, got none")
	}
	if got, ok := plan.Root.Args[0].(string); !ok || got != "[0.1,0.2,0.3]" {
		t.Fatalf("expected first arg to be normalized vector literal, got %#v", plan.Root.Args[0])
	}
}

func TestPlanVectorSearchConnection_AfterCursorSeek(t *testing.T) {
	table := vectorConnectionTestTable()
	vectorCol := table.Columns[1]
	pkCols := introspection.PrimaryKeyColumns(table)
	orderByKey := vectorOrderByKey(table, vectorCol, VectorDistanceMetricCosine, pkCols)
	after := cursor.EncodeCursor(introspection.GraphQLTypeName(table), orderByKey, []string{"ASC", "ASC"}, 0.5, 10)

	plan, err := PlanVectorSearchConnection(
		&introspection.Schema{Tables: []introspection.Table{table}},
		table,
		vectorCol,
		vectorConnectionTestField(),
		map[string]interface{}{
			"vector": []interface{}{0.1, 0.2, 0.3},
			"first":  2,
			"after":  after,
		},
		100,
		20,
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !strings.Contains(plan.Root.SQL, "`__vector_distance` > ?") {
		t.Fatalf("expected seek predicate for distance, got: %s", plan.Root.SQL)
	}
	if !strings.Contains(plan.Root.SQL, "`id` > ?") {
		t.Fatalf("expected seek predicate for PK tie-break, got: %s", plan.Root.SQL)
	}
}

func TestPlanVectorSearchConnection_Validation(t *testing.T) {
	table := vectorConnectionTestTable()
	vectorCol := table.Columns[1]

	_, err := PlanVectorSearchConnection(
		&introspection.Schema{Tables: []introspection.Table{table}},
		table,
		vectorCol,
		vectorConnectionTestField(),
		map[string]interface{}{
			"vector": []interface{}{0.1, 0.2, 0.3},
			"last":   1,
		},
		100,
		20,
	)
	if err == nil || !strings.Contains(err.Error(), "last is not supported") {
		t.Fatalf("expected last unsupported error, got %v", err)
	}

	_, err = PlanVectorSearchConnection(
		&introspection.Schema{Tables: []introspection.Table{table}},
		table,
		vectorCol,
		vectorConnectionTestField(),
		map[string]interface{}{
			"vector": []interface{}{0.1, 0.2, 0.3},
			"metric": "INNER_PRODUCT",
		},
		100,
		20,
	)
	if err == nil || !strings.Contains(err.Error(), "metric must be COSINE or L2") {
		t.Fatalf("expected metric validation error, got %v", err)
	}

	_, err = PlanVectorSearchConnection(
		&introspection.Schema{Tables: []introspection.Table{table}},
		table,
		vectorCol,
		vectorConnectionTestField(),
		map[string]interface{}{
			"vector": []interface{}{0.1, 0.2},
		},
		100,
		20,
	)
	if err == nil || !strings.Contains(err.Error(), "does not match") {
		t.Fatalf("expected vector dimension mismatch error, got %v", err)
	}
}

func TestPlanVectorSearchConnection_FirstCap(t *testing.T) {
	table := vectorConnectionTestTable()
	vectorCol := table.Columns[1]

	plan, err := PlanVectorSearchConnection(
		&introspection.Schema{Tables: []introspection.Table{table}},
		table,
		vectorCol,
		vectorConnectionTestField(),
		map[string]interface{}{
			"vector": []interface{}{0.1, 0.2, 0.3},
			"first":  500,
		},
		75,
		20,
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if plan.First != 75 {
		t.Fatalf("expected first to be capped at 75, got %d", plan.First)
	}
	if !strings.Contains(plan.Root.SQL, "LIMIT 76") {
		t.Fatalf("expected limit 76, got %s", plan.Root.SQL)
	}
}
