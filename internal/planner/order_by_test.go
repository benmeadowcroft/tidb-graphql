package planner

import (
	"testing"

	"tidb-graphql/internal/introspection"
)

func testOrderByTable() introspection.Table {
	return introspection.Table{
		Name: "posts",
		Columns: []introspection.Column{
			{Name: "id", IsPrimaryKey: true, GraphQLFieldName: "databaseId"},
			{Name: "user_id", GraphQLFieldName: "userId"},
			{Name: "created_at", GraphQLFieldName: "createdAt"},
			{Name: "title", GraphQLFieldName: "title"},
		},
		Indexes: []introspection.Index{
			{Name: "PRIMARY", Unique: true, Columns: []string{"id"}},
			{Name: "idx_user_created", Unique: false, Columns: []string{"user_id", "created_at"}},
		},
	}
}

func TestParseOrderByV2_MultiClause_MixedDirection(t *testing.T) {
	table := testOrderByTable()
	args := map[string]interface{}{
		"orderBy": []interface{}{
			map[string]interface{}{"userId": "ASC"},
			map[string]interface{}{"createdAt": "DESC"},
		},
	}

	orderBy, err := ParseOrderBy(table, args)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(orderBy.Columns) != 3 {
		t.Fatalf("expected 3 columns, got %d", len(orderBy.Columns))
	}
	expectedColumns := []string{"user_id", "created_at", "id"}
	expectedDirections := []string{"ASC", "DESC", "ASC"}
	for i := range expectedColumns {
		if orderBy.Columns[i] != expectedColumns[i] {
			t.Fatalf("column %d mismatch: got %s want %s", i, orderBy.Columns[i], expectedColumns[i])
		}
		if orderBy.Directions[i] != expectedDirections[i] {
			t.Fatalf("direction %d mismatch: got %s want %s", i, orderBy.Directions[i], expectedDirections[i])
		}
	}
}

func TestParseOrderByV2_RejectsEmptyList(t *testing.T) {
	table := testOrderByTable()
	_, err := ParseOrderBy(table, map[string]interface{}{
		"orderBy": []interface{}{},
	})
	if err == nil {
		t.Fatal("expected error for empty orderBy list")
	}
}

func TestParseOrderByV2_RejectsDuplicateField(t *testing.T) {
	table := testOrderByTable()
	_, err := ParseOrderBy(table, map[string]interface{}{
		"orderBy": []interface{}{
			map[string]interface{}{"userId": "ASC"},
			map[string]interface{}{"userId": "DESC"},
		},
	})
	if err == nil {
		t.Fatal("expected duplicate field error")
	}
}

func TestParseOrderByV2_RejectsUnknownField(t *testing.T) {
	table := testOrderByTable()
	_, err := ParseOrderBy(table, map[string]interface{}{
		"orderBy": []interface{}{
			map[string]interface{}{"title": "ASC"},
		},
	})
	if err == nil {
		t.Fatal("expected unknown/non-indexed field error")
	}
}

func TestParseOrderByV2_RejectsNonLeftPrefix(t *testing.T) {
	table := testOrderByTable()
	_, err := ParseOrderBy(table, map[string]interface{}{
		"orderBy": []interface{}{
			map[string]interface{}{"createdAt": "ASC"},
		},
	})
	if err == nil {
		t.Fatal("expected non-left-prefix error")
	}
}

func TestParseOrderByV2_AllowsNonLeftPrefixWithPolicy(t *testing.T) {
	table := testOrderByTable()
	orderBy, err := ParseOrderBy(table, map[string]interface{}{
		"orderByPolicy": "ALLOW_NON_PREFIX",
		"orderBy": []interface{}{
			map[string]interface{}{"createdAt": "ASC"},
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	expectedColumns := []string{"created_at", "id"}
	expectedDirections := []string{"ASC", "ASC"}
	for i := range expectedColumns {
		if orderBy.Columns[i] != expectedColumns[i] {
			t.Fatalf("column %d mismatch: got %s want %s", i, orderBy.Columns[i], expectedColumns[i])
		}
		if orderBy.Directions[i] != expectedDirections[i] {
			t.Fatalf("direction %d mismatch: got %s want %s", i, orderBy.Directions[i], expectedDirections[i])
		}
	}
}

func TestParseOrderByV2_RejectsInvalidPolicy(t *testing.T) {
	table := testOrderByTable()
	_, err := ParseOrderBy(table, map[string]interface{}{
		"orderByPolicy": "UNSAFE",
		"orderBy": []interface{}{
			map[string]interface{}{"userId": "ASC"},
		},
	})
	if err == nil {
		t.Fatal("expected invalid orderByPolicy error")
	}
}

func TestParseOrderByV2_RejectsClauseWithMultipleFields(t *testing.T) {
	table := testOrderByTable()
	_, err := ParseOrderBy(table, map[string]interface{}{
		"orderBy": []interface{}{
			map[string]interface{}{"userId": "ASC", "createdAt": "DESC"},
		},
	})
	if err == nil {
		t.Fatal("expected multiple fields in a clause to be rejected")
	}
}

func TestOrderByIndexedFields_IncludesNonLeftmostIndexedColumns(t *testing.T) {
	table := testOrderByTable()
	fields := OrderByIndexedFields(table)
	if fields["createdAt"] != "created_at" {
		t.Fatalf("expected createdAt to be exposed as indexed field, got %q", fields["createdAt"])
	}
}
