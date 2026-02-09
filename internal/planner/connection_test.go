package planner

import (
	"strings"
	"testing"

	"tidb-graphql/internal/introspection"

	"github.com/graphql-go/graphql/language/ast"
)

func testTable() introspection.Table {
	return introspection.Table{
		Name: "users",
		Columns: []introspection.Column{
			{Name: "id", DataType: "int", IsPrimaryKey: true, GraphQLFieldName: "databaseId"},
			{Name: "name", DataType: "varchar", GraphQLFieldName: "name"},
			{Name: "email", DataType: "varchar", GraphQLFieldName: "email"},
			{Name: "created_at", DataType: "datetime", GraphQLFieldName: "createdAt"},
		},
		Indexes: []introspection.Index{
			{Name: "PRIMARY", Unique: true, Columns: []string{"id"}},
			{Name: "idx_created_at", Unique: false, Columns: []string{"created_at", "id"}},
		},
		GraphQLTypeName:        "User",
		GraphQLQueryName:       "users",
		GraphQLSingleQueryName: "user",
	}
}

func TestBuildSeekCondition_ASC(t *testing.T) {
	cond := BuildSeekCondition([]string{"id"}, []interface{}{42}, "ASC")
	sql, args, err := cond.ToSql()
	if err != nil {
		t.Fatalf("ToSql error: %v", err)
	}
	if !strings.Contains(sql, ">") {
		t.Errorf("expected > operator for ASC, got: %s", sql)
	}
	if len(args) != 1 || args[0] != 42 {
		t.Errorf("unexpected args: %v", args)
	}
}

func TestBuildSeekCondition_DESC(t *testing.T) {
	cond := BuildSeekCondition([]string{"created_at", "id"}, []interface{}{"2024-01-01", 7}, "DESC")
	sql, args, err := cond.ToSql()
	if err != nil {
		t.Fatalf("ToSql error: %v", err)
	}
	if !strings.Contains(sql, "<") {
		t.Errorf("expected < operator for DESC, got: %s", sql)
	}
	if len(args) != 2 {
		t.Errorf("expected 2 args, got %d", len(args))
	}
}

func TestBuildSeekConditionQualified(t *testing.T) {
	cond := BuildSeekConditionQualified("users", []string{"created_at", "id"}, []interface{}{"2024-01-01", 7}, "ASC")
	sql, args, err := cond.ToSql()
	if err != nil {
		t.Fatalf("ToSql error: %v", err)
	}
	if !strings.Contains(sql, "`users`.`created_at`") || !strings.Contains(sql, "`users`.`id`") {
		t.Errorf("expected qualified columns in SQL, got: %s", sql)
	}
	if len(args) != 2 {
		t.Errorf("expected 2 args, got %d", len(args))
	}
}

func TestParseFirst_Defaults(t *testing.T) {
	first, err := parseFirst(nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if first != DefaultConnectionLimit {
		t.Errorf("expected %d, got %d", DefaultConnectionLimit, first)
	}
}

func TestParseFirst_Explicit(t *testing.T) {
	args := map[string]interface{}{"first": 50}
	first, err := parseFirst(args)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if first != 50 {
		t.Errorf("expected 50, got %d", first)
	}
}

func TestParseFirst_CapsAtMax(t *testing.T) {
	args := map[string]interface{}{"first": 500}
	first, err := parseFirst(args)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if first != MaxConnectionLimit {
		t.Errorf("expected %d, got %d", MaxConnectionLimit, first)
	}
}

func TestParseFirst_Negative(t *testing.T) {
	args := map[string]interface{}{"first": -1}
	_, err := parseFirst(args)
	if err == nil {
		t.Fatal("expected error for negative first")
	}
}

func TestParseConnectionOrderBy_DefaultPK(t *testing.T) {
	table := testTable()
	pkCols := introspection.PrimaryKeyColumns(table)

	orderBy, err := parseConnectionOrderBy(table, nil, pkCols)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if orderBy.Direction != "ASC" {
		t.Errorf("expected ASC, got %s", orderBy.Direction)
	}
	if len(orderBy.Columns) != 1 || orderBy.Columns[0] != "id" {
		t.Errorf("expected [id], got %v", orderBy.Columns)
	}
}

func TestParseConnectionOrderBy_ExplicitIndexed(t *testing.T) {
	table := testTable()
	pkCols := introspection.PrimaryKeyColumns(table)

	args := map[string]interface{}{
		"orderBy": map[string]interface{}{
			"createdAt_databaseId": "DESC",
		},
	}

	orderBy, err := parseConnectionOrderBy(table, args, pkCols)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if orderBy.Direction != "DESC" {
		t.Errorf("expected DESC, got %s", orderBy.Direction)
	}
	if len(orderBy.Columns) < 2 {
		t.Errorf("expected at least 2 columns, got %v", orderBy.Columns)
	}
}

func TestCursorColumns(t *testing.T) {
	table := testTable()
	orderBy := &OrderBy{
		Columns:   []string{"created_at", "id"},
		Direction: "ASC",
	}
	cols := cursorColumns(table, orderBy)
	if len(cols) != 2 {
		t.Fatalf("expected 2 cursor columns, got %d", len(cols))
	}
	if cols[0].Name != "created_at" {
		t.Errorf("expected created_at, got %s", cols[0].Name)
	}
	if cols[1].Name != "id" {
		t.Errorf("expected id, got %s", cols[1].Name)
	}
}

func TestBuildConnectionSQL(t *testing.T) {
	table := testTable()
	columns := table.Columns
	orderBy := &OrderBy{
		Columns:   []string{"id"},
		Direction: "ASC",
	}

	result, err := buildConnectionSQL(table, columns, nil, nil, orderBy, 26)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(result.SQL, "LIMIT 26") {
		t.Errorf("expected LIMIT 26 in SQL, got: %s", result.SQL)
	}
	if !strings.Contains(result.SQL, "ORDER BY") {
		t.Errorf("expected ORDER BY in SQL, got: %s", result.SQL)
	}
}

func TestBuildCountSQL(t *testing.T) {
	table := testTable()

	result, err := buildCountSQL(table, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(result.SQL, "COUNT(*)") {
		t.Errorf("expected COUNT(*) in SQL, got: %s", result.SQL)
	}
	if !strings.Contains(result.SQL, "`users`") {
		t.Errorf("expected table name in SQL, got: %s", result.SQL)
	}
}

func TestOrderByKey(t *testing.T) {
	table := testTable()

	key := OrderByKey(table, []string{"id"})
	if key != "databaseId" {
		t.Errorf("expected databaseId, got %s", key)
	}

	key = OrderByKey(table, []string{"created_at", "id"})
	if key != "createdAt_databaseId" {
		t.Errorf("expected createdAt_databaseId, got %s", key)
	}
}

func TestPlanManyToManyConnection_Basic(t *testing.T) {
	table := testTable()
	field := &ast.Field{
		Name: &ast.Name{Value: "usersConnection"},
		SelectionSet: &ast.SelectionSet{
			Selections: []ast.Selection{
				&ast.Field{
					Name: &ast.Name{Value: "nodes"},
					SelectionSet: &ast.SelectionSet{
						Selections: []ast.Selection{
							&ast.Field{Name: &ast.Name{Value: "databaseId"}},
						},
					},
				},
			},
		},
	}
	args := map[string]interface{}{"first": 2}
	plan, err := PlanManyToManyConnection(table, "user_tags", "user_id", "tag_id", "id", 7, field, args)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(plan.Root.SQL, "JOIN") {
		t.Errorf("expected JOIN in SQL, got: %s", plan.Root.SQL)
	}
	if !strings.Contains(plan.Root.SQL, "`user_tags`") {
		t.Errorf("expected junction table in SQL, got: %s", plan.Root.SQL)
	}
}

func TestPlanEdgeListConnection_Basic(t *testing.T) {
	junction := introspection.Table{
		Name: "user_tags",
		Columns: []introspection.Column{
			{Name: "user_id", IsPrimaryKey: true, GraphQLFieldName: "userId"},
			{Name: "tag_id", IsPrimaryKey: true, GraphQLFieldName: "tagId"},
		},
		Indexes: []introspection.Index{
			{Name: "PRIMARY", Unique: true, Columns: []string{"user_id", "tag_id"}},
		},
		GraphQLTypeName: "UserTag",
	}
	field := &ast.Field{
		Name: &ast.Name{Value: "tagsConnection"},
		SelectionSet: &ast.SelectionSet{
			Selections: []ast.Selection{
				&ast.Field{
					Name: &ast.Name{Value: "nodes"},
					SelectionSet: &ast.SelectionSet{
						Selections: []ast.Selection{
							&ast.Field{Name: &ast.Name{Value: "userId"}},
						},
					},
				},
			},
		},
	}
	args := map[string]interface{}{"first": 2}
	plan, err := PlanEdgeListConnection(junction, "user_id", 7, field, args)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(plan.Root.SQL, "`user_tags`") {
		t.Errorf("expected junction table in SQL, got: %s", plan.Root.SQL)
	}
	if !strings.Contains(plan.Root.SQL, "`user_id`") {
		t.Errorf("expected FK filter in SQL, got: %s", plan.Root.SQL)
	}
}
