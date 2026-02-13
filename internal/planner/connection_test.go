package planner

import (
	"strings"
	"testing"

	"tidb-graphql/internal/introspection"
	"tidb-graphql/internal/sqlutil"

	sq "github.com/Masterminds/squirrel"
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
	first, err := ParseFirst(nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if first != DefaultConnectionLimit {
		t.Errorf("expected %d, got %d", DefaultConnectionLimit, first)
	}
}

func TestParseFirst_Explicit(t *testing.T) {
	args := map[string]interface{}{"first": 50}
	first, err := ParseFirst(args)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if first != 50 {
		t.Errorf("expected 50, got %d", first)
	}
}

func TestParseFirst_Zero(t *testing.T) {
	args := map[string]interface{}{"first": 0}
	first, err := ParseFirst(args)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if first != 0 {
		t.Errorf("expected 0, got %d", first)
	}
}

func TestParseFirst_CapsAtMax(t *testing.T) {
	args := map[string]interface{}{"first": 500}
	first, err := ParseFirst(args)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if first != MaxConnectionLimit {
		t.Errorf("expected %d, got %d", MaxConnectionLimit, first)
	}
}

func TestParseFirst_Negative(t *testing.T) {
	args := map[string]interface{}{"first": -1}
	_, err := ParseFirst(args)
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
	cols := CursorColumns(table, orderBy)
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
	if plan.First != 2 {
		t.Errorf("expected first=2, got %d", plan.First)
	}
	if !strings.Contains(plan.Root.SQL, "JOIN") {
		t.Errorf("expected JOIN in SQL, got: %s", plan.Root.SQL)
	}
	if !strings.Contains(plan.Root.SQL, "`user_tags`") {
		t.Errorf("expected junction table in SQL, got: %s", plan.Root.SQL)
	}
	if !strings.Contains(plan.Root.SQL, "LIMIT") {
		t.Errorf("expected LIMIT in SQL, got: %s", plan.Root.SQL)
	}
	if len(plan.Root.Args) > 1 {
		if plan.Root.Args[len(plan.Root.Args)-1] != 3 {
			t.Errorf("expected last arg to be first+1 (3), got: %v", plan.Root.Args)
		}
	} else if !strings.Contains(plan.Root.SQL, "LIMIT 3") {
		t.Errorf("expected LIMIT 3 in SQL, got: %s", plan.Root.SQL)
	}
	if !strings.Contains(plan.Count.SQL, "COUNT") {
		t.Errorf("expected COUNT in count SQL, got: %s", plan.Count.SQL)
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
	if plan.First != 2 {
		t.Errorf("expected first=2, got %d", plan.First)
	}
	if !strings.Contains(plan.Root.SQL, "`user_tags`") {
		t.Errorf("expected junction table in SQL, got: %s", plan.Root.SQL)
	}
	if !strings.Contains(plan.Root.SQL, "`user_id`") {
		t.Errorf("expected FK filter in SQL, got: %s", plan.Root.SQL)
	}
	if !strings.Contains(plan.Root.SQL, "LIMIT") {
		t.Errorf("expected LIMIT in SQL, got: %s", plan.Root.SQL)
	}
	if len(plan.Root.Args) > 1 {
		if plan.Root.Args[len(plan.Root.Args)-1] != 3 {
			t.Errorf("expected last arg to be first+1 (3), got: %v", plan.Root.Args)
		}
	} else if !strings.Contains(plan.Root.SQL, "LIMIT 3") {
		t.Errorf("expected LIMIT 3 in SQL, got: %s", plan.Root.SQL)
	}
	if !strings.Contains(plan.Count.SQL, "COUNT") {
		t.Errorf("expected COUNT in count SQL, got: %s", plan.Count.SQL)
	}
}

func TestBuildOneToManyCountSQL_WithWhere(t *testing.T) {
	table := introspection.Table{Name: "posts"}
	where := &WhereClause{
		Condition: sq.Eq{sqlutil.QuoteIdentifier("title"): "first"},
	}

	count, err := BuildOneToManyCountSQL(table, "user_id", 7, where)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(count.SQL, "COUNT(*)") {
		t.Errorf("expected COUNT(*) in SQL, got: %s", count.SQL)
	}
	if !strings.Contains(count.SQL, "`posts`") {
		t.Errorf("expected posts table in SQL, got: %s", count.SQL)
	}
	if !strings.Contains(count.SQL, "`user_id` = ?") {
		t.Errorf("expected FK predicate in SQL, got: %s", count.SQL)
	}
	if !strings.Contains(count.SQL, "`title` = ?") {
		t.Errorf("expected where predicate in SQL, got: %s", count.SQL)
	}
	if len(count.Args) != 2 {
		t.Fatalf("expected 2 args, got %v", count.Args)
	}
	if count.Args[0] != 7 || count.Args[1] != "first" {
		t.Errorf("unexpected args order: %v", count.Args)
	}
}

func TestPlanOneToManyConnectionBatch(t *testing.T) {
	table := introspection.Table{
		Name: "posts",
		Columns: []introspection.Column{
			{Name: "id", IsPrimaryKey: true},
			{Name: "user_id"},
			{Name: "title"},
		},
		Indexes: []introspection.Index{
			{Name: "PRIMARY", Unique: true, Columns: []string{"id"}},
		},
	}
	columns := []introspection.Column{
		{Name: "id"},
		{Name: "user_id"},
	}

	query, err := PlanOneToManyConnectionBatch(table, "user_id", columns, []interface{}{1, 2}, 2, nil, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(query.SQL, "ROW_NUMBER() OVER") {
		t.Errorf("expected window function SQL, got: %s", query.SQL)
	}
	if !strings.Contains(query.SQL, "__batch_parent_id") {
		t.Errorf("expected batch parent alias in SQL, got: %s", query.SQL)
	}
	if len(query.Args) != 4 {
		t.Fatalf("expected 4 args, got %v", query.Args)
	}
	if query.Args[0] != 1 || query.Args[1] != 2 || query.Args[2] != 0 || query.Args[3] != 3 {
		t.Errorf("expected args [1 2 0 3], got %v", query.Args)
	}
}

func TestPlanOneToManyConnectionBatch_FirstZero(t *testing.T) {
	table := introspection.Table{
		Name: "posts",
		Columns: []introspection.Column{
			{Name: "id", IsPrimaryKey: true},
			{Name: "user_id"},
		},
		Indexes: []introspection.Index{
			{Name: "PRIMARY", Unique: true, Columns: []string{"id"}},
		},
	}
	columns := []introspection.Column{
		{Name: "id"},
		{Name: "user_id"},
	}

	query, err := PlanOneToManyConnectionBatch(table, "user_id", columns, []interface{}{1, 2}, 0, nil, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(query.Args) != 4 {
		t.Fatalf("expected 4 args, got %v", query.Args)
	}
	if query.Args[2] != 0 || query.Args[3] != 1 {
		t.Errorf("expected row window bounds [0 1], got %v", query.Args[2:])
	}
}
