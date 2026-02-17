package planner

import (
	"strings"
	"testing"

	"tidb-graphql/internal/cursor"
	"tidb-graphql/internal/introspection"
	"tidb-graphql/internal/sqlutil"

	sq "github.com/Masterminds/squirrel"
	"github.com/graphql-go/graphql/language/ast"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
	cond := BuildSeekCondition([]string{"id"}, []interface{}{42}, []string{"ASC"})
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
	cond := BuildSeekCondition([]string{"created_at", "id"}, []interface{}{"2024-01-01", 7}, []string{"DESC", "DESC"})
	sql, args, err := cond.ToSql()
	if err != nil {
		t.Fatalf("ToSql error: %v", err)
	}
	if !strings.Contains(sql, "<") {
		t.Errorf("expected < operator for DESC, got: %s", sql)
	}
	if len(args) != 3 {
		t.Errorf("expected 3 args for lexicographic seek, got %d", len(args))
	}
}

func TestBuildSeekConditionQualified(t *testing.T) {
	cond := BuildSeekConditionQualified("users", []string{"created_at", "id"}, []interface{}{"2024-01-01", 7}, []string{"ASC", "ASC"})
	sql, args, err := cond.ToSql()
	if err != nil {
		t.Fatalf("ToSql error: %v", err)
	}
	if !strings.Contains(sql, "`users`.`created_at`") || !strings.Contains(sql, "`users`.`id`") {
		t.Errorf("expected qualified columns in SQL, got: %s", sql)
	}
	if len(args) != 3 {
		t.Errorf("expected 3 args for lexicographic seek, got %d", len(args))
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

func TestParseConnectionWindow_DefaultForward(t *testing.T) {
	window, err := parseConnectionWindowWithDefault(nil, 15)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if window.mode != PaginationModeForward {
		t.Fatalf("expected forward mode, got %q", window.mode)
	}
	if window.limit != 15 {
		t.Fatalf("expected limit 15, got %d", window.limit)
	}
	if window.hasAfter || window.hasBefore {
		t.Fatalf("expected no cursor args, got after=%v before=%v", window.hasAfter, window.hasBefore)
	}
}

func TestParseConnectionWindow_LastWithoutBefore(t *testing.T) {
	window, err := parseConnectionWindowWithDefault(map[string]interface{}{"last": 7}, 25)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if window.mode != PaginationModeBackward {
		t.Fatalf("expected backward mode, got %q", window.mode)
	}
	if window.limit != 7 {
		t.Fatalf("expected limit 7, got %d", window.limit)
	}
	if window.hasBefore {
		t.Fatal("expected hasBefore=false")
	}
}

func TestParseConnectionWindow_InvalidCombinations(t *testing.T) {
	cases := []map[string]interface{}{
		{"first": 1, "last": 1},
		{"after": "a", "before": "b"},
		{"before": "b"},
		{"last": 1, "after": "a"},
	}
	for _, args := range cases {
		if _, err := parseConnectionWindowWithDefault(args, 25); err == nil {
			t.Fatalf("expected error for args=%v", args)
		}
	}
}

func TestParseConnectionOrderBy_DefaultPK(t *testing.T) {
	table := testTable()
	pkCols := introspection.PrimaryKeyColumns(table)

	orderBy, err := parseConnectionOrderBy(table, nil, pkCols)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(orderBy.Directions) != 1 || orderBy.Directions[0] != "ASC" {
		t.Errorf("expected [ASC], got %v", orderBy.Directions)
	}
	if len(orderBy.Columns) != 1 || orderBy.Columns[0] != "id" {
		t.Errorf("expected [id], got %v", orderBy.Columns)
	}
}

func TestParseConnectionOrderBy_ExplicitIndexed(t *testing.T) {
	table := testTable()
	pkCols := introspection.PrimaryKeyColumns(table)

	args := map[string]interface{}{
		"orderBy": []interface{}{
			map[string]interface{}{"createdAt": "DESC"},
			map[string]interface{}{"databaseId": "ASC"},
		},
	}

	orderBy, err := parseConnectionOrderBy(table, args, pkCols)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(orderBy.Directions) != 2 || orderBy.Directions[0] != "DESC" || orderBy.Directions[1] != "ASC" {
		t.Errorf("expected [DESC ASC], got %v", orderBy.Directions)
	}
	if len(orderBy.Columns) < 2 {
		t.Errorf("expected at least 2 columns, got %v", orderBy.Columns)
	}
}

func TestCursorColumns(t *testing.T) {
	table := testTable()
	orderBy := &OrderBy{
		Columns:    []string{"created_at", "id"},
		Directions: []string{"ASC", "ASC"},
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
		Columns:    []string{"id"},
		Directions: []string{"ASC"},
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

func TestPlanConnection_Basic(t *testing.T) {
	table := testTable()
	schema := &introspection.Schema{
		Tables: []introspection.Table{table},
	}
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
	plan, err := PlanConnection(schema, table, field, args)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if plan.AggregateBase.SQL == "" {
		t.Fatal("expected aggregate base SQL to be populated")
	}
	if !strings.Contains(plan.AggregateBase.SQL, "FROM `users`") {
		t.Errorf("expected users table in aggregate base SQL, got: %s", plan.AggregateBase.SQL)
	}
}

func TestPlanConnection_BackwardLast(t *testing.T) {
	table := testTable()
	schema := &introspection.Schema{
		Tables: []introspection.Table{table},
	}
	field := &ast.Field{
		Name: &ast.Name{Value: "users"},
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
	plan, err := PlanConnection(schema, table, field, map[string]interface{}{"last": 2})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if plan.Mode != PaginationModeBackward {
		t.Fatalf("expected backward mode, got %q", plan.Mode)
	}
	if plan.First != 2 {
		t.Fatalf("expected page size 2, got %d", plan.First)
	}
	if len(plan.OrderBy.Directions) != 1 || plan.OrderBy.Directions[0] != "ASC" {
		t.Fatalf("expected canonical directions [ASC], got %v", plan.OrderBy.Directions)
	}
	if !strings.Contains(plan.Root.SQL, "ORDER BY `id` DESC") {
		t.Fatalf("expected DESC traversal SQL, got: %s", plan.Root.SQL)
	}
	if !strings.Contains(plan.Root.SQL, "LIMIT 3") {
		t.Fatalf("expected LIMIT 3 in SQL, got: %s", plan.Root.SQL)
	}
}

func TestPlanConnection_BeforeCursorBackwardSeek(t *testing.T) {
	table := testTable()
	schema := &introspection.Schema{
		Tables: []introspection.Table{table},
	}
	field := &ast.Field{
		Name: &ast.Name{Value: "users"},
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
	before := cursor.EncodeCursor("User", "databaseId", []string{"ASC"}, 5)
	plan, err := PlanConnection(schema, table, field, map[string]interface{}{
		"last":   1,
		"before": before,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !plan.HasBefore || plan.HasAfter {
		t.Fatalf("expected HasBefore=true and HasAfter=false, got before=%v after=%v", plan.HasBefore, plan.HasAfter)
	}
	if !strings.Contains(plan.Root.SQL, "<") {
		t.Fatalf("expected before seek predicate in SQL, got: %s", plan.Root.SQL)
	}
	if !strings.Contains(plan.Root.SQL, "ORDER BY `id` DESC") {
		t.Fatalf("expected DESC traversal SQL, got: %s", plan.Root.SQL)
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
	plan, err := PlanManyToManyConnection(
		table,
		"user_tags",
		[]string{"user_id"},
		[]string{"tag_id"},
		[]string{"id"},
		[]interface{}{7},
		field,
		args,
	)
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
	if !strings.Contains(plan.AggregateBase.SQL, "FROM `users`") {
		t.Errorf("expected aggregate base SQL to reference target table, got: %s", plan.AggregateBase.SQL)
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
	plan, err := PlanEdgeListConnection(junction, []string{"user_id"}, []interface{}{7}, field, args)
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
	if !strings.Contains(plan.AggregateBase.SQL, "FROM `user_tags`") {
		t.Errorf("expected aggregate base SQL to reference junction table, got: %s", plan.AggregateBase.SQL)
	}
}

func TestPlanManyToManyConnection_CompositeKeys(t *testing.T) {
	table := introspection.Table{
		Name: "groups",
		Columns: []introspection.Column{
			{Name: "tenant_id", IsPrimaryKey: true, GraphQLFieldName: "tenantId"},
			{Name: "id", IsPrimaryKey: true, GraphQLFieldName: "databaseId"},
		},
		Indexes: []introspection.Index{
			{Name: "PRIMARY", Unique: true, Columns: []string{"tenant_id", "id"}},
		},
		GraphQLTypeName: "Group",
	}
	field := &ast.Field{
		Name: &ast.Name{Value: "groupsConnection"},
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
	plan, err := PlanManyToManyConnection(
		table,
		"user_groups",
		[]string{"tenant_id", "user_id"},
		[]string{"group_tenant_id", "group_id"},
		[]string{"tenant_id", "id"},
		[]interface{}{7, 100},
		field,
		map[string]interface{}{"first": 1},
	)
	require.NoError(t, err)
	assert.Contains(t, plan.Root.SQL, "`user_groups`.`group_tenant_id` = `groups`.`tenant_id`")
	assert.Contains(t, plan.Root.SQL, "`user_groups`.`group_id` = `groups`.`id`")
	assert.Contains(t, plan.Root.SQL, "(`user_groups`.`tenant_id`, `user_groups`.`user_id`) IN ((?,?))")
	assert.Equal(t, []interface{}{7, 100}, plan.Root.Args)
}

func TestPlanEdgeListConnection_CompositeKeys(t *testing.T) {
	junction := introspection.Table{
		Name: "user_groups",
		Columns: []introspection.Column{
			{Name: "tenant_id", IsPrimaryKey: true, GraphQLFieldName: "tenantId"},
			{Name: "user_id", IsPrimaryKey: true, GraphQLFieldName: "userId"},
			{Name: "group_id", IsPrimaryKey: true, GraphQLFieldName: "groupId"},
		},
		Indexes: []introspection.Index{
			{Name: "PRIMARY", Unique: true, Columns: []string{"tenant_id", "user_id", "group_id"}},
		},
		GraphQLTypeName: "UserGroup",
	}
	field := &ast.Field{
		Name: &ast.Name{Value: "userGroupsConnection"},
		SelectionSet: &ast.SelectionSet{
			Selections: []ast.Selection{
				&ast.Field{
					Name: &ast.Name{Value: "nodes"},
					SelectionSet: &ast.SelectionSet{
						Selections: []ast.Selection{
							&ast.Field{Name: &ast.Name{Value: "groupId"}},
						},
					},
				},
			},
		},
	}
	plan, err := PlanEdgeListConnection(
		junction,
		[]string{"tenant_id", "user_id"},
		[]interface{}{7, 100},
		field,
		map[string]interface{}{"first": 1},
	)
	require.NoError(t, err)
	assert.Contains(t, plan.Root.SQL, "(`tenant_id`, `user_id`) IN ((?,?))")
	assert.Equal(t, []interface{}{7, 100}, plan.Root.Args)
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
