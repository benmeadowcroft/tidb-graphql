package planner

import (
	"strings"
	"testing"

	"tidb-graphql/internal/introspection"

	sq "github.com/Masterminds/squirrel"
)

func relationshipWhereSchema(indexPostsByUser bool) *introspection.Schema {
	users := introspection.Table{
		Name: "users",
		Columns: []introspection.Column{
			{Name: "id", IsPrimaryKey: true, GraphQLFieldName: "databaseId"},
			{Name: "username", GraphQLFieldName: "username"},
			{Name: "email", GraphQLFieldName: "email"},
		},
		Indexes: []introspection.Index{
			{Name: "PRIMARY", Unique: true, Columns: []string{"id"}},
			{Name: "idx_users_username", Columns: []string{"username"}},
		},
	}
	postsIndexes := []introspection.Index{
		{Name: "PRIMARY", Unique: true, Columns: []string{"id"}},
	}
	if indexPostsByUser {
		postsIndexes = append(postsIndexes, introspection.Index{Name: "idx_posts_user_id", Columns: []string{"user_id"}})
	}
	posts := introspection.Table{
		Name: "posts",
		Columns: []introspection.Column{
			{Name: "id", IsPrimaryKey: true, GraphQLFieldName: "databaseId"},
			{Name: "user_id", GraphQLFieldName: "userId"},
			{Name: "published", GraphQLFieldName: "published"},
			{Name: "title", GraphQLFieldName: "title"},
		},
		Indexes: postsIndexes,
	}
	users.Relationships = []introspection.Relationship{
		{
			IsOneToMany:      true,
			LocalColumns:     []string{"id"},
			RemoteTable:      "posts",
			RemoteColumns:    []string{"user_id"},
			GraphQLFieldName: "posts",
		},
	}
	posts.Relationships = []introspection.Relationship{
		{
			IsManyToOne:      true,
			LocalColumns:     []string{"user_id"},
			RemoteTable:      "users",
			RemoteColumns:    []string{"id"},
			GraphQLFieldName: "user",
		},
	}
	return &introspection.Schema{Tables: []introspection.Table{users, posts}}
}

func tableByName(schema *introspection.Schema, name string) introspection.Table {
	for _, table := range schema.Tables {
		if table.Name == name {
			return table
		}
	}
	return introspection.Table{}
}

func whereToSQL(t *testing.T, where *WhereClause) string {
	t.Helper()
	sql, _, err := sq.Select("1").From("users").Where(where.Condition).PlaceholderFormat(sq.Question).ToSql()
	if err != nil {
		t.Fatalf("failed to build SQL: %v", err)
	}
	return sql
}

func TestBuildWhereClauseWithSchema_OneToManySome(t *testing.T) {
	schema := relationshipWhereSchema(true)
	users := tableByName(schema, "users")

	where, err := BuildWhereClauseWithSchema(schema, users, map[string]interface{}{
		"posts": map[string]interface{}{
			"some": map[string]interface{}{
				"published": map[string]interface{}{"eq": true},
			},
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if where == nil || where.Condition == nil {
		t.Fatal("expected where condition")
	}
	if err := ValidateWhereClauseIndexes(schema, users, where); err != nil {
		t.Fatalf("unexpected index validation error: %v", err)
	}

	sql := whereToSQL(t, where)
	if !strings.Contains(sql, "EXISTS") {
		t.Fatalf("expected EXISTS subquery, got: %s", sql)
	}
	if !strings.Contains(sql, "`posts`") {
		t.Fatalf("expected posts table reference, got: %s", sql)
	}
	if !strings.Contains(sql, "`users`.`id`") {
		t.Fatalf("expected qualified outer correlation on users.id, got: %s", sql)
	}
}

func TestBuildWhereClauseWithSchema_OneToManyNone(t *testing.T) {
	schema := relationshipWhereSchema(true)
	users := tableByName(schema, "users")

	where, err := BuildWhereClauseWithSchema(schema, users, map[string]interface{}{
		"posts": map[string]interface{}{
			"none": map[string]interface{}{},
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	sql := whereToSQL(t, where)
	if !strings.Contains(sql, "NOT EXISTS") {
		t.Fatalf("expected NOT EXISTS subquery, got: %s", sql)
	}
}

func TestBuildWhereClauseWithSchema_ManyToOneIsNull(t *testing.T) {
	schema := relationshipWhereSchema(true)
	posts := tableByName(schema, "posts")

	where, err := BuildWhereClauseWithSchema(schema, posts, map[string]interface{}{
		"user": map[string]interface{}{
			"isNull": false,
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	sql, _, err := sq.Select("1").From("posts").Where(where.Condition).PlaceholderFormat(sq.Question).ToSql()
	if err != nil {
		t.Fatalf("failed to build SQL: %v", err)
	}
	if !strings.Contains(sql, "EXISTS") {
		t.Fatalf("expected EXISTS for isNull:false, got: %s", sql)
	}
	if !strings.Contains(sql, "`users`") {
		t.Fatalf("expected users table reference, got: %s", sql)
	}
}

func TestBuildWhereClauseWithSchema_RejectsNestedRelationFilters(t *testing.T) {
	schema := relationshipWhereSchema(true)
	users := tableByName(schema, "users")

	_, err := BuildWhereClauseWithSchema(schema, users, map[string]interface{}{
		"posts": map[string]interface{}{
			"some": map[string]interface{}{
				"user": map[string]interface{}{
					"isNull": false,
				},
			},
		},
	})
	if err == nil {
		t.Fatal("expected nested relation filter error")
	}
	if !strings.Contains(err.Error(), "single hop") {
		t.Fatalf("expected single-hop error, got: %v", err)
	}
}

func TestBuildWhereClauseWithSchema_RejectsIsAndIsNullTogether(t *testing.T) {
	schema := relationshipWhereSchema(true)
	posts := tableByName(schema, "posts")

	_, err := BuildWhereClauseWithSchema(schema, posts, map[string]interface{}{
		"user": map[string]interface{}{
			"is": map[string]interface{}{
				"username": map[string]interface{}{"eq": "alice"},
			},
			"isNull": false,
		},
	})
	if err == nil {
		t.Fatal("expected conflict error for is + isNull")
	}
}

func TestValidateWhereClauseIndexes_RelationPathFailure(t *testing.T) {
	schema := relationshipWhereSchema(false)
	users := tableByName(schema, "users")

	where, err := BuildWhereClauseWithSchema(schema, users, map[string]interface{}{
		"posts": map[string]interface{}{
			"some": map[string]interface{}{},
		},
	})
	if err != nil {
		t.Fatalf("unexpected where build error: %v", err)
	}
	err = ValidateWhereClauseIndexes(schema, users, where)
	if err == nil {
		t.Fatal("expected indexed validation error for posts table")
	}
	if !strings.Contains(err.Error(), "table posts") {
		t.Fatalf("expected table name in error, got: %v", err)
	}
}
