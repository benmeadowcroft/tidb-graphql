package schemafilter

import (
	"testing"

	"tidb-graphql/internal/introspection"
	"tidb-graphql/internal/planner"
)

func TestApply_AllowsAllByDefault(t *testing.T) {
	schema := &introspection.Schema{
		Tables: []introspection.Table{
			{Name: "users", Columns: []introspection.Column{{Name: "id"}}},
			{Name: "orders", Columns: []introspection.Column{{Name: "id"}}},
		},
	}

	Apply(schema, Config{})

	if len(schema.Tables) != 2 {
		t.Fatalf("expected all tables to remain, got %d", len(schema.Tables))
	}
}

func TestApply_TableAndColumnFilters(t *testing.T) {
	schema := &introspection.Schema{
		Tables: []introspection.Table{
			{
				Name: "users",
				Columns: []introspection.Column{
					{Name: "id", IsPrimaryKey: true},
					{Name: "email"},
					{Name: "password_hash"},
				},
				Indexes: []introspection.Index{
					{Name: "idx_email", Columns: []string{"email"}, Unique: false},
					{Name: "idx_password", Columns: []string{"password_hash"}, Unique: false},
				},
			},
			{
				Name: "audit_intern",
				Columns: []introspection.Column{
					{Name: "id", IsPrimaryKey: true},
					{Name: "payload"},
				},
			},
		},
	}

	cfg := Config{
		AllowTables: []string{"*"},
		DenyTables:  []string{"*_intern"},
		AllowColumns: map[string][]string{
			"*": {"*"},
		},
		DenyColumns: map[string][]string{
			"users": {"password_*"},
		},
	}

	Apply(schema, cfg)

	if len(schema.Tables) != 1 || schema.Tables[0].Name != "users" {
		t.Fatalf("expected only users table to remain, got %+v", schema.Tables)
	}

	if len(schema.Tables[0].Columns) != 2 {
		t.Fatalf("expected password_hash to be filtered, got %+v", schema.Tables[0].Columns)
	}

	if len(schema.Tables[0].Indexes) != 1 || schema.Tables[0].Indexes[0].Name != "idx_email" {
		t.Fatalf("expected only idx_email to remain, got %+v", schema.Tables[0].Indexes)
	}
}

func TestApply_RemovesForeignKeysAndRelationshipsForFilteredColumns(t *testing.T) {
	schema := &introspection.Schema{
		Tables: []introspection.Table{
			{
				Name: "users",
				Columns: []introspection.Column{
					{Name: "id", IsPrimaryKey: true},
				},
			},
			{
				Name: "posts",
				Columns: []introspection.Column{
					{Name: "id", IsPrimaryKey: true},
					{Name: "user_id"},
				},
				ForeignKeys: []introspection.ForeignKey{
					{
						ColumnName:       "user_id",
						ReferencedTable:  "users",
						ReferencedColumn: "id",
						ConstraintName:   "posts_user_fk",
					},
				},
			},
		},
	}

	cfg := Config{
		AllowTables: []string{"*"},
		AllowColumns: map[string][]string{
			"*": {"*"},
		},
		DenyColumns: map[string][]string{
			"posts": {"user_id"},
		},
	}

	Apply(schema, cfg)

	posts := findTable(schema, "posts")
	if posts == nil {
		t.Fatalf("expected posts table to remain")
	}
	if len(posts.ForeignKeys) != 0 {
		t.Fatalf("expected foreign keys removed, got %+v", posts.ForeignKeys)
	}
	if len(posts.Relationships) != 0 {
		t.Fatalf("expected relationships removed, got %+v", posts.Relationships)
	}
}

func TestApply_FiltersOrderByOptionsWithIndexes(t *testing.T) {
	schema := &introspection.Schema{
		Tables: []introspection.Table{
			{
				Name: "users",
				Columns: []introspection.Column{
					{Name: "id", IsPrimaryKey: true},
					{Name: "email"},
					{Name: "password_hash"},
				},
				Indexes: []introspection.Index{
					{Name: "idx_email", Columns: []string{"email"}, Unique: false},
					{Name: "idx_password", Columns: []string{"password_hash"}, Unique: false},
				},
			},
		},
	}

	cfg := Config{
		AllowTables: []string{"*"},
		AllowColumns: map[string][]string{
			"*": {"*"},
		},
		DenyColumns: map[string][]string{
			"users": {"password_*"},
		},
	}

	Apply(schema, cfg)

	table := schema.Tables[0]
	orderBy := planner.OrderByOptions(table)
	if _, ok := orderBy["email"]; !ok {
		t.Fatalf("expected email to remain in orderBy options, got %+v", orderBy)
	}
	if _, ok := orderBy["passwordHash"]; ok {
		t.Fatalf("expected password_hash to be removed from orderBy options")
	}
}

func TestApply_ScanViews(t *testing.T) {
	schema := &introspection.Schema{
		Tables: []introspection.Table{
			{Name: "users", Columns: []introspection.Column{{Name: "id"}}},
			{Name: "active_users", IsView: true, Columns: []introspection.Column{{Name: "id"}}},
		},
	}

	Apply(schema, Config{})
	if len(schema.Tables) != 1 || schema.Tables[0].Name != "users" {
		t.Fatalf("expected views to be skipped by default, got %+v", schema.Tables)
	}

	schema = &introspection.Schema{
		Tables: []introspection.Table{
			{Name: "users", Columns: []introspection.Column{{Name: "id"}}},
			{Name: "active_users", IsView: true, Columns: []introspection.Column{{Name: "id"}}},
		},
	}

	Apply(schema, Config{ScanViews: true, AllowTables: []string{"*"}})
	if len(schema.Tables) != 2 {
		t.Fatalf("expected views to be included when scan_views is enabled, got %+v", schema.Tables)
	}
}

func findTable(schema *introspection.Schema, name string) *introspection.Table {
	for i := range schema.Tables {
		if schema.Tables[i].Name == name {
			return &schema.Tables[i]
		}
	}
	return nil
}
