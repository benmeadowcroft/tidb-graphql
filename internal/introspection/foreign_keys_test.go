package introspection

import "testing"

func TestForeignKeyConstraints_GroupsByConstraintName(t *testing.T) {
	table := Table{
		Name: "membership",
		ForeignKeys: []ForeignKey{
			{ConstraintName: "fk_user", ColumnName: "tenant_id", ReferencedTable: "users", ReferencedColumn: "tenant_id", OrdinalPosition: 1},
			{ConstraintName: "fk_user", ColumnName: "user_id", ReferencedTable: "users", ReferencedColumn: "id", OrdinalPosition: 2},
			{ConstraintName: "fk_group", ColumnName: "group_tenant_id", ReferencedTable: "groups", ReferencedColumn: "tenant_id", OrdinalPosition: 1},
			{ConstraintName: "fk_group", ColumnName: "group_id", ReferencedTable: "groups", ReferencedColumn: "id", OrdinalPosition: 2},
		},
	}

	got := ForeignKeyConstraints(table)
	if len(got) != 2 {
		t.Fatalf("expected 2 FK constraints, got %d", len(got))
	}

	if got[0].ConstraintName != "fk_group" {
		t.Fatalf("expected first constraint fk_group, got %s", got[0].ConstraintName)
	}
	if got[1].ConstraintName != "fk_user" {
		t.Fatalf("expected second constraint fk_user, got %s", got[1].ConstraintName)
	}
	if len(got[1].ColumnNames) != 2 || got[1].ColumnNames[0] != "tenant_id" || got[1].ColumnNames[1] != "user_id" {
		t.Fatalf("unexpected grouped local columns: %#v", got[1].ColumnNames)
	}
	if len(got[1].ReferencedColumns) != 2 || got[1].ReferencedColumns[0] != "tenant_id" || got[1].ReferencedColumns[1] != "id" {
		t.Fatalf("unexpected grouped referenced columns: %#v", got[1].ReferencedColumns)
	}
}

func TestForeignKeyConstraints_UnnamedRowsStayIsolated(t *testing.T) {
	table := Table{
		Name: "posts",
		ForeignKeys: []ForeignKey{
			{ColumnName: "author_id", ReferencedTable: "users", ReferencedColumn: "id"},
			{ColumnName: "editor_id", ReferencedTable: "users", ReferencedColumn: "id"},
		},
	}

	got := ForeignKeyConstraints(table)
	if len(got) != 2 {
		t.Fatalf("expected 2 unnamed FK constraints, got %d", len(got))
	}
	if got[0].ColumnNames[0] == got[1].ColumnNames[0] {
		t.Fatalf("expected isolated unnamed constraints, got %#v", got)
	}
}
