package introspection

import (
	"testing"

	"tidb-graphql/internal/naming"
)

func TestRebuildRelationshipsWithJunctions_CompositeMappings(t *testing.T) {
	schema := &Schema{
		Tables: []Table{
			{
				Name: "users",
				Columns: []Column{
					{Name: "tenant_id", IsPrimaryKey: true},
					{Name: "id", IsPrimaryKey: true},
				},
			},
			{
				Name: "groups",
				Columns: []Column{
					{Name: "tenant_id", IsPrimaryKey: true},
					{Name: "id", IsPrimaryKey: true},
				},
			},
			{
				Name: "user_groups",
				Columns: []Column{
					{Name: "tenant_id", IsPrimaryKey: true},
					{Name: "user_id", IsPrimaryKey: true},
					{Name: "group_tenant_id", IsPrimaryKey: true},
					{Name: "group_id", IsPrimaryKey: true},
				},
			},
		},
	}

	junctions := JunctionMap{
		"user_groups": {
			Table: "user_groups",
			Type:  JunctionTypePure,
			LeftFK: JunctionFKInfo{
				ConstraintName:    "fk_user_groups_users",
				ReferencedTable:   "users",
				ColumnNames:       []string{"tenant_id", "user_id"},
				ReferencedColumns: []string{"tenant_id", "id"},
				ColumnName:        "tenant_id",
				ReferencedColumn:  "tenant_id",
			},
			RightFK: JunctionFKInfo{
				ConstraintName:    "fk_user_groups_groups",
				ReferencedTable:   "groups",
				ColumnNames:       []string{"group_tenant_id", "group_id"},
				ReferencedColumns: []string{"tenant_id", "id"},
				ColumnName:        "group_tenant_id",
				ReferencedColumn:  "tenant_id",
			},
		},
	}

	if err := RebuildRelationshipsWithJunctions(schema, naming.Default(), junctions); err != nil {
		t.Fatalf("failed to rebuild relationships: %v", err)
	}

	var userRel *Relationship
	for i := range schema.Tables[0].Relationships {
		rel := &schema.Tables[0].Relationships[i]
		if rel.IsManyToMany && rel.RemoteTable == "groups" {
			userRel = rel
			break
		}
	}
	if userRel == nil {
		t.Fatalf("expected users -> groups many-to-many relationship")
	}
	if len(userRel.LocalColumns) != 2 || userRel.LocalColumns[0] != "tenant_id" || userRel.LocalColumns[1] != "id" {
		t.Fatalf("unexpected local columns: %#v", userRel.LocalColumns)
	}
	if len(userRel.JunctionLocalFKColumns) != 2 || userRel.JunctionLocalFKColumns[0] != "tenant_id" || userRel.JunctionLocalFKColumns[1] != "user_id" {
		t.Fatalf("unexpected junction local columns: %#v", userRel.JunctionLocalFKColumns)
	}
	if userRel.LocalColumn != "tenant_id" || userRel.JunctionLocalFK != "tenant_id" {
		t.Fatalf("single-column compatibility fields were not populated")
	}
}
