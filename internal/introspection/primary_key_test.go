package introspection

import "testing"

func TestPrimaryKeyColumn(t *testing.T) {
	tests := []struct {
		name     string
		table    Table
		wantName string
		wantNil  bool
	}{
		{
			name: "single primary key",
			table: Table{
				Name: "users",
				Columns: []Column{
					{Name: "id", DataType: "int", IsPrimaryKey: true},
					{Name: "name", DataType: "varchar"},
				},
			},
			wantName: "id",
			wantNil:  false,
		},
		{
			name: "composite primary key returns first",
			table: Table{
				Name: "order_items",
				Columns: []Column{
					{Name: "order_id", DataType: "int", IsPrimaryKey: true},
					{Name: "product_id", DataType: "int", IsPrimaryKey: true},
					{Name: "quantity", DataType: "int"},
				},
			},
			wantName: "order_id",
			wantNil:  false,
		},
		{
			name: "no primary key",
			table: Table{
				Name: "logs",
				Columns: []Column{
					{Name: "message", DataType: "text"},
					{Name: "created_at", DataType: "datetime"},
				},
			},
			wantNil: true,
		},
		{
			name: "empty table",
			table: Table{
				Name:    "empty",
				Columns: []Column{},
			},
			wantNil: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := PrimaryKeyColumn(tt.table)
			if tt.wantNil {
				if result != nil {
					t.Errorf("PrimaryKeyColumn() = %v, want nil", result)
				}
				return
			}
			if result == nil {
				t.Errorf("PrimaryKeyColumn() = nil, want column named %q", tt.wantName)
				return
			}
			if result.Name != tt.wantName {
				t.Errorf("PrimaryKeyColumn().Name = %q, want %q", result.Name, tt.wantName)
			}
		})
	}
}

func TestPrimaryKeyColumns(t *testing.T) {
	tests := []struct {
		name      string
		table     Table
		wantNames []string
	}{
		{
			name: "single primary key",
			table: Table{
				Name: "users",
				Columns: []Column{
					{Name: "id", DataType: "int", IsPrimaryKey: true},
					{Name: "name", DataType: "varchar"},
				},
			},
			wantNames: []string{"id"},
		},
		{
			name: "composite primary key two columns",
			table: Table{
				Name: "order_items",
				Columns: []Column{
					{Name: "order_id", DataType: "int", IsPrimaryKey: true},
					{Name: "product_id", DataType: "int", IsPrimaryKey: true},
					{Name: "quantity", DataType: "int"},
				},
			},
			wantNames: []string{"order_id", "product_id"},
		},
		{
			name: "composite primary key three columns",
			table: Table{
				Name: "inventory_locations",
				Columns: []Column{
					{Name: "warehouse_id", DataType: "int", IsPrimaryKey: true},
					{Name: "aisle", DataType: "varchar", IsPrimaryKey: true},
					{Name: "shelf", DataType: "int", IsPrimaryKey: true},
					{Name: "product_id", DataType: "int"},
					{Name: "quantity", DataType: "int"},
				},
			},
			wantNames: []string{"warehouse_id", "aisle", "shelf"},
		},
		{
			name: "no primary key",
			table: Table{
				Name: "logs",
				Columns: []Column{
					{Name: "message", DataType: "text"},
					{Name: "created_at", DataType: "datetime"},
				},
			},
			wantNames: []string{},
		},
		{
			name: "empty table",
			table: Table{
				Name:    "empty",
				Columns: []Column{},
			},
			wantNames: []string{},
		},
		{
			name: "primary key columns not contiguous",
			table: Table{
				Name: "mixed",
				Columns: []Column{
					{Name: "pk1", DataType: "int", IsPrimaryKey: true},
					{Name: "data1", DataType: "varchar"},
					{Name: "pk2", DataType: "int", IsPrimaryKey: true},
					{Name: "data2", DataType: "varchar"},
				},
			},
			wantNames: []string{"pk1", "pk2"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := PrimaryKeyColumns(tt.table)

			if len(result) != len(tt.wantNames) {
				t.Errorf("PrimaryKeyColumns() returned %d columns, want %d", len(result), len(tt.wantNames))
				return
			}

			for i, col := range result {
				if col.Name != tt.wantNames[i] {
					t.Errorf("PrimaryKeyColumns()[%d].Name = %q, want %q", i, col.Name, tt.wantNames[i])
				}
			}
		})
	}
}
