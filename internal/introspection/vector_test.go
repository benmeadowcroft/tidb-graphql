package introspection

import "testing"

func TestParseVectorDimension(t *testing.T) {
	tests := []struct {
		name       string
		columnType string
		want       int
	}{
		{name: "unspecified", columnType: "vector", want: 0},
		{name: "fixed", columnType: "vector(1536)", want: 1536},
		{name: "fixed with spaces", columnType: " VECTOR(3) ", want: 3},
		{name: "invalid", columnType: "vector(x)", want: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := parseVectorDimension(tt.columnType); got != tt.want {
				t.Fatalf("parseVectorDimension(%q) = %d, want %d", tt.columnType, got, tt.want)
			}
		})
	}
}

func TestVectorColumnsAndIndexes(t *testing.T) {
	table := Table{
		Name: "docs",
		Columns: []Column{
			{Name: "id", DataType: "bigint", IsPrimaryKey: true},
			{Name: "embedding", DataType: "vector", ColumnType: "vector(3)", VectorDimension: 3},
			{Name: "title", DataType: "varchar"},
		},
		Indexes: []Index{
			{Name: "idx_embedding", Type: "HNSW", Columns: []string{"embedding"}},
			{Name: "idx_title", Type: "BTREE", Columns: []string{"title"}},
		},
	}

	cols := VectorColumns(table)
	if len(cols) != 1 || cols[0].Name != "embedding" {
		t.Fatalf("expected embedding as vector column, got %+v", cols)
	}
	if !HasVectorIndexForColumn(table, "embedding") {
		t.Fatalf("expected embedding to have vector index")
	}
	if HasVectorIndexForColumn(table, "title") {
		t.Fatalf("did not expect title to have vector index")
	}
}
