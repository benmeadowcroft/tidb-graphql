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

func TestHasVectorIndexForColumn_UsesVectorCapabilityFlag(t *testing.T) {
	table := Table{
		Name: "docs",
		Columns: []Column{
			{Name: "id", DataType: "bigint", IsPrimaryKey: true},
			{Name: "embedding", DataType: "vector", ColumnType: "vector(3)", VectorDimension: 3},
		},
		Indexes: []Index{
			{
				Name:                  "idx_embedding_vector",
				Type:                  "BTREE",
				Columns:               []string{"embedding"},
				IsVectorSearchCapable: true,
			},
		},
	}

	if !HasVectorIndexForColumn(table, "embedding") {
		t.Fatalf("expected embedding to have vector index via capability flag")
	}
}

func TestHasVectorIndexForColumn_HNSWFallback(t *testing.T) {
	table := Table{
		Name: "docs",
		Columns: []Column{
			{Name: "id", DataType: "bigint", IsPrimaryKey: true},
			{Name: "embedding", DataType: "vector", ColumnType: "vector(3)", VectorDimension: 3},
		},
		Indexes: []Index{
			{
				Name:    "idx_embedding_hnsw",
				Type:    "HNSW",
				Columns: []string{"embedding"},
			},
		},
	}

	if !HasVectorIndexForColumn(table, "embedding") {
		t.Fatalf("expected embedding to have vector index via HNSW fallback")
	}
}

func TestIsAutoEmbeddingVectorColumn(t *testing.T) {
	tests := []struct {
		name string
		col  Column
		want bool
	}{
		{
			name: "generated vector with embed_text",
			col: Column{
				Name:                 "embedding",
				DataType:             "vector",
				ColumnType:           "vector(1024)",
				VectorDimension:      1024,
				GenerationExpression: `EMBED_TEXT("tidbcloud_free/amazon/titan-embed-text-v2", review_text, '{"dimensions":1024}')`,
			},
			want: true,
		},
		{
			name: "case insensitive embed_text detection",
			col: Column{
				Name:                 "embedding",
				DataType:             "vector",
				ColumnType:           "vector(1024)",
				VectorDimension:      1024,
				GenerationExpression: `embed_text("model", review_text)`,
			},
			want: true,
		},
		{
			name: "plain vector column",
			col: Column{
				Name:       "embedding",
				DataType:   "vector",
				ColumnType: "vector(8)",
			},
			want: false,
		},
		{
			name: "non vector generated column",
			col: Column{
				Name:                 "search_text",
				DataType:             "varchar",
				ColumnType:           "varchar(255)",
				GenerationExpression: `EMBED_TEXT("model", review_text)`,
			},
			want: false,
		},
		{
			name: "generated vector without embed_text",
			col: Column{
				Name:                 "embedding",
				DataType:             "vector",
				ColumnType:           "vector(8)",
				GenerationExpression: "some_other_function(review_text)",
			},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := IsAutoEmbeddingVectorColumn(tt.col)
			if got != tt.want {
				t.Fatalf("IsAutoEmbeddingVectorColumn() = %v, want %v", got, tt.want)
			}
		})
	}
}
