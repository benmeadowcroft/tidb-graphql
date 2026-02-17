package cursor

import (
	"testing"
	"time"

	"tidb-graphql/internal/introspection"
)

func TestEncodeDecode_Roundtrip(t *testing.T) {
	tests := []struct {
		name       string
		typeName   string
		orderByKey string
		directions []string
		values     []interface{}
	}{
		{
			name:       "single int PK",
			typeName:   "User",
			orderByKey: "databaseId",
			directions: []string{"ASC"},
			values:     []interface{}{int64(42)},
		},
		{
			name:       "multi-column cursor",
			typeName:   "Post",
			orderByKey: "createdAt_databaseId",
			directions: []string{"DESC", "ASC"},
			values:     []interface{}{"2024-01-15T10:30:00Z", int64(7)},
		},
		{
			name:       "string value",
			typeName:   "User",
			orderByKey: "name",
			directions: []string{"ASC"},
			values:     []interface{}{"Alice"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encoded := EncodeCursor(tt.typeName, tt.orderByKey, tt.directions, tt.values...)
			if encoded == "" {
				t.Fatal("EncodeCursor returned empty string")
			}

			gotType, gotKey, gotDirs, gotValues, err := DecodeCursor(encoded)
			if err != nil {
				t.Fatalf("DecodeCursor error: %v", err)
			}
			if gotType != tt.typeName {
				t.Errorf("typeName: got %q, want %q", gotType, tt.typeName)
			}
			if gotKey != tt.orderByKey {
				t.Errorf("orderByKey: got %q, want %q", gotKey, tt.orderByKey)
			}
			if len(gotDirs) != len(tt.directions) {
				t.Fatalf("directions count: got %d, want %d", len(gotDirs), len(tt.directions))
			}
			for i := range tt.directions {
				if gotDirs[i] != tt.directions[i] {
					t.Errorf("direction[%d]: got %q, want %q", i, gotDirs[i], tt.directions[i])
				}
			}
			if len(gotValues) != len(tt.values) {
				t.Fatalf("values count: got %d, want %d", len(gotValues), len(tt.values))
			}
		})
	}
}

func TestDecodeCursor_Errors(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{"invalid base64", "not-valid-base64!!!"},
		{"invalid json", "bm90LWpzb24="},                            // "not-json"
		{"legacy array format", "WyJVc2VyIiwiaWQiLCJBU0MiLCIxIl0="}, // ["User","id","ASC","1"]
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, _, _, _, err := DecodeCursor(tt.input)
			if err == nil {
				t.Fatal("expected error, got nil")
			}
		})
	}
}

func TestValidateCursor(t *testing.T) {
	// matching
	if err := ValidateCursor("User", "databaseId", []string{"ASC"}, "User", "databaseId", []string{"ASC"}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// type mismatch
	if err := ValidateCursor("User", "databaseId", []string{"ASC"}, "Post", "databaseId", []string{"ASC"}); err == nil {
		t.Fatal("expected type mismatch error")
	}

	// orderBy mismatch
	if err := ValidateCursor("User", "databaseId", []string{"ASC"}, "User", "name", []string{"ASC"}); err == nil {
		t.Fatal("expected orderBy mismatch error")
	}

	// direction mismatch
	if err := ValidateCursor("User", "databaseId", []string{"ASC"}, "User", "databaseId", []string{"DESC"}); err == nil {
		t.Fatal("expected direction mismatch error")
	}
}

func TestParseCursorValues(t *testing.T) {
	cols := []introspection.Column{
		{Name: "id", DataType: "int"},
		{Name: "name", DataType: "varchar"},
	}

	values, err := ParseCursorValues([]string{"42", "Alice"}, cols)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(values) != 2 {
		t.Fatalf("expected 2 values, got %d", len(values))
	}
	if values[0] != int64(42) {
		t.Errorf("values[0]: got %v (%T), want int64(42)", values[0], values[0])
	}
	if values[1] != "Alice" {
		t.Errorf("values[1]: got %v, want Alice", values[1])
	}
}

func TestParseCursorValues_CountMismatch(t *testing.T) {
	cols := []introspection.Column{
		{Name: "id", DataType: "int"},
	}
	_, err := ParseCursorValues([]string{"1", "2"}, cols)
	if err == nil {
		t.Fatal("expected count mismatch error")
	}
}

func TestParseCursorValues_DateColumn(t *testing.T) {
	cols := []introspection.Column{
		{Name: "created_at", DataType: "datetime"},
	}
	values, err := ParseCursorValues([]string{"2024-01-15T10:30:00Z"}, cols)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	ts, ok := values[0].(time.Time)
	if !ok {
		t.Fatalf("expected time.Time, got %T", values[0])
	}
	if ts.Year() != 2024 || ts.Month() != 1 || ts.Day() != 15 {
		t.Errorf("unexpected date: %v", ts)
	}
}

func TestCoerceToString(t *testing.T) {
	tests := []struct {
		input    interface{}
		expected string
	}{
		{int64(42), "42"},
		{"hello", "hello"},
		{float64(3.14), "3.14"},
		{true, "true"},
		{time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC), "2024-01-15T10:30:00Z"},
	}

	for _, tt := range tests {
		got := coerceToString(tt.input)
		if got != tt.expected {
			t.Errorf("coerceToString(%v): got %q, want %q", tt.input, got, tt.expected)
		}
	}
}

func TestParseVectorCursorValues(t *testing.T) {
	pkCols := []introspection.Column{
		{Name: "id", DataType: "bigint", IsPrimaryKey: true},
	}

	distance, pkValues, err := ParseVectorCursorValues([]string{"0.42", "7"}, pkCols)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if distance != 0.42 {
		t.Fatalf("distance = %v, want 0.42", distance)
	}
	if len(pkValues) != 1 || pkValues[0] != int64(7) {
		t.Fatalf("pkValues = %#v, want [int64(7)]", pkValues)
	}
}

func TestParseVectorCursorValues_Errors(t *testing.T) {
	pkCols := []introspection.Column{
		{Name: "id", DataType: "bigint", IsPrimaryKey: true},
	}

	if _, _, err := ParseVectorCursorValues([]string{"0.5"}, pkCols); err == nil {
		t.Fatal("expected count mismatch error")
	}
	if _, _, err := ParseVectorCursorValues([]string{"nan", "1"}, pkCols); err == nil {
		t.Fatal("expected invalid distance error")
	}
	if _, _, err := ParseVectorCursorValues([]string{"0.5", "bad-int"}, pkCols); err == nil {
		t.Fatal("expected invalid PK error")
	}
}
