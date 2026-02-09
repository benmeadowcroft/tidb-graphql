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
		direction  string
		values     []interface{}
	}{
		{
			name:       "single int PK",
			typeName:   "User",
			orderByKey: "databaseId",
			direction:  "ASC",
			values:     []interface{}{int64(42)},
		},
		{
			name:       "multi-column cursor",
			typeName:   "Post",
			orderByKey: "createdAt_databaseId",
			direction:  "DESC",
			values:     []interface{}{"2024-01-15T10:30:00Z", int64(7)},
		},
		{
			name:       "string value",
			typeName:   "User",
			orderByKey: "name",
			direction:  "ASC",
			values:     []interface{}{"Alice"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encoded := EncodeCursor(tt.typeName, tt.orderByKey, tt.direction, tt.values...)
			if encoded == "" {
				t.Fatal("EncodeCursor returned empty string")
			}

			gotType, gotKey, gotDir, gotValues, err := DecodeCursor(encoded)
			if err != nil {
				t.Fatalf("DecodeCursor error: %v", err)
			}
			if gotType != tt.typeName {
				t.Errorf("typeName: got %q, want %q", gotType, tt.typeName)
			}
			if gotKey != tt.orderByKey {
				t.Errorf("orderByKey: got %q, want %q", gotKey, tt.orderByKey)
			}
			if gotDir != tt.direction {
				t.Errorf("direction: got %q, want %q", gotDir, tt.direction)
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
		{"invalid json", "bm90LWpzb24="},                           // "not-json"
		{"too few elements", "WyJVc2VyIiwiQVNDIl0="},               // ["User","ASC"]
		{"three elements", "WyJVc2VyIiwiaWQiLCJBU0MiXQ=="},         // ["User","id","ASC"]
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
	if err := ValidateCursor("User", "databaseId", "ASC", "User", "databaseId", "ASC"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// type mismatch
	if err := ValidateCursor("User", "databaseId", "ASC", "Post", "databaseId", "ASC"); err == nil {
		t.Fatal("expected type mismatch error")
	}

	// orderBy mismatch
	if err := ValidateCursor("User", "databaseId", "ASC", "User", "name", "ASC"); err == nil {
		t.Fatal("expected orderBy mismatch error")
	}

	// direction mismatch
	if err := ValidateCursor("User", "databaseId", "ASC", "User", "databaseId", "DESC"); err == nil {
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
