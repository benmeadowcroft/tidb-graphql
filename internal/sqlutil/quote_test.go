package sqlutil

import "testing"

func TestQuoteIdentifier(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"users", "`users`"},
		{"user_data", "`user_data`"},
		{"select", "`select`"},           // reserved word
		{"first name", "`first name`"},   // space in name
		{"user`data", "`user``data`"},    // backtick in name
		{"a`b`c", "`a``b``c`"},           // multiple backticks
		{"", "``"},                        // empty string
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result := QuoteIdentifier(tt.input)
			if result != tt.expected {
				t.Errorf("QuoteIdentifier(%q) = %q, want %q", tt.input, result, tt.expected)
			}
		})
	}
}

func TestQuoteString(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"hello", "'hello'"},
		{"it's", "'it''s'"},                    // single quote
		{"a'b'c", "'a''b''c'"},                 // multiple quotes
		{"hello world", "'hello world'"},       // space
		{"", "''"},                             // empty string
		{"password123", "'password123'"},       // typical password
		{"pass'word", "'pass''word'"},          // quote in password
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result := QuoteString(tt.input)
			if result != tt.expected {
				t.Errorf("QuoteString(%q) = %q, want %q", tt.input, result, tt.expected)
			}
		})
	}
}
