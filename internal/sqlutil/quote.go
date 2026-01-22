// Package sqlutil provides SQL utility functions.
package sqlutil

import "strings"

// QuoteIdentifier quotes a SQL identifier (table name, column name, etc.)
// with backticks and escapes any backticks within the identifier.
func QuoteIdentifier(name string) string {
	escaped := strings.ReplaceAll(name, "`", "``")
	return "`" + escaped + "`"
}

// QuoteString quotes a SQL string literal with single quotes and escapes
// any single quotes within the string by doubling them.
func QuoteString(s string) string {
	escaped := strings.ReplaceAll(s, "'", "''")
	return "'" + escaped + "'"
}
