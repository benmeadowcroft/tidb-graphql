// Package tablekey provides the canonical identity type for tables across
// single- and multi-database deployments.
//
// In single-database mode (Key.Database == ""), all methods produce output
// identical to the legacy bare-name behaviour, so there is no behaviour change
// for existing single-database users.
package tablekey

import "tidb-graphql/internal/sqlutil"

// TableKey uniquely identifies a table within a (possibly multi-database)
// schema.  Database is the physical SQL TABLE_SCHEMA name; Table is the SQL
// TABLE_NAME.
//
// When Database is empty the key represents a table in the single, implicit
// database — all methods fall back to single-database behaviour.
type TableKey struct {
	Database string // physical SQL TABLE_SCHEMA; empty in single-db mode
	Table    string // SQL TABLE_NAME
}

// QualifiedSQL returns the fully-qualified backtick-quoted SQL reference to
// the table.
//
//	Key.Database == ""  →  `table`
//	Key.Database != ""  →  `database`.`table`
func (k TableKey) QualifiedSQL() string {
	if k.Database == "" {
		return sqlutil.QuoteIdentifier(k.Table)
	}
	return sqlutil.QuoteIdentifier(k.Database) + "." + sqlutil.QuoteIdentifier(k.Table)
}

// MapKey returns a string suitable for use as a map key that uniquely
// identifies the table.  Dot-delimited format matches the format used in
// user-facing config (type_overrides keys, etc.).
//
//	Key.Database == ""  →  "table"
//	Key.Database != ""  →  "database.table"
func (k TableKey) MapKey() string {
	if k.Database == "" {
		return k.Table
	}
	return k.Database + "." + k.Table
}

// String implements fmt.Stringer; returns the same value as MapKey.
func (k TableKey) String() string {
	return k.MapKey()
}

// IsZero reports whether the key is the zero value (both fields empty).
func (k TableKey) IsZero() bool {
	return k.Database == "" && k.Table == ""
}
