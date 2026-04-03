package introspection

import "tidb-graphql/internal/sqlutil"

// MapKey returns a string suitable for use as a map key that uniquely
// identifies the table.
//
// When Key is non-zero it delegates to Key.MapKey() (e.g. "mydb.users" in
// multi-database mode, "users" in single-database mode when Key.Database is
// empty but Key.Table is set).
//
// When Key is the zero value (both Database and Table empty), it falls back
// to the bare Name so that tables created in tests or legacy code without an
// explicit Key still behave correctly.
func (t Table) MapKey() string {
	if !t.Key.IsZero() {
		return t.Key.MapKey()
	}
	return t.Name
}

// SQLFrom returns the fully-qualified, backtick-quoted SQL table reference
// suitable for use in a FROM clause.
//
// When Key.Table is non-empty it delegates to Key.QualifiedSQL() so that
// multi-database queries produce `database`.`table`.
//
// When Key is the zero value (e.g. tables created in tests or caller code
// that does not set Key), it falls back to sqlutil.QuoteIdentifier(Name)
// preserving identical behaviour to the pre-TableKey codebase.
func (t Table) SQLFrom() string {
	if !t.Key.IsZero() {
		return t.Key.QualifiedSQL()
	}
	return sqlutil.QuoteIdentifier(t.Name)
}
