package tablekey_test

import (
	"testing"

	"tidb-graphql/internal/tablekey"
)

func TestTableKey_SingleDatabase(t *testing.T) {
	k := tablekey.TableKey{Table: "users"}

	if got := k.QualifiedSQL(); got != "`users`" {
		t.Errorf("QualifiedSQL() = %q, want %q", got, "`users`")
	}
	if got := k.MapKey(); got != "users" {
		t.Errorf("MapKey() = %q, want %q", got, "users")
	}
	if got := k.String(); got != "users" {
		t.Errorf("String() = %q, want %q", got, "users")
	}
	if k.IsZero() {
		t.Error("IsZero() = true, want false")
	}
}

func TestTableKey_MultiDatabase(t *testing.T) {
	k := tablekey.TableKey{Database: "myapp", Table: "orders"}

	if got := k.QualifiedSQL(); got != "`myapp`.`orders`" {
		t.Errorf("QualifiedSQL() = %q, want %q", got, "`myapp`.`orders`")
	}
	if got := k.MapKey(); got != "myapp.orders" {
		t.Errorf("MapKey() = %q, want %q", got, "myapp.orders")
	}
	if got := k.String(); got != "myapp.orders" {
		t.Errorf("String() = %q, want %q", got, "myapp.orders")
	}
	if k.IsZero() {
		t.Error("IsZero() = true, want false")
	}
}

func TestTableKey_BacktickEscaping(t *testing.T) {
	k := tablekey.TableKey{Database: "my`db", Table: "my`table"}

	if got := k.QualifiedSQL(); got != "`my``db`.`my``table`" {
		t.Errorf("QualifiedSQL() = %q, want %q", got, "`my``db`.`my``table`")
	}
	// MapKey uses raw strings (not SQL-escaped)
	if got := k.MapKey(); got != "my`db.my`table" {
		t.Errorf("MapKey() = %q, want %q", got, "my`db.my`table")
	}
}

func TestTableKey_Zero(t *testing.T) {
	var k tablekey.TableKey
	if !k.IsZero() {
		t.Error("IsZero() = false for zero value, want true")
	}
	if got := k.MapKey(); got != "" {
		t.Errorf("MapKey() = %q for zero value, want %q", got, "")
	}
	if got := k.QualifiedSQL(); got != "``" {
		t.Errorf("QualifiedSQL() = %q for zero value, want %q", got, "``")
	}
}

func TestTableKey_MapKeyUniqueness(t *testing.T) {
	// Ensure tables with the same name in different databases have distinct map keys.
	k1 := tablekey.TableKey{Database: "db1", Table: "users"}
	k2 := tablekey.TableKey{Database: "db2", Table: "users"}
	k3 := tablekey.TableKey{Table: "users"}

	if k1.MapKey() == k2.MapKey() {
		t.Errorf("db1.users and db2.users should have different MapKeys: %q == %q", k1.MapKey(), k2.MapKey())
	}
	if k1.MapKey() == k3.MapKey() {
		t.Errorf("db1.users and (bare) users should have different MapKeys: %q == %q", k1.MapKey(), k3.MapKey())
	}
}

func TestTableKey_FindTableByKey(t *testing.T) {
	// Simulate the map-based findTableByKey lookup replacing the O(n) linear scan.
	type mockTable struct {
		key  tablekey.TableKey
		name string
	}
	tables := []mockTable{
		{key: tablekey.TableKey{Database: "db1", Table: "users"}, name: "users"},
		{key: tablekey.TableKey{Database: "db2", Table: "users"}, name: "users"},
		{key: tablekey.TableKey{Database: "db1", Table: "orders"}, name: "orders"},
	}

	index := make(map[string]*mockTable)
	for i := range tables {
		index[tables[i].key.MapKey()] = &tables[i]
	}

	find := func(k tablekey.TableKey) *mockTable { return index[k.MapKey()] }

	if got := find(tablekey.TableKey{Database: "db1", Table: "users"}); got == nil {
		t.Error("expected to find db1.users, got nil")
	}
	if got := find(tablekey.TableKey{Database: "db2", Table: "users"}); got == nil {
		t.Error("expected to find db2.users, got nil")
	}
	if got := find(tablekey.TableKey{Database: "db3", Table: "users"}); got != nil {
		t.Errorf("expected nil for db3.users, got %+v", got)
	}
}
