package introspection

import (
	"testing"
)

func TestExtractCrossDBForeignKeysFromCreateSQL(t *testing.T) {
	// Typical SHOW CREATE TABLE output for tidb_graphql_shipping.shipments.
	shipmentsCreateSQL := "CREATE TABLE `shipments` (\n" +
		"  `id` bigint NOT NULL /*T![auto_rand] AUTO_RANDOM(5) */,\n" +
		"  `order_id` bigint NOT NULL COMMENT 'References tidb_graphql_tutorial.orders.id.',\n" +
		"  `carrier_id` bigint NOT NULL,\n" +
		"  `tracking_number` varchar(128) DEFAULT NULL,\n" +
		"  `status` enum('pending','in_transit','delivered','returned','exception') NOT NULL DEFAULT 'pending',\n" +
		"  PRIMARY KEY (`id`) /*T![clustered_index] CLUSTERED */,\n" +
		"  UNIQUE KEY `uq_shipments_tracking` (`carrier_id`,`tracking_number`),\n" +
		"  KEY `idx_shipments_order` (`order_id`),\n" +
		"  KEY `idx_shipments_status` (`status`),\n" +
		"  CONSTRAINT `fk_shipments_carrier` FOREIGN KEY (`carrier_id`) REFERENCES `carriers` (`id`),\n" +
		"  CONSTRAINT `fk_shipments_order` FOREIGN KEY (`order_id`) REFERENCES `tidb_graphql_tutorial`.`orders` (`id`)\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"

	fks := extractCrossDBForeignKeysFromCreateSQL(shipmentsCreateSQL, "tidb_graphql_shipping")

	// Should only return the cross-db FK (fk_shipments_order), not fk_shipments_carrier.
	if len(fks) != 1 {
		t.Fatalf("expected 1 cross-db FK, got %d: %+v", len(fks), fks)
	}

	fk := fks[0]
	if fk.ConstraintName != "fk_shipments_order" {
		t.Errorf("expected constraint name %q, got %q", "fk_shipments_order", fk.ConstraintName)
	}
	if fk.ColumnName != "order_id" {
		t.Errorf("expected column name %q, got %q", "order_id", fk.ColumnName)
	}
	if fk.ReferencedDatabase != "tidb_graphql_tutorial" {
		t.Errorf("expected referenced database %q, got %q", "tidb_graphql_tutorial", fk.ReferencedDatabase)
	}
	if fk.ReferencedTable != "orders" {
		t.Errorf("expected referenced table %q, got %q", "orders", fk.ReferencedTable)
	}
	if fk.ReferencedColumn != "id" {
		t.Errorf("expected referenced column %q, got %q", "id", fk.ReferencedColumn)
	}
	if fk.OrdinalPosition != 1 {
		t.Errorf("expected ordinal position 1, got %d", fk.OrdinalPosition)
	}
}

func TestExtractCrossDBForeignKeysFromCreateSQL_NoFK(t *testing.T) {
	createSQL := "CREATE TABLE `carriers` (\n" +
		"  `id` bigint NOT NULL,\n" +
		"  `code` varchar(16) NOT NULL,\n" +
		"  PRIMARY KEY (`id`)\n" +
		") ENGINE=InnoDB"

	fks := extractCrossDBForeignKeysFromCreateSQL(createSQL, "tidb_graphql_shipping")
	if len(fks) != 0 {
		t.Errorf("expected 0 cross-db FKs, got %d", len(fks))
	}
}

func TestExtractCrossDBForeignKeysFromCreateSQL_IntraDBOnly(t *testing.T) {
	// Intra-db FK only — should not be returned.
	createSQL := "CREATE TABLE `shipment_tracking` (\n" +
		"  `id` bigint NOT NULL,\n" +
		"  `shipment_id` bigint NOT NULL,\n" +
		"  PRIMARY KEY (`id`),\n" +
		"  CONSTRAINT `fk_tracking_shipment` FOREIGN KEY (`shipment_id`) REFERENCES `shipments` (`id`)\n" +
		") ENGINE=InnoDB"

	fks := extractCrossDBForeignKeysFromCreateSQL(createSQL, "tidb_graphql_shipping")
	if len(fks) != 0 {
		t.Errorf("expected 0 cross-db FKs for intra-db constraint, got %d: %+v", len(fks), fks)
	}
}

func TestExtractCrossDBForeignKeysFromCreateSQL_CompositeCrossDBFK(t *testing.T) {
	createSQL := "CREATE TABLE `t` (\n" +
		"  `a` bigint NOT NULL,\n" +
		"  `b` bigint NOT NULL,\n" +
		"  PRIMARY KEY (`a`, `b`),\n" +
		"  CONSTRAINT `fk_composite` FOREIGN KEY (`a`, `b`) REFERENCES `other_db`.`parent` (`x`, `y`)\n" +
		") ENGINE=InnoDB"

	fks := extractCrossDBForeignKeysFromCreateSQL(createSQL, "mydb")

	if len(fks) != 2 {
		t.Fatalf("expected 2 FK rows (one per column), got %d: %+v", len(fks), fks)
	}

	if fks[0].ConstraintName != "fk_composite" || fks[1].ConstraintName != "fk_composite" {
		t.Errorf("unexpected constraint names: %q, %q", fks[0].ConstraintName, fks[1].ConstraintName)
	}
	if fks[0].ColumnName != "a" || fks[1].ColumnName != "b" {
		t.Errorf("unexpected column names: %q, %q", fks[0].ColumnName, fks[1].ColumnName)
	}
	if fks[0].ReferencedColumn != "x" || fks[1].ReferencedColumn != "y" {
		t.Errorf("unexpected referenced columns: %q, %q", fks[0].ReferencedColumn, fks[1].ReferencedColumn)
	}
	if fks[0].OrdinalPosition != 1 || fks[1].OrdinalPosition != 2 {
		t.Errorf("unexpected ordinal positions: %d, %d", fks[0].OrdinalPosition, fks[1].OrdinalPosition)
	}
}
