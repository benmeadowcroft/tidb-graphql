-- Warehouses (single column PK)
INSERT INTO warehouses (id, name, location) VALUES
    (1, 'Main Warehouse', 'New York'),
    (2, 'West Coast Hub', 'Los Angeles');

-- Order items (two-column composite PK)
INSERT INTO order_items (order_id, product_id, quantity, unit_price) VALUES
    (100, 1, 2, 29.99),
    (100, 2, 1, 49.99),
    (100, 3, 5, 9.99),
    (101, 1, 1, 29.99),
    (101, 4, 3, 19.99),
    (102, 2, 2, 49.99);

-- Inventory locations (three-column composite PK)
INSERT INTO inventory_locations (warehouse_id, aisle, shelf, product_id, quantity) VALUES
    (1, 'A', 1, 1, 100),
    (1, 'A', 2, 2, 50),
    (1, 'B', 1, 3, 200),
    (1, 'B', 2, 4, 75),
    (2, 'A', 1, 1, 150),
    (2, 'A', 2, 2, 80),
    (2, 'C', 1, 5, 300);
