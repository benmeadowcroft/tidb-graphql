-- Seed data for mutation integration tests

INSERT INTO categories (id, name, description) VALUES
    (1, 'Electronics', 'Electronic devices and accessories'),
    (2, 'Clothing', 'Apparel and fashion items'),
    (3, 'Books', 'Books and publications');

INSERT INTO products (id, category_id, sku, name, price, stock_quantity, is_active) VALUES
    (1, 1, 'ELEC-001', 'Laptop', 999.99, 50, TRUE),
    (2, 1, 'ELEC-002', 'Wireless Mouse', 29.99, 200, TRUE),
    (3, 2, 'CLTH-001', 'T-Shirt', 19.99, 100, TRUE),
    (4, 3, 'BOOK-001', 'Programming Guide', 49.99, 30, TRUE);

INSERT INTO order_items (order_id, line_number, product_id, quantity, unit_price) VALUES
    (100, 1, 1, 1, 999.99),
    (100, 2, 2, 2, 29.99),
    (101, 1, 3, 3, 19.99);

INSERT INTO inventory (id, product_id, quantity, unit_cost, location) VALUES
    (1, 1, 50, 750.00, 'Warehouse A'),
    (2, 2, 200, 15.00, 'Warehouse A'),
    (3, 3, 100, 8.00, 'Warehouse B');
