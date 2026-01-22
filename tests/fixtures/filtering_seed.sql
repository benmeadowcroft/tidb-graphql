-- Seed data for filtering tests
-- Includes edge cases: nulls, empty strings, boundary values

-- Manufacturers
INSERT INTO manufacturers (id, name, country, email, phone, is_verified, rating, founded_year, created_at) VALUES
(1, 'Acme Corp', 'USA', 'contact@acme.com', '+1-555-0100', TRUE, 4.50, 1995, '2020-01-15 10:00:00'),
(2, 'Global Industries', 'Germany', 'info@global.de', NULL, TRUE, 4.80, 2000, '2020-02-20 11:00:00'),
(3, 'TechMakers', NULL, NULL, '+44-555-0200', FALSE, 3.20, 2010, '2020-03-10 12:00:00'),
(4, 'Quality Goods', 'Japan', 'sales@quality.jp', '+81-555-0300', TRUE, 4.95, 1985, '2020-04-05 13:00:00'),
(5, 'StartupCo', 'USA', 'hello@startup.com', NULL, FALSE, NULL, 2020, '2021-01-01 14:00:00');

-- Products
INSERT INTO products (id, sku, name, description, category, price, cost, stock_quantity, weight_kg, is_active, is_featured, manufacturer_id, created_at, updated_at, discontinued_at) VALUES
(1, 'WIDGET-001', 'Blue Widget', 'A standard blue widget', 'Widgets', 29.99, 15.00, 100, 0.5, TRUE, FALSE, 1, '2023-01-10 10:00:00', '2023-06-15 10:00:00', NULL),
(2, 'WIDGET-002', 'Red Widget', 'A premium red widget', 'Widgets', 49.99, 25.00, 50, 0.6, TRUE, TRUE, 1, '2023-01-15 10:00:00', '2023-07-20 10:00:00', NULL),
(3, 'GADGET-001', 'Smart Gadget', 'An intelligent gadget with AI', 'Electronics', 299.99, 150.00, 25, 1.2, TRUE, TRUE, 2, '2023-02-01 10:00:00', NULL, NULL),
(4, 'GADGET-002', 'Basic Gadget', NULL, 'Electronics', 99.99, 50.00, 0, 0.8, TRUE, FALSE, 2, '2023-02-10 10:00:00', '2023-08-01 10:00:00', NULL),
(5, 'TOOL-001', 'Power Tool', 'Heavy duty power tool', 'Tools', 199.99, 100.00, 15, 3.5, TRUE, FALSE, 3, '2023-03-01 10:00:00', NULL, NULL),
(6, 'TOOL-002', 'Manual Tool', 'Traditional manual tool', 'Tools', 39.99, 20.00, 200, 0.9, TRUE, FALSE, 4, '2023-03-05 10:00:00', '2023-09-01 10:00:00', NULL),
(7, 'LEGACY-001', 'Old Product', 'Discontinued legacy product', NULL, 9.99, 5.00, 5, NULL, FALSE, FALSE, 1, '2020-01-01 10:00:00', '2023-10-01 10:00:00', '2023-10-01 10:00:00'),
(8, 'PREMIUM-001', 'Luxury Item', 'Expensive premium product', 'Luxury', 999.99, 500.00, 3, 2.0, TRUE, TRUE, 4, '2023-05-01 10:00:00', NULL, NULL),
(9, 'BUDGET-001', 'Economy Item', 'Budget-friendly option', 'Budget', 14.99, 8.00, 500, 0.3, TRUE, FALSE, 5, '2023-06-01 10:00:00', NULL, NULL),
(10, 'SPECIAL-001', 'Limited Edition', 'Rare limited edition', 'Collectibles', 599.99, NULL, 1, 1.5, TRUE, TRUE, 2, '2023-07-01 10:00:00', '2023-11-01 10:00:00', NULL);

-- Orders
INSERT INTO orders (id, order_number, customer_email, product_id, quantity, unit_price, total_price, status, notes, shipped_at, delivered_at, created_at) VALUES
(1, 'ORD-2023-0001', 'alice@example.com', 1, 2, 29.99, 59.98, 'delivered', 'First order', '2023-01-11 10:00:00', '2023-01-13 10:00:00', '2023-01-10 10:00:00'),
(2, 'ORD-2023-0002', 'bob@example.com', 3, 1, 299.99, 299.99, 'delivered', NULL, '2023-02-02 10:00:00', '2023-02-05 10:00:00', '2023-02-01 10:00:00'),
(3, 'ORD-2023-0003', 'alice@example.com', 2, 1, 49.99, 49.99, 'shipped', 'Express shipping', '2023-03-02 10:00:00', NULL, '2023-03-01 10:00:00'),
(4, 'ORD-2023-0004', 'charlie@example.com', 5, 1, 199.99, 199.99, 'pending', NULL, NULL, NULL, '2023-03-10 10:00:00'),
(5, 'ORD-2023-0005', 'bob@example.com', 8, 1, 999.99, 999.99, 'cancelled', 'Customer requested cancellation', NULL, NULL, '2023-05-15 10:00:00'),
(6, 'ORD-2023-0006', 'alice@example.com', 6, 5, 39.99, 199.95, 'delivered', 'Bulk order', '2023-06-02 10:00:00', '2023-06-04 10:00:00', '2023-06-01 10:00:00'),
(7, 'ORD-2023-0007', 'david@example.com', 9, 10, 14.99, 149.90, 'shipped', NULL, '2023-07-02 10:00:00', NULL, '2023-07-01 10:00:00'),
(8, 'ORD-2023-0008', 'alice@example.com', 10, 1, 599.99, 599.99, 'delivered', 'Gift wrapped', '2023-08-02 10:00:00', '2023-08-05 10:00:00', '2023-08-01 10:00:00');

-- Categories
INSERT INTO categories (id, slug, name, parent_id, display_order, is_visible, icon_url, created_at) VALUES
(1, 'electronics', 'Electronics', NULL, 1, TRUE, 'https://example.com/icons/electronics.png', '2023-01-01 10:00:00'),
(2, 'computers', 'Computers', 1, 1, TRUE, 'https://example.com/icons/computers.png', '2023-01-01 10:00:00'),
(3, 'laptops', 'Laptops', 2, 1, TRUE, NULL, '2023-01-01 10:00:00'),
(4, 'desktops', 'Desktops', 2, 2, TRUE, NULL, '2023-01-01 10:00:00'),
(5, 'phones', 'Phones', 1, 2, TRUE, 'https://example.com/icons/phones.png', '2023-01-01 10:00:00'),
(6, 'accessories', 'Accessories', NULL, 2, TRUE, NULL, '2023-01-01 10:00:00'),
(7, 'hidden-category', 'Hidden Category', NULL, 99, FALSE, NULL, '2023-01-01 10:00:00');