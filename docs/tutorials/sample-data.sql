-- Sample schema and data for tutorials.
-- Designed for filters, sorting, and unique lookups.

CREATE DATABASE IF NOT EXISTS tidb_graphql_tutorial;
USE tidb_graphql_tutorial;

DROP TABLE IF EXISTS order_items;
DROP TABLE IF EXISTS orders;
DROP TABLE IF EXISTS products;
DROP TABLE IF EXISTS users;

CREATE TABLE users (
  id BIGINT PRIMARY KEY AUTO_INCREMENT,
  email VARCHAR(255) NOT NULL,
  full_name VARCHAR(255) NOT NULL,
  status VARCHAR(32) NOT NULL,
  created_at DATETIME NOT NULL,
  UNIQUE KEY uq_users_email (email),
  KEY idx_users_status (status),
  KEY idx_users_created_at (created_at)
);

CREATE TABLE products (
  id BIGINT PRIMARY KEY AUTO_INCREMENT,
  sku VARCHAR(64) NOT NULL,
  name VARCHAR(255) NOT NULL,
  category VARCHAR(64) NOT NULL,
  price_cents INT NOT NULL,
  created_at DATETIME NOT NULL,
  UNIQUE KEY uq_products_sku (sku),
  KEY idx_products_category (category),
  KEY idx_products_price (price_cents)
);

CREATE TABLE orders (
  id BIGINT PRIMARY KEY AUTO_INCREMENT,
  user_id BIGINT NOT NULL,
  status VARCHAR(32) NOT NULL,
  total_cents INT NOT NULL,
  created_at DATETIME NOT NULL,
  CONSTRAINT fk_orders_user
    FOREIGN KEY (user_id) REFERENCES users(id),
  KEY idx_orders_user_id (user_id),
  KEY idx_orders_status (status),
  KEY idx_orders_created_at (created_at)
);

CREATE TABLE order_items (
  id BIGINT PRIMARY KEY AUTO_INCREMENT,
  order_id BIGINT NOT NULL,
  product_id BIGINT NOT NULL,
  quantity INT NOT NULL,
  unit_price_cents INT NOT NULL,
  CONSTRAINT fk_order_items_order
    FOREIGN KEY (order_id) REFERENCES orders(id),
  CONSTRAINT fk_order_items_product
    FOREIGN KEY (product_id) REFERENCES products(id),
  KEY idx_items_order_id (order_id),
  KEY idx_items_product_id (product_id)
);

INSERT INTO users (email, full_name, status, created_at) VALUES
  ('ava.lee@example.com', 'Ava Lee', 'active', '2024-01-05 09:15:00'),
  ('ben.turner@example.com', 'Ben Turner', 'active', '2024-01-12 13:30:00'),
  ('chloe.park@example.com', 'Chloe Park', 'inactive', '2024-02-02 08:45:00'),
  ('drew.ng@example.com', 'Drew Ng', 'active', '2024-02-11 17:20:00'),
  ('eli.jones@example.com', 'Eli Jones', 'pending', '2024-02-20 10:10:00');

INSERT INTO products (sku, name, category, price_cents, created_at) VALUES
  ('SKU-1001', 'Graphite Notebook', 'stationery', 1200, '2024-01-03 08:00:00'),
  ('SKU-1002', 'Ceramic Mug', 'kitchen', 1800, '2024-01-10 09:30:00'),
  ('SKU-1003', 'Desk Lamp', 'office', 4900, '2024-01-18 11:45:00'),
  ('SKU-1004', 'Wireless Mouse', 'office', 3500, '2024-02-01 14:15:00'),
  ('SKU-1005', 'Insulated Bottle', 'kitchen', 2600, '2024-02-12 16:40:00');

INSERT INTO orders (user_id, status, total_cents, created_at) VALUES
  (1, 'paid', 3000, '2024-02-15 12:05:00'),
  (1, 'shipped', 6100, '2024-02-18 09:25:00'),
  (2, 'paid', 1800, '2024-02-21 15:10:00'),
  (3, 'canceled', 1200, '2024-02-22 11:00:00'),
  (4, 'paid', 6100, '2024-02-23 18:45:00');

INSERT INTO order_items (order_id, product_id, quantity, unit_price_cents) VALUES
  (1, 1, 1, 1200),
  (1, 2, 1, 1800),
  (2, 3, 1, 4900),
  (2, 1, 1, 1200),
  (3, 2, 1, 1800),
  (4, 1, 1, 1200),
  (5, 3, 1, 4900),
  (5, 4, 1, 1200);
