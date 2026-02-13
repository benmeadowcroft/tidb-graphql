CREATE TABLE products (
  id INT PRIMARY KEY AUTO_INCREMENT,
  name VARCHAR(255) NOT NULL,
  price DECIMAL(10,2) NOT NULL,
  tags SET('featured','new','clearance','seasonal','limited') NOT NULL DEFAULT '',
  KEY idx_products_name (name),
  KEY idx_products_tags (tags),
  KEY idx_products_price (price)
);
