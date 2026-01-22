-- Composite primary key: two columns
CREATE TABLE order_items (
    order_id INT NOT NULL,
    product_id INT NOT NULL,
    quantity INT NOT NULL,
    unit_price DECIMAL(10,2) NOT NULL,
    PRIMARY KEY (order_id, product_id)
);

-- Composite primary key: three columns
CREATE TABLE inventory_locations (
    warehouse_id INT NOT NULL,
    aisle VARCHAR(10) NOT NULL,
    shelf INT NOT NULL,
    product_id INT,
    quantity INT DEFAULT 0,
    PRIMARY KEY (warehouse_id, aisle, shelf)
);

-- Single column primary key for comparison
CREATE TABLE warehouses (
    id INT PRIMARY KEY AUTO_INCREMENT,
    name VARCHAR(100) NOT NULL,
    location VARCHAR(200)
);
