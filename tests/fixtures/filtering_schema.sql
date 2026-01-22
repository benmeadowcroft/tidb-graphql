-- Comprehensive test schema for filtering and unique key lookups
-- Covers: various data types, nullable columns, unique indexes, composite unique keys

CREATE TABLE products (
    id INT PRIMARY KEY AUTO_INCREMENT,
    sku VARCHAR(50) NOT NULL UNIQUE,
    name VARCHAR(100) NOT NULL,
    description TEXT,
    category VARCHAR(50),  -- nullable, indexed
    price DECIMAL(10,2) NOT NULL,
    cost DECIMAL(10,2),  -- nullable
    stock_quantity INT NOT NULL DEFAULT 0,
    weight_kg FLOAT,  -- nullable
    is_active BOOLEAN DEFAULT TRUE,
    is_featured BOOLEAN DEFAULT FALSE,
    manufacturer_id INT NOT NULL,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME,  -- nullable
    discontinued_at DATETIME,  -- nullable

    -- Unique constraints
    UNIQUE KEY uk_manufacturer_sku (manufacturer_id, sku),

    -- Regular indexes for testing indexed column requirement
    INDEX idx_category (category),
    INDEX idx_price (price),
    INDEX idx_stock (stock_quantity),
    INDEX idx_created_at (created_at),
    INDEX idx_manufacturer (manufacturer_id),
    INDEX idx_active_category (is_active, category)
);

CREATE TABLE manufacturers (
    id INT PRIMARY KEY AUTO_INCREMENT,
    name VARCHAR(100) NOT NULL UNIQUE,
    country VARCHAR(50),  -- nullable
    email VARCHAR(255) UNIQUE,  -- nullable, unique
    phone VARCHAR(20),  -- nullable
    is_verified BOOLEAN DEFAULT FALSE,
    rating DECIMAL(3,2),  -- nullable, 0.00-5.00
    founded_year INT,  -- nullable
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,

    INDEX idx_country (country),
    INDEX idx_verified (is_verified)
);

CREATE TABLE orders (
    id INT PRIMARY KEY AUTO_INCREMENT,
    order_number VARCHAR(50) NOT NULL UNIQUE,
    customer_email VARCHAR(255) NOT NULL,
    product_id INT NOT NULL,
    quantity INT NOT NULL,
    unit_price DECIMAL(10,2) NOT NULL,
    total_price DECIMAL(10,2) NOT NULL,
    status VARCHAR(20) NOT NULL,  -- pending, shipped, delivered, cancelled
    notes TEXT,  -- nullable
    shipped_at DATETIME,  -- nullable
    delivered_at DATETIME,  -- nullable
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,

    -- Composite unique key
    UNIQUE KEY uk_customer_product_date (customer_email, product_id, created_at),

    FOREIGN KEY (product_id) REFERENCES products(id),

    INDEX idx_customer (customer_email),
    INDEX idx_product (product_id),
    INDEX idx_status (status),
    INDEX idx_created_at (created_at),
    INDEX idx_shipped_at (shipped_at)
);

CREATE TABLE categories (
    id INT PRIMARY KEY AUTO_INCREMENT,
    slug VARCHAR(50) NOT NULL UNIQUE,
    name VARCHAR(100) NOT NULL,
    parent_id INT,  -- nullable, for hierarchical categories
    display_order INT NOT NULL DEFAULT 0,
    is_visible BOOLEAN DEFAULT TRUE,
    icon_url VARCHAR(255),  -- nullable
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,

    FOREIGN KEY (parent_id) REFERENCES categories(id) ON DELETE SET NULL,

    INDEX idx_parent (parent_id),
    INDEX idx_visible_order (is_visible, display_order)
);