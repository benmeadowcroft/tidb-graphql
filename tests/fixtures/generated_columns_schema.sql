-- Test schema for TiDB generated columns (virtual and stored)
-- Tests JSON extraction and computed numeric columns

-- Table with JSON column and generated columns extracting values
CREATE TABLE person (
    id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    address_info JSON,
    -- Virtual generated column (computed on read)
    city VARCHAR(64) AS (JSON_UNQUOTE(JSON_EXTRACT(address_info, '$.city'))) VIRTUAL,
    -- Stored generated column (computed on write)
    country VARCHAR(64) AS (JSON_UNQUOTE(JSON_EXTRACT(address_info, '$.country'))) STORED,
    -- Index on virtual generated column for filtering
    INDEX idx_city (city),
    -- Unique index on stored generated column for direct lookup
    UNIQUE INDEX uk_country_name (country, name)
);

-- Table with computed numeric column
CREATE TABLE products_computed (
    id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    price DECIMAL(10,2) NOT NULL,
    quantity INT NOT NULL,
    -- Stored generated column: computed total value
    total_value DECIMAL(12,2) AS (price * quantity) STORED,
    -- Index for filtering by computed value
    INDEX idx_total_value (total_value)
);
