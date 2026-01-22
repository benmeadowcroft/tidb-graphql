-- Seed data for generated columns tests

INSERT INTO person (name, address_info) VALUES
    ('Alice', '{"city": "New York", "country": "USA", "zip": "10001"}'),
    ('Bob', '{"city": "Los Angeles", "country": "USA", "zip": "90001"}'),
    ('Charlie', '{"city": "London", "country": "UK", "zip": "SW1A"}'),
    ('Diana', '{"city": "Paris", "country": "France", "zip": "75001"}');

INSERT INTO products_computed (name, price, quantity) VALUES
    ('Widget A', 10.00, 100),
    ('Widget B', 25.50, 50),
    ('Gadget C', 99.99, 10),
    ('Gadget D', 5.00, 500);
