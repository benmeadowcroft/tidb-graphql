INSERT INTO uuid_records (uuid_bin, uuid_text, label) VALUES
  (UNHEX(REPLACE('550e8400-e29b-41d4-a716-446655440000', '-', '')), '550e8400-e29b-41d4-a716-446655440000', 'alpha'),
  (UNHEX(REPLACE('123e4567-e89b-12d3-a456-426614174000', '-', '')), '123e4567-e89b-12d3-a456-426614174000', 'beta');
