DROP TABLE IF EXISTS files;

CREATE TABLE files (
  id BIGINT PRIMARY KEY AUTO_INCREMENT,
  name VARCHAR(64) NOT NULL,
  payload BLOB NULL,
  hash VARBINARY(16) NULL,
  UNIQUE KEY uq_files_name (name),
  KEY idx_files_payload (payload(255)),
  KEY idx_files_hash (hash)
);
