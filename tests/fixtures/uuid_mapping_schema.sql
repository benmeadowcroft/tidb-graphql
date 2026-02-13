DROP TABLE IF EXISTS uuid_records;

CREATE TABLE uuid_records (
  id BIGINT PRIMARY KEY AUTO_INCREMENT,
  uuid_bin BINARY(16) NOT NULL,
  uuid_text CHAR(36) NULL,
  label VARCHAR(64) NOT NULL,
  UNIQUE KEY uq_uuid_bin (uuid_bin),
  UNIQUE KEY uq_uuid_text (uuid_text),
  KEY idx_uuid_bin (uuid_bin),
  KEY idx_uuid_text (uuid_text)
);
