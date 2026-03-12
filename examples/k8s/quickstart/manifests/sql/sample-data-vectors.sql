-- Vector add-on sample schema and data for tutorials.
-- Intended to be loaded AFTER docs/tutorials/sample-data.sql.
--
-- This script focuses on self-managed or generic TiDB setups where embeddings
-- are precomputed outside the database and stored directly in a VECTOR column.

CREATE DATABASE IF NOT EXISTS tidb_graphql_tutorial;
USE tidb_graphql_tutorial;

DROP TABLE IF EXISTS product_reviews;

CREATE TABLE product_reviews (
  id BIGINT PRIMARY KEY AUTO_RANDOM COMMENT 'Primary key for product reviews.',
  product_id BIGINT NOT NULL COMMENT 'Reviewed product identifier.',
  user_id BIGINT NOT NULL COMMENT 'Review author identifier.',
  rating ENUM('thumbs_up','thumbs_down') NOT NULL COMMENT 'Simple sentiment label for the review.',
  review_text TEXT NOT NULL COMMENT 'Free-form review content.',
  embedding VECTOR(8) NOT NULL COMMENT 'Precomputed embedding for semantic search demos.',
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'Row creation time.',
  last_updated TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Row update time (UTC).',
  CONSTRAINT fk_product_reviews_product
    FOREIGN KEY (product_id) REFERENCES products(id),
  CONSTRAINT fk_product_reviews_user
    FOREIGN KEY (user_id) REFERENCES users(id),
  UNIQUE KEY uq_product_reviews_product_user (product_id, user_id),
  KEY idx_product_reviews_product (product_id),
  KEY idx_product_reviews_user (user_id),
  KEY idx_product_reviews_rating (rating),
  KEY idx_product_reviews_created_at (created_at)
) COMMENT='User-submitted product reviews with vector embeddings.';

CREATE TEMPORARY TABLE product_review_seeds (
  user_email VARCHAR(255) NOT NULL,
  sku VARCHAR(64) NOT NULL,
  rating VARCHAR(16) NOT NULL,
  review_text TEXT NOT NULL,
  embedding VARCHAR(255) NOT NULL,
  PRIMARY KEY (user_email, sku)
);

INSERT INTO product_review_seeds (user_email, sku, rating, review_text, embedding) VALUES
  ('ava.smith@example.com',      'SKU-1011', 'thumbs_up',   'Charged my phone twice on a weekend trip and stayed cool.',            '[0.92,0.10,0.08,0.06,0.86,0.04,0.03,0.02]'),
  ('ben.johnson@example.com',    'SKU-1011', 'thumbs_up',   'Solid charge speed and small enough for daily carry.',                 '[0.90,0.09,0.06,0.07,0.83,0.05,0.02,0.03]'),
  ('chloe.williams@example.com', 'SKU-1010', 'thumbs_up',   'Heat retention is good and smart temp indicator is useful.',           '[0.88,0.11,0.05,0.05,0.79,0.07,0.03,0.04]'),
  ('drew.smith@example.com',     'SKU-1005', 'thumbs_up',   'Great seal and insulation. No leaks in my backpack.',                  '[0.87,0.12,0.04,0.05,0.76,0.08,0.05,0.03]'),

  ('ava.brown@example.com',      'SKU-1006', 'thumbs_up',   'Keys are tactile and typing fatigue is much lower now.',               '[0.12,0.91,0.09,0.88,0.06,0.05,0.03,0.07]'),
  ('ben.jackson@example.com',    'SKU-1003', 'thumbs_up',   'Good brightness control and stable base for my workspace.',            '[0.10,0.89,0.08,0.85,0.05,0.06,0.04,0.08]'),
  ('chloe.garcia@example.com',   'SKU-1013', 'thumbs_up',   'Text is crisp and colors look accurate for office tasks.',             '[0.11,0.93,0.12,0.82,0.04,0.05,0.03,0.06]'),
  ('drew.davis@example.com',     'SKU-1019', 'thumbs_down', 'Gets the job done for notes, but marker residue builds up.',           '[0.09,0.86,0.07,0.79,0.06,0.06,0.05,0.09]'),

  ('ava.jones@example.com',      'SKU-1007', 'thumbs_up',   'Blocks office chatter and calls are clear.',                           '[0.08,0.84,0.93,0.10,0.05,0.05,0.04,0.06]'),
  ('ben.garcia@example.com',     'SKU-1018', 'thumbs_up',   'Great for podcasts and casual music listening.',                       '[0.07,0.80,0.89,0.12,0.04,0.07,0.06,0.05]'),
  ('chloe.miller@example.com',   'SKU-1004', 'thumbs_up',   'Pointer is accurate and battery life is better than expected.',        '[0.10,0.82,0.86,0.14,0.05,0.08,0.05,0.06]'),
  ('drew.martinez@example.com',  'SKU-1007', 'thumbs_down', 'Audio is good but clamping force is a little high for long sessions.', '[0.09,0.78,0.84,0.11,0.04,0.06,0.05,0.05]'),

  ('ava.lopez@example.com',      'SKU-1015', 'thumbs_up',   'Yoga mat feels stable and thickness is ideal for floor work.',         '[0.14,0.18,0.06,0.09,0.21,0.90,0.86,0.08]'),
  ('ben.moore@example.com',      'SKU-1017', 'thumbs_up',   'Fits daily essentials comfortably with solid straps.',                  '[0.16,0.17,0.05,0.08,0.24,0.86,0.81,0.10]'),
  ('chloe.anderson@example.com', 'SKU-1020', 'thumbs_up',   'Pages hold up well in light rain and rough handling.',                 '[0.13,0.15,0.04,0.07,0.19,0.88,0.84,0.09]'),
  ('drew.hernandez@example.com', 'SKU-1008', 'thumbs_up',   'Desk mat reduced foot fatigue over long workdays.',                    '[0.15,0.20,0.05,0.10,0.20,0.85,0.80,0.11]'),

  ('eli.smith@example.com',      'SKU-1011', 'thumbs_up',   'Great backup battery for travel and conference days.',                 '[0.91,0.10,0.08,0.07,0.85,0.05,0.03,0.02]'),
  ('eli.johnson@example.com',    'SKU-1010', 'thumbs_up',   'Temperature indicator is simple and surprisingly practical.',           '[0.87,0.12,0.06,0.05,0.78,0.08,0.03,0.04]'),
  ('eli.williams@example.com',   'SKU-1005', 'thumbs_up',   'Stays cold all day and cap seal has been reliable.',                   '[0.86,0.13,0.05,0.05,0.74,0.08,0.06,0.03]'),
  ('eli.brown@example.com',      'SKU-1006', 'thumbs_up',   'Comfortable for long typing sessions and feels very consistent.',      '[0.13,0.90,0.08,0.86,0.06,0.05,0.03,0.07]'),

  ('eli.jones@example.com',      'SKU-1003', 'thumbs_up',   'Lamp arm is sturdy and dimmer settings cover my needs.',              '[0.11,0.88,0.09,0.84,0.05,0.06,0.04,0.08]'),
  ('eli.garcia@example.com',     'SKU-1013', 'thumbs_up',   'Nice brightness and color profile for long office use.',               '[0.12,0.92,0.11,0.81,0.04,0.05,0.03,0.06]'),
  ('eli.miller@example.com',     'SKU-1019', 'thumbs_down', 'The board is usable but cleaning takes more effort than expected.',    '[0.09,0.85,0.08,0.78,0.06,0.06,0.05,0.09]'),
  ('eli.davis@example.com',      'SKU-1007', 'thumbs_down', 'Sound is solid, but fit becomes uncomfortable after an hour.',         '[0.09,0.77,0.83,0.12,0.04,0.06,0.05,0.05]'),

  ('eli.rodriguez@example.com',  'SKU-1018', 'thumbs_up',   'Speaker is compact and audio quality is better than expected.',        '[0.08,0.81,0.88,0.13,0.04,0.07,0.06,0.05]'),
  ('eli.martinez@example.com',   'SKU-1004', 'thumbs_up',   'Mouse tracks smoothly and the shape feels natural in hand.',          '[0.11,0.83,0.85,0.15,0.05,0.08,0.05,0.06]'),
  ('eli.hernandez@example.com',  'SKU-1015', 'thumbs_up',   'Good mat thickness and grip for mixed yoga sessions.',                 '[0.15,0.19,0.06,0.09,0.21,0.89,0.85,0.09]'),
  ('eli.lopez@example.com',      'SKU-1017', 'thumbs_up',   'Backpack balances weight well and has useful compartments.',           '[0.17,0.17,0.05,0.09,0.24,0.85,0.80,0.10]'),

  ('eli.gonzalez@example.com',   'SKU-1020', 'thumbs_up',   'Notebook paper stays intact in damp conditions.',                      '[0.14,0.15,0.04,0.08,0.20,0.87,0.83,0.09]'),
  ('eli.wilson@example.com',     'SKU-1008', 'thumbs_up',   'Standing support helped reduce fatigue over the week.',                '[0.15,0.21,0.05,0.11,0.21,0.84,0.79,0.11]'),
  ('eli.anderson@example.com',   'SKU-1002', 'thumbs_down', 'Looks good, but handle comfort could be better for daily use.',        '[0.76,0.12,0.05,0.06,0.70,0.06,0.03,0.03]'),
  ('eli.thomas@example.com',     'SKU-1009', 'thumbs_down', 'Pens skip occasionally and ink consistency is not great.',             '[0.72,0.11,0.06,0.07,0.68,0.07,0.04,0.03]'),

  ('ava.williams@example.com',   'SKU-1011', 'thumbs_up',   'Reliable charger for long commutes and flights.',                      '[0.91,0.09,0.08,0.07,0.84,0.05,0.03,0.02]'),
  ('ava.garcia@example.com',     'SKU-1010', 'thumbs_up',   'Smart thermos keeps coffee hot all morning.',                          '[0.87,0.11,0.06,0.05,0.77,0.08,0.03,0.04]'),
  ('ava.miller@example.com',     'SKU-1005', 'thumbs_up',   'Excellent insulation and easy-to-clean interior.',                     '[0.86,0.12,0.05,0.05,0.75,0.08,0.05,0.03]'),
  ('ava.davis@example.com',      'SKU-1006', 'thumbs_up',   'Keyboard feel is crisp and responsive for coding.',                    '[0.13,0.89,0.09,0.87,0.06,0.05,0.03,0.07]'),

  ('ava.rodriguez@example.com',  'SKU-1003', 'thumbs_up',   'Lamp is bright enough without glare at night.',                        '[0.11,0.87,0.09,0.83,0.05,0.06,0.04,0.08]'),
  ('ava.martinez@example.com',   'SKU-1013', 'thumbs_up',   'Monitor clarity is great for spreadsheets and docs.',                  '[0.12,0.91,0.12,0.82,0.04,0.05,0.03,0.06]'),
  ('ava.hernandez@example.com',  'SKU-1019', 'thumbs_down', 'Board works, but marker ghosting appears too quickly.',                '[0.10,0.84,0.08,0.78,0.06,0.06,0.05,0.09]'),
  ('ava.taylor@example.com',     'SKU-1007', 'thumbs_down', 'Noise canceling is fine, but ear pressure is uncomfortable.',          '[0.09,0.76,0.82,0.12,0.04,0.06,0.05,0.05]'),

  ('ava.moore@example.com',      'SKU-1018', 'thumbs_up',   'Speaker has clear mids and enough volume for meetings.',               '[0.08,0.80,0.87,0.13,0.04,0.07,0.06,0.05]'),
  ('ava.jackson@example.com',    'SKU-1004', 'thumbs_up',   'Mouse glides well and clicks feel precise.',                           '[0.11,0.82,0.84,0.15,0.05,0.08,0.05,0.06]'),
  ('ava.martin@example.com',     'SKU-1015', 'thumbs_up',   'Mat surface is stable and comfortable during floor routines.',         '[0.15,0.19,0.06,0.09,0.22,0.88,0.84,0.09]'),
  ('ben.smith@example.com',      'SKU-1017', 'thumbs_up',   'Backpack build quality is solid and zippers feel durable.',            '[0.17,0.18,0.05,0.09,0.23,0.85,0.79,0.10]'),

  ('ben.williams@example.com',   'SKU-1020', 'thumbs_up',   'Water-resistant notebook held up well on a rainy trip.',              '[0.14,0.16,0.04,0.08,0.19,0.87,0.82,0.09]'),
  ('ben.brown@example.com',      'SKU-1008', 'thumbs_up',   'Desk mat support is noticeable during long standing sessions.',        '[0.16,0.21,0.05,0.10,0.20,0.84,0.78,0.11]'),
  ('ben.jones@example.com',      'SKU-1002', 'thumbs_down', 'Mug finish is good but heat transfer to handle is noticeable.',        '[0.75,0.13,0.05,0.06,0.69,0.06,0.03,0.03]'),
  ('ben.davis@example.com',      'SKU-1009', 'thumbs_down', 'Pen tips are inconsistent and a couple dried quickly.',                '[0.71,0.12,0.06,0.07,0.67,0.07,0.04,0.03]');

INSERT INTO product_reviews (
  product_id,
  user_id,
  rating,
  review_text,
  embedding,
  last_updated
)
SELECT
  p.id,
  u.id,
  s.rating,
  s.review_text,
  s.embedding,
  CURRENT_TIMESTAMP
FROM product_review_seeds s
JOIN users u ON u.email = s.user_email
JOIN products p ON p.sku = s.sku;

DROP TEMPORARY TABLE product_review_seeds;

-- Optional: add a vector index for ANN acceleration.
-- If this fails (for example, TiFlash is not available in self-managed setup),
-- skip this step and continue. Vector search still works without this index.
--
-- ALTER TABLE product_reviews SET TIFLASH REPLICA 1;
-- ALTER TABLE product_reviews
--   ADD VECTOR INDEX idx_product_reviews_embedding_cosine ((VEC_COSINE_DISTANCE(embedding))) USING HNSW;

-- Example SQL nearest-neighbor query:
-- SELECT pr.id, p.sku, pr.rating, pr.review_text
-- FROM product_reviews pr
-- JOIN products p ON p.id = pr.product_id
-- ORDER BY VEC_COSINE_DISTANCE(pr.embedding, '[0.10,0.85,0.90,0.12,0.05,0.07,0.05,0.06]') ASC
-- LIMIT 5;
