-- Vector auto-embedding sample schema and data for tutorials.
-- Intended to be loaded AFTER docs/tutorials/sample-data.sql.
--
-- This script targets TiDB Cloud Zero auto-embedding workflows where embeddings
-- are generated in-database using EMBED_TEXT()

CREATE DATABASE IF NOT EXISTS tidb_graphql_tutorial;
USE tidb_graphql_tutorial;

DROP TABLE IF EXISTS product_reviews;

CREATE TABLE product_reviews (
  id BIGINT PRIMARY KEY AUTO_RANDOM COMMENT 'Primary key for product reviews.',
  product_id BIGINT NOT NULL COMMENT 'Reviewed product identifier.',
  user_id BIGINT NOT NULL COMMENT 'Review author identifier.',
  rating ENUM('thumbs_up','thumbs_down') NOT NULL COMMENT 'Simple sentiment label for the review.',
  review_text TEXT NOT NULL COMMENT 'Free-form review content.',
  embedding VECTOR(1024) GENERATED ALWAYS AS (
    EMBED_TEXT("tidbcloud_free/amazon/titan-embed-text-v2", review_text, '{"dimensions":1024}')
  ) STORED COMMENT 'Auto-generated embedding for semantic search demos.',
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
  KEY idx_product_reviews_created_at (created_at),
  VECTOR INDEX idx_product_reviews_embedding_cosine ((VEC_COSINE_DISTANCE(embedding))) USING HNSW
) COMMENT='User-submitted product reviews with vector embeddings.';

CREATE TEMPORARY TABLE product_review_seeds (
  user_email VARCHAR(255) NOT NULL,
  sku VARCHAR(64) NOT NULL,
  rating VARCHAR(16) NOT NULL,
  review_text TEXT NOT NULL,
  PRIMARY KEY (user_email, sku)
);

INSERT INTO product_review_seeds (user_email, sku, rating, review_text) VALUES
  ('ava.smith@example.com',      'SKU-1011', 'thumbs_up',   'Charged my phone twice on a weekend trip and stayed cool.'),
  ('ben.johnson@example.com',    'SKU-1011', 'thumbs_up',   'Solid charge speed and small enough for daily carry.'),
  ('chloe.williams@example.com', 'SKU-1010', 'thumbs_up',   'Heat retention is good and smart temp indicator is useful.'),
  ('drew.smith@example.com',     'SKU-1005', 'thumbs_up',   'Great seal and insulation. No leaks in my backpack.'),

  ('ava.brown@example.com',      'SKU-1006', 'thumbs_up',   'Keys are tactile and typing fatigue is much lower now.'),
  ('ben.jackson@example.com',    'SKU-1003', 'thumbs_up',   'Good brightness control and stable base for my workspace.'),
  ('chloe.garcia@example.com',   'SKU-1013', 'thumbs_up',   'Text is crisp and colors look accurate for office tasks.'),
  ('drew.davis@example.com',     'SKU-1019', 'thumbs_down', 'Gets the job done for notes, but marker residue builds up.'),

  ('ava.jones@example.com',      'SKU-1007', 'thumbs_up',   'Blocks office chatter and calls are clear.'),
  ('ben.garcia@example.com',     'SKU-1018', 'thumbs_up',   'Great for podcasts and casual music listening.'),
  ('chloe.miller@example.com',   'SKU-1004', 'thumbs_up',   'Pointer is accurate and battery life is better than expected.'),
  ('drew.martinez@example.com',  'SKU-1007', 'thumbs_down', 'Audio is good but clamping force is a little high for long sessions.'),

  ('ava.lopez@example.com',      'SKU-1015', 'thumbs_up',   'Yoga mat feels stable and thickness is ideal for floor work.'),
  ('ben.moore@example.com',      'SKU-1017', 'thumbs_up',   'Fits daily essentials comfortably with solid straps.'),
  ('chloe.anderson@example.com', 'SKU-1020', 'thumbs_up',   'Pages hold up well in light rain and rough handling.'),
  ('drew.hernandez@example.com', 'SKU-1008', 'thumbs_up',   'Desk mat reduced foot fatigue over long workdays.'),

  ('eli.smith@example.com',      'SKU-1011', 'thumbs_up',   'Great backup battery for travel and conference days.'),
  ('eli.johnson@example.com',    'SKU-1010', 'thumbs_up',   'Temperature indicator is simple and surprisingly practical.'),
  ('eli.williams@example.com',   'SKU-1005', 'thumbs_up',   'Stays cold all day and cap seal has been reliable.'),
  ('eli.brown@example.com',      'SKU-1006', 'thumbs_up',   'Comfortable for long typing sessions and feels very consistent.'),

  ('eli.jones@example.com',      'SKU-1003', 'thumbs_up',   'Lamp arm is sturdy and dimmer settings cover my needs.'),
  ('eli.garcia@example.com',     'SKU-1013', 'thumbs_up',   'Nice brightness and color profile for long office use.'),
  ('eli.miller@example.com',     'SKU-1019', 'thumbs_down', 'The board is usable but cleaning takes more effort than expected.'),
  ('eli.davis@example.com',      'SKU-1007', 'thumbs_down', 'Sound is solid, but fit becomes uncomfortable after an hour.'),

  ('eli.rodriguez@example.com',  'SKU-1018', 'thumbs_up',   'Speaker is compact and audio quality is better than expected.'),
  ('eli.martinez@example.com',   'SKU-1004', 'thumbs_up',   'Mouse tracks smoothly and the shape feels natural in hand.'),
  ('eli.hernandez@example.com',  'SKU-1015', 'thumbs_up',   'Good mat thickness and grip for mixed yoga sessions.'),
  ('eli.lopez@example.com',      'SKU-1017', 'thumbs_up',   'Backpack balances weight well and has useful compartments.'),

  ('eli.gonzalez@example.com',   'SKU-1020', 'thumbs_up',   'Notebook paper stays intact in damp conditions.'),
  ('eli.wilson@example.com',     'SKU-1008', 'thumbs_up',   'Standing support helped reduce fatigue over the week.'),
  ('eli.anderson@example.com',   'SKU-1002', 'thumbs_down', 'Looks good, but handle comfort could be better for daily use.'),
  ('eli.thomas@example.com',     'SKU-1009', 'thumbs_down', 'Pens skip occasionally and ink consistency is not great.'),

  ('ava.williams@example.com',   'SKU-1011', 'thumbs_up',   'Reliable charger for long commutes and flights.'),
  ('ava.garcia@example.com',     'SKU-1010', 'thumbs_up',   'Smart thermos keeps coffee hot all morning.'),
  ('ava.miller@example.com',     'SKU-1005', 'thumbs_up',   'Excellent insulation and easy-to-clean interior.'),
  ('ava.davis@example.com',      'SKU-1006', 'thumbs_up',   'Keyboard feel is crisp and responsive for coding.'),

  ('ava.rodriguez@example.com',  'SKU-1003', 'thumbs_up',   'Lamp is bright enough without glare at night.'),
  ('ava.martinez@example.com',   'SKU-1013', 'thumbs_up',   'Monitor clarity is great for spreadsheets and docs.'),
  ('ava.hernandez@example.com',  'SKU-1019', 'thumbs_down', 'Board works, but marker ghosting appears too quickly.'),
  ('ava.taylor@example.com',     'SKU-1007', 'thumbs_down', 'Noise canceling is fine, but ear pressure is uncomfortable.'),

  ('ava.moore@example.com',      'SKU-1018', 'thumbs_up',   'Speaker has clear mids and enough volume for meetings.'),
  ('ava.jackson@example.com',    'SKU-1004', 'thumbs_up',   'Mouse glides well and clicks feel precise.'),
  ('ava.martin@example.com',     'SKU-1015', 'thumbs_up',   'Mat surface is stable and comfortable during floor routines.'),
  ('ben.smith@example.com',      'SKU-1017', 'thumbs_up',   'Backpack build quality is solid and zippers feel durable.'),

  ('ben.williams@example.com',   'SKU-1020', 'thumbs_up',   'Water-resistant notebook held up well on a rainy trip.'),
  ('ben.brown@example.com',      'SKU-1008', 'thumbs_up',   'Desk mat support is noticeable during long standing sessions.'),
  ('ben.jones@example.com',      'SKU-1002', 'thumbs_down', 'Mug finish is good but heat transfer to handle is noticeable.'),
  ('ben.davis@example.com',      'SKU-1009', 'thumbs_down', 'Pen tips are inconsistent and a couple dried quickly.');

INSERT INTO product_reviews (
  product_id,
  user_id,
  rating,
  review_text,
  last_updated
)
SELECT
  p.id,
  u.id,
  s.rating,
  s.review_text,
  CURRENT_TIMESTAMP
FROM product_review_seeds s
JOIN users u ON u.email = s.user_email
JOIN products p ON p.sku = s.sku;

DROP TEMPORARY TABLE product_review_seeds;

-- Example SQL nearest-neighbor query:
-- SELECT pr.id, p.sku, pr.rating, pr.review_text
-- FROM product_reviews pr
-- JOIN products p ON p.id = pr.product_id
-- ORDER BY VEC_EMBED_COSINE_DISTANCE(pr.embedding, 'great battery life for travel days') ASC
-- LIMIT 5;
