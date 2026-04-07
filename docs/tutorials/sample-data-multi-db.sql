-- Multi-database shipping sample data for tutorials.
-- Demonstrates cross-database foreign key relationships between two databases.
--
-- Requires docs/tutorials/sample-data.sql to be loaded first so that
-- the tidb_graphql_tutorial database and its orders table exist.
--
-- Creates: tidb_graphql_shipping
--   carriers          — shipping carrier master data
--   shipments         — one shipment per order (FK → tidb_graphql_tutorial.orders)
--   shipment_tracking — individual carrier scan events per shipment

CREATE DATABASE IF NOT EXISTS tidb_graphql_shipping;
USE tidb_graphql_shipping;

-- Reset teardown: temporarily disable FK checks so tables can be dropped in any order.
SET @OLD_FOREIGN_KEY_CHECKS = @@FOREIGN_KEY_CHECKS;
SET FOREIGN_KEY_CHECKS = 0;

DROP TABLE IF EXISTS shipment_tracking;
DROP TABLE IF EXISTS shipments;
DROP TABLE IF EXISTS carriers;

SET FOREIGN_KEY_CHECKS = @OLD_FOREIGN_KEY_CHECKS;

-- ---------------------------------------------------------------------------
-- Schema
-- ---------------------------------------------------------------------------

CREATE TABLE carriers (
  id   BIGINT PRIMARY KEY AUTO_RANDOM COMMENT 'Primary key for carriers.',
  code VARCHAR(16)  NOT NULL COMMENT 'Short carrier code used internally (e.g. UPS, FEDEX).',
  name VARCHAR(128) NOT NULL COMMENT 'Carrier display name.',
  tracking_url_template VARCHAR(512) NULL
    COMMENT 'Tracking URL template; substitute {tracking_number} to build a live link.',
  created_at   TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'Row creation time (UTC).',
  last_updated TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
    ON UPDATE CURRENT_TIMESTAMP COMMENT 'Row update time (UTC).',
  UNIQUE KEY uq_carriers_code (code)
) COMMENT='Shipping carriers used to fulfil store orders.';

CREATE TABLE shipments (
  id               BIGINT PRIMARY KEY AUTO_RANDOM COMMENT 'Primary key for shipments.',
  order_id         BIGINT NOT NULL
    COMMENT 'Order being shipped. References tidb_graphql_tutorial.orders.id.',
  carrier_id       BIGINT NOT NULL COMMENT 'Carrier handling this shipment.',
  tracking_number  VARCHAR(128) NULL COMMENT 'Carrier-assigned tracking number.',
  status           ENUM('pending','in_transit','delivered','returned','exception')
                   NOT NULL DEFAULT 'pending' COMMENT 'Current shipment lifecycle status.',
  estimated_delivery DATE NULL COMMENT 'Carrier-estimated delivery date.',
  shipped_at       TIMESTAMP NULL COMMENT 'When the shipment left the warehouse (UTC).',
  delivered_at     TIMESTAMP NULL COMMENT 'When the shipment was delivered (UTC).',
  created_at       TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'Row creation time (UTC).',
  last_updated     TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
    ON UPDATE CURRENT_TIMESTAMP COMMENT 'Row update time (UTC).',
  UNIQUE KEY uq_shipments_tracking (carrier_id, tracking_number),
  KEY idx_shipments_order_id (order_id),
  KEY idx_shipments_status (status),
  CONSTRAINT fk_shipments_carrier
    FOREIGN KEY (carrier_id) REFERENCES carriers(id),
  -- Cross-database FK: links shipping data back to the tutorial store's orders.
  -- TiDB enforces this as an advisory reference when both databases live on the
  -- same server; the tidb-graphql server surfaces it as a traversable relationship.
  CONSTRAINT fk_shipments_order
    FOREIGN KEY (order_id) REFERENCES tidb_graphql_tutorial.orders(id)
) COMMENT='Shipments fulfilling orders placed in the tutorial store.';

CREATE TABLE shipment_tracking (
  id          BIGINT PRIMARY KEY AUTO_RANDOM COMMENT 'Primary key for tracking events.',
  shipment_id BIGINT NOT NULL COMMENT 'Shipment this event belongs to.',
  event_time  TIMESTAMP NOT NULL COMMENT 'When this carrier scan event occurred (UTC).',
  location    VARCHAR(255) NULL COMMENT 'City or facility name reported by the carrier.',
  status      VARCHAR(64)  NOT NULL COMMENT 'Carrier status code or short description.',
  message     VARCHAR(512) NULL COMMENT 'Human-readable carrier event message.',
  created_at  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'Row creation time (UTC).',
  KEY idx_tracking_shipment_time (shipment_id, event_time),
  CONSTRAINT fk_tracking_shipment
    FOREIGN KEY (shipment_id) REFERENCES shipments(id)
) COMMENT='Individual carrier scan events for a shipment.';

-- ---------------------------------------------------------------------------
-- Seed: carriers
-- ---------------------------------------------------------------------------

INSERT INTO carriers (code, name, tracking_url_template) VALUES
  ('DHL',    'DHL Express',
   'https://www.dhl.com/en/express/tracking.html?AWB={tracking_number}'),
  ('UPS',    'UPS',
   'https://www.ups.com/track?loc=en_US&tracknum={tracking_number}'),
  ('FEDEX',  'FedEx',
   'https://www.fedex.com/en-us/tracking.html?tracknumbers={tracking_number}'),
  ('USPS',   'United States Postal Service',
   'https://tools.usps.com/go/TrackConfirmAction?tLabels={tracking_number}'),
  ('ONTRAC', 'OnTrac',
   'https://www.ontrac.com/tracking/?number={tracking_number}');

-- ---------------------------------------------------------------------------
-- Seed: shipments
-- ---------------------------------------------------------------------------
-- Join against tidb_graphql_tutorial.orders using created_at (unique per order
-- in the sample dataset) so order_id does not need to be hard-coded.

CREATE TEMPORARY TABLE shipment_seeds (
  order_created_at  TIMESTAMP    NOT NULL COMMENT 'Matches tidb_graphql_tutorial.orders.created_at.',
  carrier_code      VARCHAR(16)  NOT NULL,
  tracking_number   VARCHAR(128) NOT NULL,
  status            ENUM('pending','in_transit','delivered','returned','exception') NOT NULL,
  estimated_delivery DATE,
  shipped_at        TIMESTAMP,
  delivered_at      TIMESTAMP
);

INSERT INTO shipment_seeds
  (order_created_at, carrier_code, tracking_number, status, estimated_delivery, shipped_at, delivered_at)
VALUES
  -- ORD-0001  paid      → pending (label created, not yet picked up)
  ('2024-01-01 08:01:00','UPS',   '1Z999AA10123456784','pending',    '2024-01-05',NULL,NULL),
  -- ORD-0002  shipped   → in_transit
  ('2024-01-01 08:02:00','FEDEX','774899015280609',   'in_transit', '2024-01-04','2024-01-02 09:15:00',NULL),
  -- ORD-0005  paid      → pending
  ('2024-01-01 08:05:00','DHL',  '1234567890',        'pending',    '2024-01-07',NULL,NULL),
  -- ORD-0006  shipped   → delivered
  ('2024-01-01 08:06:00','UPS',  '1Z999AA10987654321','delivered',  '2024-01-04','2024-01-02 10:00:00','2024-01-04 14:22:00'),
  -- ORD-0009  paid      → pending
  ('2024-01-01 08:09:00','USPS', '9400111899223397521977','pending', '2024-01-08',NULL,NULL),
  -- ORD-0010  shipped   → delivered
  ('2024-01-01 08:10:00','FEDEX','772899285200296',   'delivered',  '2024-01-03','2024-01-02 11:00:00','2024-01-03 16:05:00'),
  -- ORD-0013  paid      → in_transit (picked up same day)
  ('2024-01-01 08:13:00','UPS',  '1Z999AA10234567891','in_transit', '2024-01-04','2024-01-01 17:00:00',NULL),
  -- ORD-0014  shipped   → delivered
  ('2024-01-01 08:14:00','DHL',  '9876543210',        'delivered',  '2024-01-03','2024-01-02 08:30:00','2024-01-03 11:45:00'),
  -- ORD-0017  paid      → pending
  ('2024-01-01 08:17:00','ONTRAC','C11899009900123',  'pending',    '2024-01-06',NULL,NULL),
  -- ORD-0018  shipped   → in_transit
  ('2024-01-01 08:18:00','FEDEX','774899726700002',   'in_transit', '2024-01-04','2024-01-02 14:00:00',NULL),
  -- ORD-0021  paid      → pending
  ('2024-01-01 08:21:00','UPS',  '1Z999AA10345678902','pending',    '2024-01-07',NULL,NULL),
  -- ORD-0022  shipped   → delivered
  ('2024-01-01 08:22:00','USPS', '9400111899223397522088','delivered','2024-01-04','2024-01-02 09:00:00','2024-01-04 13:00:00'),
  -- ORD-0025  paid      → in_transit
  ('2024-01-01 08:25:00','DHL',  '1122334455',        'in_transit', '2024-01-05','2024-01-01 18:30:00',NULL),
  -- ORD-0026  shipped   → delivered
  ('2024-01-01 08:26:00','FEDEX','774899837500193',   'delivered',  '2024-01-03','2024-01-02 10:45:00','2024-01-03 14:30:00'),
  -- ORD-0029  paid      → pending
  ('2024-01-01 08:29:00','ONTRAC','C11899009900456',  'pending',    '2024-01-08',NULL,NULL),
  -- ORD-0030  shipped   → in_transit
  ('2024-01-01 08:30:00','UPS',  '1Z999AA10456789013','in_transit', '2024-01-04','2024-01-02 08:00:00',NULL),
  -- ORD-0033  paid      → pending
  ('2024-01-01 08:33:00','USPS', '9400111899223397523199','pending', '2024-01-07',NULL,NULL),
  -- ORD-0034  shipped   → delivered
  ('2024-01-01 08:34:00','DHL',  '5566778899',        'delivered',  '2024-01-04','2024-01-02 11:30:00','2024-01-04 10:15:00'),
  -- ORD-0037  paid      → in_transit
  ('2024-01-01 08:37:00','FEDEX','774899948600384',   'in_transit', '2024-01-05','2024-01-01 16:15:00',NULL),
  -- ORD-0038  shipped   → delivered
  ('2024-01-01 08:38:00','UPS',  '1Z999AA10567890124','delivered',  '2024-01-03','2024-01-02 09:30:00','2024-01-03 15:00:00'),
  -- ORD-0041  paid      → pending
  ('2024-01-01 08:41:00','ONTRAC','C11899009900789',  'pending',    '2024-01-09',NULL,NULL),
  -- ORD-0042  shipped   → delivered
  ('2024-01-01 08:42:00','FEDEX','774899059700475',   'delivered',  '2024-01-04','2024-01-02 12:00:00','2024-01-04 11:30:00'),
  -- ORD-0045  paid      → in_transit
  ('2024-01-01 08:45:00','DHL',  '9988776655',        'in_transit', '2024-01-05','2024-01-01 19:00:00',NULL),
  -- ORD-0046  shipped   → delivered
  ('2024-01-01 08:46:00','UPS',  '1Z999AA10678901235','delivered',  '2024-01-03','2024-01-02 10:15:00','2024-01-03 16:45:00'),
  -- ORD-0049  paid      → pending
  ('2024-01-01 08:49:00','USPS', '9400111899223397524300','pending', '2024-01-08',NULL,NULL),
  -- ORD-0050  shipped   → in_transit
  ('2024-01-01 08:50:00','FEDEX','774899170800566',   'in_transit', '2024-01-04','2024-01-02 13:30:00',NULL),
  -- ORD-0053  paid      → pending
  ('2024-01-01 08:53:00','ONTRAC','C11899009901012',  'pending',    '2024-01-09',NULL,NULL),
  -- ORD-0054  shipped   → delivered
  ('2024-01-01 08:54:00','DHL',  '4433221100',        'delivered',  '2024-01-03','2024-01-02 08:45:00','2024-01-03 12:00:00'),
  -- ORD-0057  paid      → in_transit
  ('2024-01-01 08:57:00','UPS',  '1Z999AA10789012346','in_transit', '2024-01-05','2024-01-01 17:45:00',NULL),
  -- ORD-0058  shipped   → delivered
  ('2024-01-01 08:58:00','FEDEX','774899281900657',   'delivered',  '2024-01-04','2024-01-02 11:00:00','2024-01-04 13:15:00'),
  -- ORD-0061  paid      → pending
  ('2024-01-01 09:01:00','USPS', '9400111899223397525411','pending', '2024-01-07',NULL,NULL),
  -- ORD-0062  shipped   → in_transit
  ('2024-01-01 09:02:00','DHL',  '7766554433',        'in_transit', '2024-01-05','2024-01-02 15:00:00',NULL),
  -- ORD-0065  paid      → returned
  ('2024-01-01 09:05:00','UPS',  '1Z999AA10890123457','returned',   '2024-01-04','2024-01-02 09:00:00',NULL),
  -- ORD-0066  shipped   → exception (delivery attempt failed)
  ('2024-01-01 09:06:00','FEDEX','774899392000748',   'exception',  '2024-01-03','2024-01-02 10:30:00',NULL),
  -- ORD-0069  paid      → in_transit
  ('2024-01-01 09:09:00','ONTRAC','C11899009901345',  'in_transit', '2024-01-05','2024-01-01 18:00:00',NULL),
  -- ORD-0070  shipped   → delivered
  ('2024-01-01 09:10:00','UPS',  '1Z999AA10901234568','delivered',  '2024-01-04','2024-01-02 09:45:00','2024-01-04 15:30:00');

INSERT INTO shipments
  (order_id, carrier_id, tracking_number, status, estimated_delivery, shipped_at, delivered_at)
SELECT
  o.id,
  c.id,
  s.tracking_number,
  s.status,
  s.estimated_delivery,
  s.shipped_at,
  s.delivered_at
FROM shipment_seeds s
JOIN tidb_graphql_tutorial.orders o ON o.created_at = s.order_created_at
JOIN carriers c ON c.code = s.carrier_code;

DROP TEMPORARY TABLE shipment_seeds;

-- ---------------------------------------------------------------------------
-- Seed: shipment_tracking events
-- ---------------------------------------------------------------------------
-- Events are keyed by tracking_number (unique within this seed set).

CREATE TEMPORARY TABLE tracking_seeds (
  tracking_number VARCHAR(128) NOT NULL,
  event_time      TIMESTAMP    NOT NULL,
  location        VARCHAR(255),
  status          VARCHAR(64)  NOT NULL,
  message         VARCHAR(512)
);

INSERT INTO tracking_seeds
  (tracking_number, event_time, location, status, message)
VALUES
  -- UPS 1Z999AA10123456784  (pending — label created, not yet scanned)
  ('1Z999AA10123456784','2024-01-01 08:30:00','Seattle, WA',        'Label created',  'Shipment label created; package not yet received by UPS.'),

  -- FEDEX 774899015280609  (in_transit)
  ('774899015280609',  '2024-01-02 09:15:00','Portland, OR',        'Picked up',      'Package picked up from shipper.'),
  ('774899015280609',  '2024-01-02 18:40:00','Sacramento, CA',      'In transit',     'Arrived at FedEx sort facility.'),
  ('774899015280609',  '2024-01-03 06:00:00','Los Angeles, CA',     'In transit',     'Departed FedEx facility.'),

  -- DHL 1234567890  (pending — no events yet)

  -- UPS 1Z999AA10987654321  (delivered)
  ('1Z999AA10987654321','2024-01-02 10:00:00','Denver, CO',         'Picked up',      'Package received by UPS.'),
  ('1Z999AA10987654321','2024-01-02 20:15:00','Albuquerque, NM',    'In transit',     'Package transferred to destination facility.'),
  ('1Z999AA10987654321','2024-01-03 08:30:00','Phoenix, AZ',        'Out for delivery','Package is out for delivery.'),
  ('1Z999AA10987654321','2024-01-04 14:22:00','Phoenix, AZ',        'Delivered',      'Package delivered. Left at front door.'),

  -- USPS 9400111899223397521977  (pending — no events yet)

  -- FEDEX 772899285200296  (delivered)
  ('772899285200296',  '2024-01-02 11:00:00','Chicago, IL',         'Picked up',      'Package picked up from shipper.'),
  ('772899285200296',  '2024-01-02 22:30:00','Indianapolis, IN',    'In transit',     'Arrived at FedEx facility.'),
  ('772899285200296',  '2024-01-03 09:15:00','Columbus, OH',        'Out for delivery','On FedEx vehicle for delivery.'),
  ('772899285200296',  '2024-01-03 16:05:00','Columbus, OH',        'Delivered',      'Delivered to recipient.'),

  -- UPS 1Z999AA10234567891  (in_transit)
  ('1Z999AA10234567891','2024-01-01 17:00:00','Minneapolis, MN',    'Picked up',      'Package received by UPS.'),
  ('1Z999AA10234567891','2024-01-02 04:30:00','Milwaukee, WI',      'In transit',     'Arrived at UPS hub.'),
  ('1Z999AA10234567891','2024-01-02 18:00:00','Chicago, IL',        'In transit',     'Departed facility.'),

  -- DHL 9876543210  (delivered)
  ('9876543210',       '2024-01-02 08:30:00','Miami, FL',           'Picked up',      'Shipment picked up.'),
  ('9876543210',       '2024-01-02 20:00:00','Orlando, FL',         'In transit',     'In transit to destination.'),
  ('9876543210',       '2024-01-03 11:45:00','Tampa, FL',           'Delivered',      'Delivered to recipient. Signed by: J. SMITH'),

  -- ONTRAC C11899009900123  (pending — no events yet)

  -- FEDEX 774899726700002  (in_transit)
  ('774899726700002',  '2024-01-02 14:00:00','Dallas, TX',          'Picked up',      'Package picked up from sender.'),
  ('774899726700002',  '2024-01-02 23:00:00','Houston, TX',         'In transit',     'Arrived at FedEx sort facility.'),
  ('774899726700002',  '2024-01-03 11:00:00','San Antonio, TX',     'In transit',     'In transit to delivery location.'),

  -- UPS 1Z999AA10345678902  (pending — no events yet)

  -- USPS 9400111899223397522088  (delivered)
  ('9400111899223397522088','2024-01-02 09:00:00','Boston, MA',      'Acceptance',     'Package accepted at post office.'),
  ('9400111899223397522088','2024-01-02 21:30:00','Providence, RI',  'In transit',     'Arrived at USPS facility.'),
  ('9400111899223397522088','2024-01-03 08:00:00','Hartford, CT',    'In transit',     'Departed USPS facility.'),
  ('9400111899223397522088','2024-01-04 13:00:00','New Haven, CT',   'Delivered',      'Delivered, In/At Mailbox.'),

  -- DHL 1122334455  (in_transit)
  ('1122334455',       '2024-01-01 18:30:00','Atlanta, GA',         'Picked up',      'Shipment picked up.'),
  ('1122334455',       '2024-01-02 07:00:00','Charlotte, NC',       'In transit',     'Arrived at DHL facility.'),
  ('1122334455',       '2024-01-02 19:00:00','Raleigh, NC',         'In transit',     'In transit.'),

  -- FEDEX 774899837500193  (delivered)
  ('774899837500193',  '2024-01-02 10:45:00','Seattle, WA',         'Picked up',      'Package picked up from sender.'),
  ('774899837500193',  '2024-01-02 22:00:00','Portland, OR',        'In transit',     'Arrived at FedEx facility.'),
  ('774899837500193',  '2024-01-03 14:30:00','Portland, OR',        'Delivered',      'Delivered. Left at front door.'),

  -- ONTRAC C11899009900456  (pending — no events yet)

  -- UPS 1Z999AA10456789013  (in_transit)
  ('1Z999AA10456789013','2024-01-02 08:00:00','Denver, CO',         'Picked up',      'Package received by UPS.'),
  ('1Z999AA10456789013','2024-01-02 21:30:00','Salt Lake City, UT', 'In transit',     'Arrived at UPS hub.'),
  ('1Z999AA10456789013','2024-01-03 10:00:00','Las Vegas, NV',      'In transit',     'In transit.'),

  -- USPS 9400111899223397523199  (pending — no events yet)

  -- DHL 5566778899  (delivered)
  ('5566778899',       '2024-01-02 11:30:00','San Francisco, CA',   'Picked up',      'Package picked up.'),
  ('5566778899',       '2024-01-03 00:30:00','Sacramento, CA',      'In transit',     'In transit.'),
  ('5566778899',       '2024-01-03 08:00:00','Fresno, CA',          'Out for delivery','Out for delivery.'),
  ('5566778899',       '2024-01-04 10:15:00','Bakersfield, CA',     'Delivered',      'Delivered to recipient.'),

  -- FEDEX 774899948600384  (in_transit)
  ('774899948600384',  '2024-01-01 16:15:00','Nashville, TN',       'Picked up',      'Package picked up from sender.'),
  ('774899948600384',  '2024-01-02 05:00:00','Memphis, TN',         'In transit',     'Arrived at FedEx hub.'),
  ('774899948600384',  '2024-01-02 16:30:00','Little Rock, AR',     'In transit',     'Departed facility.'),

  -- UPS 1Z999AA10567890124  (delivered)
  ('1Z999AA10567890124','2024-01-02 09:30:00','Philadelphia, PA',   'Picked up',      'Package received by UPS.'),
  ('1Z999AA10567890124','2024-01-02 22:00:00','Trenton, NJ',        'In transit',     'Arrived at UPS facility.'),
  ('1Z999AA10567890124','2024-01-03 09:00:00','Newark, NJ',         'Out for delivery','Package is out for delivery.'),
  ('1Z999AA10567890124','2024-01-03 15:00:00','Newark, NJ',         'Delivered',      'Package delivered. Signature obtained.'),

  -- ONTRAC C11899009900789  (pending — no events yet)

  -- FEDEX 774899059700475  (delivered)
  ('774899059700475',  '2024-01-02 12:00:00','San Diego, CA',       'Picked up',      'Picked up from sender.'),
  ('774899059700475',  '2024-01-03 01:30:00','Los Angeles, CA',     'In transit',     'Arrived at sort facility.'),
  ('774899059700475',  '2024-01-04 09:00:00','Long Beach, CA',      'Out for delivery','On FedEx vehicle for delivery.'),
  ('774899059700475',  '2024-01-04 11:30:00','Long Beach, CA',      'Delivered',      'Delivered to recipient.'),

  -- DHL 9988776655  (in_transit)
  ('9988776655',       '2024-01-01 19:00:00','Portland, OR',        'Picked up',      'Shipment picked up.'),
  ('9988776655',       '2024-01-02 10:00:00','Eugene, OR',          'In transit',     'In transit.'),
  ('9988776655',       '2024-01-02 20:00:00','Medford, OR',         'In transit',     'Arrived at facility.'),

  -- UPS 1Z999AA10678901235  (delivered)
  ('1Z999AA10678901235','2024-01-02 10:15:00','Kansas City, MO',    'Picked up',      'Package received.'),
  ('1Z999AA10678901235','2024-01-02 22:30:00','St. Louis, MO',      'In transit',     'In transit.'),
  ('1Z999AA10678901235','2024-01-03 15:00:00','Springfield, IL',    'In transit',     'Departed facility.'),
  ('1Z999AA10678901235','2024-01-03 16:45:00','Springfield, IL',    'Delivered',      'Package delivered.'),

  -- USPS 9400111899223397524300  (pending — no events yet)

  -- FEDEX 774899170800566  (in_transit)
  ('774899170800566',  '2024-01-02 13:30:00','Detroit, MI',         'Picked up',      'Package picked up from shipper.'),
  ('774899170800566',  '2024-01-03 02:00:00','Cleveland, OH',       'In transit',     'Arrived at FedEx facility.'),
  ('774899170800566',  '2024-01-03 14:00:00','Pittsburgh, PA',      'In transit',     'In transit.'),

  -- ONTRAC C11899009901012  (pending — no events yet)

  -- DHL 4433221100  (delivered)
  ('4433221100',       '2024-01-02 08:45:00','Austin, TX',          'Picked up',      'Shipment picked up.'),
  ('4433221100',       '2024-01-02 19:00:00','San Antonio, TX',     'In transit',     'In transit to delivery area.'),
  ('4433221100',       '2024-01-03 12:00:00','Corpus Christi, TX',  'Delivered',      'Delivered to recipient.'),

  -- UPS 1Z999AA10789012346  (in_transit)
  ('1Z999AA10789012346','2024-01-01 17:45:00','Cincinnati, OH',     'Picked up',      'Package received.'),
  ('1Z999AA10789012346','2024-01-02 06:30:00','Columbus, OH',       'In transit',     'In transit.'),
  ('1Z999AA10789012346','2024-01-02 20:00:00','Pittsburgh, PA',     'In transit',     'Arrived at hub.'),

  -- FEDEX 774899281900657  (delivered)
  ('774899281900657',  '2024-01-02 11:00:00','Minneapolis, MN',     'Picked up',      'Package picked up.'),
  ('774899281900657',  '2024-01-02 23:30:00','Madison, WI',         'In transit',     'Arrived at facility.'),
  ('774899281900657',  '2024-01-03 11:00:00','Milwaukee, WI',       'Out for delivery','On vehicle for delivery.'),
  ('774899281900657',  '2024-01-04 13:15:00','Milwaukee, WI',       'Delivered',      'Delivered to front door.'),

  -- USPS 9400111899223397525411  (pending — no events yet)

  -- DHL 7766554433  (in_transit)
  ('7766554433',       '2024-01-02 15:00:00','Baltimore, MD',       'Picked up',      'Package picked up.'),
  ('7766554433',       '2024-01-03 04:00:00','Washington, DC',      'In transit',     'In transit.'),
  ('7766554433',       '2024-01-03 15:00:00','Richmond, VA',        'In transit',     'Arrived at DHL facility.'),

  -- UPS 1Z999AA10890123457  (returned)
  ('1Z999AA10890123457','2024-01-02 09:00:00','Denver, CO',         'Picked up',      'Package received by UPS.'),
  ('1Z999AA10890123457','2024-01-02 20:00:00','Boulder, CO',        'In transit',     'In transit.'),
  ('1Z999AA10890123457','2024-01-03 10:00:00','Boulder, CO',        'Delivery attempt failed','Recipient not available. Notice left.'),
  ('1Z999AA10890123457','2024-01-04 10:00:00','Boulder, CO',        'Return to sender','Return initiated at recipient request.'),

  -- FEDEX 774899392000748  (exception)
  ('774899392000748',  '2024-01-02 10:30:00','Phoenix, AZ',         'Picked up',      'Package picked up.'),
  ('774899392000748',  '2024-01-02 22:00:00','Tucson, AZ',          'In transit',     'In transit.'),
  ('774899392000748',  '2024-01-03 09:00:00','Flagstaff, AZ',       'Delivery exception','Address information insufficient; contacting sender.'),

  -- ONTRAC C11899009901345  (in_transit)
  ('C11899009901345',  '2024-01-01 18:00:00','Los Angeles, CA',     'Picked up',      'Package received at OnTrac facility.'),
  ('C11899009901345',  '2024-01-02 07:00:00','Riverside, CA',       'In transit',     'In transit.'),
  ('C11899009901345',  '2024-01-02 19:00:00','San Bernardino, CA',  'In transit',     'Arrived at facility.'),

  -- UPS 1Z999AA10901234568  (delivered)
  ('1Z999AA10901234568','2024-01-02 09:45:00','Houston, TX',        'Picked up',      'Package received.'),
  ('1Z999AA10901234568','2024-01-02 22:00:00','Baton Rouge, LA',    'In transit',     'In transit.'),
  ('1Z999AA10901234568','2024-01-03 13:00:00','New Orleans, LA',    'Out for delivery','On vehicle for delivery.'),
  ('1Z999AA10901234568','2024-01-04 15:30:00','New Orleans, LA',    'Delivered',      'Package delivered. Left with neighbor.');

INSERT INTO shipment_tracking (shipment_id, event_time, location, status, message)
SELECT
  sh.id,
  ts.event_time,
  ts.location,
  ts.status,
  ts.message
FROM tracking_seeds ts
JOIN shipments sh ON sh.tracking_number = ts.tracking_number;

DROP TEMPORARY TABLE tracking_seeds;
