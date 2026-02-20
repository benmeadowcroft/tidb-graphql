-- Idempotent security bootstrap for OIDC + role-based local scenario.
-- This runs only on first TiDB data-dir initialization.

CREATE ROLE IF NOT EXISTS app_viewer;
CREATE ROLE IF NOT EXISTS app_admin;
CREATE ROLE IF NOT EXISTS app_introspect;

GRANT SELECT ON tidb_graphql_tutorial.products TO app_viewer;
GRANT SELECT ON tidb_graphql_tutorial.categories TO app_viewer;
GRANT SELECT ON tidb_graphql_tutorial.product_categories TO app_viewer;
GRANT SELECT ON tidb_graphql_tutorial.orders TO app_viewer;
GRANT SELECT ON tidb_graphql_tutorial.order_items TO app_viewer;

GRANT SELECT, INSERT, UPDATE, DELETE ON tidb_graphql_tutorial.* TO app_admin;

GRANT SELECT ON tidb_graphql_tutorial.* TO app_introspect;
GRANT SELECT ON mysql.role_edges TO app_introspect;

CREATE USER IF NOT EXISTS 'app_mtls'@'%' REQUIRE SUBJECT '/CN=app_mtls';
GRANT SELECT ON mysql.role_edges TO 'app_mtls'@'%';
GRANT app_viewer TO 'app_mtls'@'%';
GRANT app_admin TO 'app_mtls'@'%';
GRANT app_introspect TO 'app_mtls'@'%';

CREATE USER IF NOT EXISTS 'seed_mtls'@'%' REQUIRE SUBJECT '/CN=seed_mtls';
GRANT CREATE ON *.* TO 'seed_mtls'@'%';
GRANT ALL PRIVILEGES ON tidb_graphql_tutorial.* TO 'seed_mtls'@'%';

FLUSH PRIVILEGES;
