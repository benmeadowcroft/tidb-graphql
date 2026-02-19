CREATE ROLE IF NOT EXISTS app_viewer;
CREATE ROLE IF NOT EXISTS app_admin;
CREATE ROLE IF NOT EXISTS app_introspect;

GRANT SELECT ON tidb_graphql_tutorial.products TO app_viewer;
GRANT SELECT ON tidb_graphql_tutorial.product_reviews TO app_viewer;
GRANT SELECT ON tidb_graphql_tutorial.categories TO app_viewer;
GRANT SELECT ON tidb_graphql_tutorial.product_categories TO app_viewer;
GRANT SELECT ON tidb_graphql_tutorial.orders TO app_viewer;
GRANT SELECT ON tidb_graphql_tutorial.order_items TO app_viewer;

GRANT SELECT, INSERT, UPDATE, DELETE ON tidb_graphql_tutorial.* TO app_admin;

GRANT SELECT ON tidb_graphql_tutorial.* TO app_introspect;
GRANT SELECT ON mysql.role_edges TO app_introspect;

CREATE USER IF NOT EXISTS 'tidb_graphql'@'%' IDENTIFIED BY 'tidb_graphql_pw';
GRANT SELECT ON mysql.role_edges TO 'tidb_graphql'@'%';
GRANT app_viewer TO 'tidb_graphql'@'%';
GRANT app_admin TO 'tidb_graphql'@'%';
GRANT app_introspect TO 'tidb_graphql'@'%';
