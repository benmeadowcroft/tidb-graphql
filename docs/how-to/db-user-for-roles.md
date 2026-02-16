# Create a database user for role-based authorization

Goal: set up a TiDB user with minimal privileges that relies entirely on roles for table access, enabling effective role-based authorization via SET ROLE.

## Why this matters

When `server.auth.db_role_enabled` is true, tidb-graphql uses `SET ROLE` to restrict access based on JWT claims. However, SET ROLE only affects **role-based privileges**, not direct user grants. If your database user has `SELECT ON *.*` or `SELECT ON database.*`, those privileges would remain active regardless of what role is set. If overly broad grants are detected when role based authorization is enabled the system will fail to start.

For role-based authorization to work correctly, your database user must:
1. Have **no direct SELECT privileges** on the target database
2. Have only the ability to assume roles
3. Rely entirely on roles for table access

## 1) Create roles with specific permissions

First, create roles that define different access levels:

```sql
-- Create example user roles
CREATE ROLE app_viewer;
CREATE ROLE app_admin;

-- Create example introspection role
CREATE ROLE app_introspect;

-- Grant permissions to each role
GRANT SELECT ON tidb_graphql_tutorial.products TO app_viewer;
GRANT SELECT ON tidb_graphql_tutorial.categories TO app_viewer;
GRANT SELECT ON tidb_graphql_tutorial.product_categories TO app_viewer;
GRANT SELECT ON tidb_graphql_tutorial.orders TO app_viewer;
GRANT SELECT ON tidb_graphql_tutorial.order_items TO app_viewer;

GRANT SELECT ON tidb_graphql_tutorial.* TO app_admin;

GRANT SELECT ON tidb_graphql_tutorial.* TO app_introspect;
GRANT SELECT ON mysql.role_edges TO app_introspect;
```

## 2) Create a restricted database user

Create a new user with minimal privileges - only what's needed to connect and assume roles:

Note:
- If creating a user on TIDB Cloud Starter then you may need to include a prefix in the name, see https://docs.pingcap.com/tidbcloud/select-cluster-tier/?plan=starter#user-name-prefix

```sql
-- Create the user (use a strong password)
CREATE USER 'tidb_graphql'@'%' IDENTIFIED BY 'your-secure-password';

-- Grant ability to discover roles on TiDB
GRANT SELECT ON mysql.role_edges TO 'tidb_graphql'@'%';

-- Grant the ability to assume the application roles
GRANT app_viewer TO 'tidb_graphql'@'%';
GRANT app_admin TO 'tidb_graphql'@'%';

-- Grant the ability to assume the introspection role
GRANT app_introspect TO 'tidb_graphql'@'%';
```

**Important:** Do NOT grant SELECT directly on your application tables. The user should only access tables through roles.

## 3) Verify the user has correct privileges

Connect as the new user and check grants:

```sql
SHOW GRANTS FOR CURRENT_USER();
```

You should see output like:
```
+-----------------------------------------------------------------------------------------------------+
| Grants for tidb_graphql@%                                                           |
+-----------------------------------------------------------------------------------------------------+
| GRANT USAGE ON *.* TO 'tidb_graphql'@'%'                                            |
| GRANT SELECT ON `mysql`.`role_edges` TO 'tidb_graphql'@'%'                          |
| GRANT 'app_admin'@'%', 'app_introspect'@'%', 'app_viewer'@'%' TO 'tidb_graphql'@'%' |
+-----------------------------------------------------------------------------------------------------+
```

You should **NOT** see any `GRANT SELECT ON tidb_graphql_tutorial.*` or `GRANT SELECT ON *.*`.

## 4) Test role switching

Verify that role-based access control works:

```sql
-- Without any role, should fail
SELECT * FROM tidb_graphql_tutorial.orders;
-- ERROR: SELECT command denied

-- Set viewer role
SET ROLE app_viewer;
SELECT * FROM tidb_graphql_tutorial.orders;
-- Success

-- Try to access users (viewer role shouldn't have access)
SELECT * FROM tidb_graphql_tutorial.users;
-- ERROR: SELECT command denied

-- Switch to admin role
SET ROLE app_admin;
SELECT * FROM tidb_graphql_tutorial.users;
-- Success

-- Reset to no roles
SET ROLE NONE;
```

## 5) Configure tidb-graphql

Update your configuration to use the new restricted user:

```yaml
database:
  host: "your-tidb-host"
  port: 4000
  user: "tidb_graphql"
  password: "your-secure-password"
  database: "tidb_graphql_tutorial"

server:
  auth:
    oidc_enabled: true
    oidc_issuer_url: "https://your-issuer.example.com"
    oidc_audience: "tidb-graphql"

    db_role_enabled: true
    db_role_claim_name: "db_role"  # JWT claim containing the role name
    db_role_validation_enabled: true  # Validate role against discovered roles
    db_role_introspection_role: "app_introspect" # role to assume for introspection
```

Notes:
- The claim must be a single string value.
- The introspection role is used only while building the schema snapshot.
- Role-based auth requires OIDC to be enabled with an issuer URL and audience.

## 6) Mint tokens with role claims

Your OIDC provider should include the database role in the JWT:

```json
{
  "sub": "user-123",
  "aud": "tidb-graphql",
  "db_role": "app_viewer"
}
```

The `db_role` claim value must match one of the roles granted to the database user.

Note:
- If you are using the Local dev (self-signed JWKS) then you can mint tokens with the desired role as follows:

```bash
go run ./scripts/jwt-mint --issuer https://localhost:9000 --audience tidb-graphql --kid local-key --db_role app_viewer
```

## 7) Start the server

When tidb-graphql starts with `server.auth.db_role_enabled: true`, it validates that the database user doesn't have overly broad privileges. If validation passes, you'll see a log message like:

```
INFO database user privileges validated for role-based authorization
```

If the user has direct SELECT privileges, the server will fail to start with an error explaining the issue.

## Troubleshooting

### Server fails with "database user has overly broad privileges"

Your database user has direct SELECT access. Check grants:

```sql
SHOW GRANTS FOR 'tidb_graphql'@'%';
```

If you see `GRANT SELECT ON myapp.*` or similar, revoke it:

```sql
REVOKE SELECT ON myapp.* FROM 'tidb_graphql'@'%';
```

### "role not allowed" errors

The JWT contains a role that isn't granted to the database user. Either:
- Grant the role: `GRANT role_name TO 'tidb_graphql'@'%';`
- Or update your OIDC provider to issue valid role names

### Queries succeed when they should fail

Double-check that the database user has no direct table privileges. The server validates this at startup, but if you modified privileges after starting, restart the server.

---
# Related Docs

## Next steps
- [Configure OIDC/JWKS auth](oidc-jwks-auth.md)
- [Secure local dev with OIDC/JWKS](../tutorials/local-oidc.md)

## Reference
- [Auth reference](../reference/auth.md)
- [Configuration reference](../reference/configuration.md)

## Further reading
- [Database-first authorization](../explanation/database-first-auth.md)
- [Auth architecture](../explanation/auth-architecture.md)

## Back
- [How-to home](README.md)
- [Docs home](../README.md)
