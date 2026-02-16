# Database-first authorization

The short version: I did not want to build a second authorization system.

We already trust the database to tell us what data exists. Introspection is the source of truth for schema shape. It felt consistent to trust the database to decide who can see that data too, rather than inventing a parallel set of rules in YAML config files.

When role-based authorization is enabled, TiDB GraphQL maps claims from the JWT to database roles and activates those roles per request. TiDB then enforces table and column privileges. The server is just a mapper.

Schema introspection is handled separately. When role-based authorization is enabled, the server will assume a dedicated introspection role in order to build the schema. This role must be configured explicitly and should have read access to the schema. This keeps runtime queries constrained to request roles while still allowing the schema to refresh safely.

With role-based auth enabled, the server can also build per-role schema snapshots from discovered
roles so each role sees only the GraphQL surface it can query. This keeps schema visibility aligned
with runtime access decisions while preserving database-led authorization.

The practical upside is that access control stays in one place. You can change permissions with SQL, not by editing config or redeploying the API. If a role claim is missing or invalid, the request is rejected. The system fails closed by default.

---
# Related Docs

## Next steps
- [Configure OIDC/JWKS auth](../how-to/oidc-jwks-auth.md)
- [DB user for roles](../how-to/db-user-for-roles.md)

## Reference
- [Auth reference](../reference/auth.md)
- [Configuration reference](../reference/configuration.md)

## Further reading
- [Auth architecture](auth-architecture.md)

## Back
- [Explanation home](README.md)
- [Docs home](../README.md)
