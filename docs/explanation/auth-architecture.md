# Auth architecture

Authentication and authorization in TiDB GraphQL are intentionally thin. The server validates JWTs and maps a single claim to a database role, but the database is still the source of truth for access control.

## Request flow

1) **OIDC/JWKS middleware** validates the bearer token and loads claim data into the request context.
2) **Role activation** extracts the configured claim (`server.auth.db_role_claim_name`) and, when enabled, applies `SET ROLE` for the request.
3) **Resolvers** execute SQL under that role. TiDB enforces table/column access.

This keeps policy centralized in the database while still letting the API enforce authentication consistently for GraphQL and admin endpoints.

## Role discovery and validation

When `server.auth.db_role_validation_enabled` is enabled, the server discovers the roles granted to the database user and rejects roles that are not on the allowlist. This prevents callers from supplying arbitrary role names.

## Introspection flow

Schema building uses a dedicated introspection role (`server.auth.db_role_introspection_role`). It is assumed only while building the schema snapshot; runtime requests are constrained to the caller's role.

When role-based authorization is enabled, the server also builds role-specific schema snapshots
for discovered roles (filtered by `server.auth.role_schema_include`/`server.auth.role_schema_exclude`).
Requests are routed to the snapshot that matches the validated role claim, and unknown role snapshots
are rejected (fail-closed).

## Error behavior

- Missing or invalid tokens are rejected at the HTTP layer.
- Missing or invalid roles are rejected before query execution.
- Valid roles that lack privileges result in database-level access errors in GraphQL responses.

---
# Related Docs

## Next steps
- [Configure OIDC/JWKS auth](../how-to/oidc-jwks-auth.md)
- [DB user for roles](../how-to/db-user-for-roles.md)

## Reference
- [Auth reference](../reference/auth.md)
- [Configuration reference](../reference/configuration.md)

## Further reading
- [Database-first authorization](database-first-auth.md)
- [Middleware architecture](middleware-architecture.md)

## Back
- [Explanation home](README.md)
- [Docs home](../README.md)
