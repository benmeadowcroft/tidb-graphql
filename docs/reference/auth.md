# Auth reference

This server supports OIDC/JWKS authentication and optional database role activation.

## OIDC/JWKS

Key settings:

- `server.auth.oidc_enabled` (bool)
- `server.auth.oidc_issuer_url` (HTTPS only)
- `server.auth.oidc_audience` (expected audience)
- `server.auth.oidc_clock_skew` (JWT clock skew allowance)
- `server.auth.oidc_skip_tls_verify` (dev-only; logs a warning)

When enabled:
- `/graphql` requires a Bearer token.
- `/admin/reload-schema` requires a Bearer token.

There is no `oidc_allow_insecure_http` option; issuer URLs must be HTTPS.
`server.auth.oidc_issuer_url` and `server.auth.oidc_audience` are required when OIDC is enabled.

## Database role authorization

When enabled, the server maps a JWT claim to a TiDB role and runs `SET ROLE` per request.
This requires OIDC to be enabled so the claim is validated.

Key settings:

- `server.auth.db_role_enabled` (bool)
- `server.auth.db_role_claim_name` (string; default `db_role`)
- `server.auth.db_role_introspection_role` (string; role used during schema introspection)
- `server.auth.role_schema_include` (list; default `["*"]`)
- `server.auth.role_schema_exclude` (list; default empty)
- `server.auth.role_schema_max_roles` (int; default `64`)

Role discovery attempts `mysql.role_edges` and falls back to `information_schema.applicable_roles` if needed.
When DB role auth is enabled, role-specific GraphQL schemas are built for selected roles
based on include/exclude filters. Unknown role schemas are rejected (fail-closed).

Claim rules:
- The claim value must be a string.
- Missing or invalid roles are rejected (fail-closed).
