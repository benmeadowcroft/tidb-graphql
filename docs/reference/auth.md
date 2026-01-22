# Auth reference

This server supports OIDC/JWKS authentication and optional database role activation.

## OIDC/JWKS

Key settings:

- `server.oidc_enabled` (bool)
- `server.oidc_issuer_url` (HTTPS only)
- `server.oidc_audience` (expected audience)
- `server.oidc_clock_skew` (JWT clock skew allowance)
- `server.oidc_skip_tls_verify` (dev-only; logs a warning)

When enabled:
- `/graphql` requires a Bearer token.
- `/admin/reload-schema` requires a Bearer token.

There is no `oidc_allow_insecure_http` option; issuer URLs must be HTTPS.
`server.oidc_issuer_url` and `server.oidc_audience` are required when OIDC is enabled.

## Database role authorization

When enabled, the server maps a JWT claim to a TiDB role and runs `SET ROLE` per request.
This requires OIDC to be enabled so the claim is validated.

Key settings:

- `server.db_role_enabled` (bool)
- `server.db_role_claim_name` (string; default `db_role`)
- `server.db_role_validation` (bool; validate claim against discovered roles)
- `server.db_role_introspection_role` (string; role used during schema introspection)

Role discovery attempts `mysql.role_edges` and falls back to `information_schema.applicable_roles` if needed.

Claim rules:
- The claim value must be a string.
- Missing or invalid roles are rejected (fail-closed).
