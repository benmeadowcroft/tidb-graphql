# Configure OIDC/JWKS authentication

Goal: enable authentication with JWT validation using OIDC (OpenID Connect) discovery + JWKS for `/graphql` and `/admin/*`.

## 1) Choose your OIDC source

Pick one of these and use it for the rest of the guide:

Production (OIDC provider):
- Gather your issuer URL and audience from your provider.

Local dev (self-signed JWKS):
- Run the local JWKS server and mint a token:

```bash
go run ./scripts/jwt-generate-keys
go run ./scripts/jwks-server --addr :9000 --issuer https://localhost:9000
go run ./scripts/jwt-mint --issuer https://localhost:9000 --audience tidb-graphql --kid local-key
```

See [Secure local dev with OIDC/JWKS](../tutorials/local-oidc.md) for more details on the local JWKS server for development testing.

## 2) Set the OIDC config

Production (OIDC provider):
```yaml
server:
  auth:
    oidc_enabled: true
    oidc_issuer_url: "https://issuer.example.com"
    oidc_audience: "tidb-graphql"
    oidc_clock_skew: 2m
    oidc_skip_tls_verify: false
```

Local dev (self-signed JWKS):
```yaml
server:
  auth:
    oidc_enabled: true
    oidc_issuer_url: "https://localhost:9000"
    oidc_audience: "tidb-graphql"
    oidc_skip_tls_verify: true
```

Notes:
- `server.auth.oidc_issuer_url` must be HTTPS.
- `server.auth.oidc_skip_tls_verify` is for local dev with self-signed certs and logs a warning.

## 3) Restart the TiDB GraphQL server

The OIDC configuration is loaded at startup.

## 4) Call GraphQL with a token

```bash
curl -H "Authorization: Bearer <TOKEN>" \
  -H "Content-Type: application/json" \
  -d '{"query":"{ __typename }"}' \
  http://localhost:8080/graphql
```

If you omit the token, requests should be rejected.

## 5) Protect admin endpoints too

Enable admin schema reload explicitly:

```yaml
server:
  admin:
    schema_reload_enabled: true
```

When OIDC is enabled and the admin endpoint is enabled, `/admin/reload-schema` is protected automatically.

---
# Related Docs

## Next steps
- [DB user for roles](db-user-for-roles.md) - Enable authorization (not just authentication)
- [Secure local dev with OIDC/JWKS](../tutorials/local-oidc.md)

## Reference
- [Auth reference](../reference/auth.md)
- [Configuration reference](../reference/configuration.md)

## Further reading
- [Auth architecture](../explanation/auth-architecture.md)
- [Middleware architecture](../explanation/middleware-architecture.md)

## Back
- [How-to home](README.md)
- [Docs home](../README.md)
