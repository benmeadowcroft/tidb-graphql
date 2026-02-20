# OIDC + Roles scenario

This scenario starts:
- step-ca (local CA)
- pki-bootstrap (one-shot certificate minting into a Docker named volume)
- TiDB with TLS + initialize-sql-file bootstrap for roles/users
- local JWKS server with a CA-issued TLS certificate
- `tidb-graphql` with HTTPS, OIDC, and DB role authorization enabled

Primary app settings are in:

[examples/compose/oidc-roles/config/tidb-graphql/tidb-graphql.example.yaml](./config/tidb-graphql/tidb-graphql.example.yaml)

Copy `.env.example` to `.env` to optionally override runtime values:

```bash
cp examples/compose/oidc-roles/.env.example examples/compose/oidc-roles/.env
```

## Start

```bash
docker compose -f examples/compose/oidc-roles/docker-compose.yml up
```

If you have previously started this scenario before the TLS/cert-auth changes, reset volumes once so `initialize-sql-file` can bootstrap users/roles:

```bash
docker compose -f examples/compose/oidc-roles/docker-compose.yml down -v
docker compose -f examples/compose/oidc-roles/docker-compose.yml up
```

`tidb-graphql` serves HTTPS on `https://localhost:8080/graphql`.

## Mint a role token

In a separate shell from the project root:

```bash
make token-viewer SCENARIO=oidc-roles
```

To mint an admin-role token:

```bash
make token-admin SCENARIO=oidc-roles
```

`make token-*` calls JWKS `POST /dev/token` with `X-Admin-Token`.
It reads `DEV_ADMIN_TOKEN` from `examples/compose/oidc-roles/.env`, and falls back to `dev-admin-token`.

## Call GraphQL with token

```bash
curl -H "Authorization: Bearer <TOKEN>" \
  -H "Content-Type: application/json" \
  -d '{"query":"{ __typename }"}' \
  -k \
  https://localhost:8080/graphql
```

`-k` is used because the local dev CA root is stored in a Docker volume (`pki`) rather than the host trust store.
