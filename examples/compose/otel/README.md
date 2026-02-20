# OTEL scenario

This scenario is configured to emit OpenTelemetry traces (using Honeycomb as an example). It starts:
- step-ca (local CA)
- pki-bootstrap (one-shot certificate minting into a Docker named volume)
- TiDB with TLS + initialize-sql-file bootstrap for roles/users
- local JWKS server with a CA-issued TLS certificate
- `tidb-graphql` with HTTPS, OIDC, and DB role authorization enabled

Primary app settings are in:

[examples/compose/otel/config/tidb-graphql/tidb-graphql.example.yaml](./config/tidb-graphql/tidb-graphql.example.yaml)

Copy `.env.example` to `.env` and set your Honeycomb API key (plus optional `DEV_ADMIN_TOKEN` override):

```bash
cp examples/compose/otel/.env.example examples/compose/otel/.env
```

```dotenv
HONEYCOMB_API_KEY=<your-api-key>
# DEV_ADMIN_TOKEN=dev-admin-token
```

`docker-compose.yml` maps that value to `OTEL_EXPORTER_OTLP_HEADERS=x-honeycomb-team=<key>` at runtime so no token is committed to git.

## Start

```bash
docker compose -f examples/compose/otel/docker-compose.yml up
```

If you have previously started this scenario before the TLS/cert-auth changes, reset volumes once so `initialize-sql-file` can bootstrap users/roles:

```bash
docker compose -f examples/compose/otel/docker-compose.yml down -v
docker compose -f examples/compose/otel/docker-compose.yml up
```

`tidb-graphql` serves HTTPS on `https://localhost:8080/graphql`.

## Mint a role token

In a separate shell from the project root:

```bash
make token-viewer SCENARIO=otel
```

To mint an admin-role token:

```bash
make token-admin SCENARIO=otel
```

`make token-*` calls JWKS `POST /dev/token` with `X-Admin-Token`.
It reads `DEV_ADMIN_TOKEN` from `examples/compose/otel/.env`, and falls back to `dev-admin-token`.

## Call GraphQL with token

```bash
curl -H "Authorization: Bearer <TOKEN>" \
  -H "Content-Type: application/json" \
  -d '{"query":"{ __typename }"}' \
  -k \
  https://localhost:8080/graphql
```

`-k` is used because the local dev CA root is stored in a Docker volume (`pki`) rather than the host trust store.
