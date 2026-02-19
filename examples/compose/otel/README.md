# OTEL scenario

This scenario is configured to emit OpenTelemetry traces (using Honeycomb as an example). The scenario starts TiDB, seeds sample data and roles, runs a local JWKS server, and enables OIDC + DB role authorization in `tidb-graphql`.

Primary app settings are in:

[examples/compose/otel/config/tidb-graphql/tidb-graphql.example.yaml](./config/tidb-graphql/tidb-graphql.example.yaml)

Copy `.env.example` to `.env` and set your Honeycomb API key:

```bash
cp examples/compose/otel/.env.example examples/compose/otel/.env
```

```dotenv
HONEYCOMB_API_KEY=<your-api-key>
```

`docker-compose.yml` maps that value to `OTEL_EXPORTER_OTLP_HEADERS=x-honeycomb-team=<key>` at runtime so no token is committed to git.

## Start

```bash
docker compose -f examples/compose/otel/docker-compose.yml up
```

## Mint a role token

In a separate shell from the project root:

```bash
go run ./scripts/jwt-mint --issuer https://jwks:9000 --audience tidb-graphql --kid local-key --db_role app_viewer
```

> [!NOTE]
> The issuer is set to `https://jwks:9000` as `jwks` is the service name in [docker-compose.yml](docker-compose.yml). The same issuer is also set in the [tidb-graphql.example.yaml](./config/tidb-graphql/tidb-graphql.example.yaml) configuration.

## Call GraphQL with token

```bash
curl -H "Authorization: Bearer <TOKEN>" \
  -H "Content-Type: application/json" \
  -d '{"query":"{ __typename }"}' \
  http://localhost:8080/graphql
```
