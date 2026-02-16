# OIDC + Roles scenario

This scenario starts TiDB, seeds sample data and roles, runs a local JWKS server, and enables OIDC + DB role authorization in `tidb-graphql`.

Primary app settings are in:

[examples/compose/oidc-roles/config/tidb-graphql/tidb-graphql.example.yaml](./config/tidb-graphql/tidb-graphql.example.yaml)

## Start

```bash
docker compose -f examples/compose/oidc-roles/docker-compose.yml up
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
