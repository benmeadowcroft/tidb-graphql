# TiDB GraphQL

TiDB GraphQL is a SQL-first GraphQL server for teams who want a fast API over TiDB by introspecting a live schema into GraphQL.

This project is experimental.

## Quick start

Run:

```bash
git clone https://github.com/benmeadowcroft/tidb-graphql.git && cd tidb-graphql
docker compose up -d
```

This starts TiDB, tutorial sample data, `tidb-graphql`, and GraphiQL.

> [!NOTE]
> if you are using `podman compose`, due to an issue handling the `include:` it is recommended to explicitly provide the path to the scenarios compose file using the `-f` parameter, e.g.:
> `docker compose up -f examples/compose/quickstart/docker-compose.yml -d`

## Run local code image (development)

If you are testing local code changes, build and run a local image explicitly:

```bash
docker build -t tidb-graphql:dev -f Containerfile .
TIGQL_IMAGE=tidb-graphql:dev docker compose up -d
```

## Verify it works

Open [http://localhost:8080/graphql](http://localhost:8080/graphql) and run:

```graphql
query getUsers {
  users(first: 3) {
    nodes {
      id
      fullName
      email
    }
  }
}
```

You should see user records from the tutorial dataset, for example:

```json
{
  "data": {
    "users": {
      "nodes": [
        {
          "id": "...",
          "fullName": "...",
          "email": "..."
        }
      ]
    }
  }
}
```

Headless check (optional):

```bash
curl -s -X POST http://localhost:8080/graphql \
  -H "content-type: application/json" \
  -d '{"query":"query getUsers { users(first: 3) { nodes { id fullName email } } }"}'
```

## Prerequisites

- Docker Compose v2 or Podman Compose
- Port `8080` (for TiDB-GraphQL) & `4000` (for TiDB) available
- Internet access for first-run image pulls (GHCR/Docker Hub)

## Choose a scenario

Use one of these compose scenarios:

| Scenario | Use when | Guide |
|---|---|---|
| `quickstart` | You want a complete local demo (TiDB + sample data + GraphQL). | [quickstart README](examples/compose/quickstart/README.md) |
| `quickstart-db-zero` | You want the same quickstart flow but with a remote TiDB Zero database. | [quickstart-db-zero README](examples/compose/quickstart-db-zero/README.md) |
| `remote-db` | You already have a running TiDB and only need GraphQL. | [remote-db README](examples/compose/remote-db/README.md) |
| `oidc-roles` | You want local OIDC/JWKS + role-based database auth testing. | [oidc-roles README](examples/compose/oidc-roles/README.md) |
| `otel` | You want to test OpenTelemetry integration. | [otel README](examples/compose/otel/README.md) |

See [Compose scenarios](examples/compose/README.md) for details.

## Stop / reset

Stop default quickstart:

```bash
docker compose down
```

Reset default quickstart (remove volumes):

```bash
docker compose down -v
```

Scenario-specific example:

```bash
docker compose -f examples/compose/quickstart-db-zero/docker-compose.yml down
docker compose -f examples/compose/quickstart-db-zero/docker-compose.yml down -v
```

Podman users can replace `docker compose` with `podman compose`.

## Common first-run issues

- See [First-run troubleshooting](docs/how-to/troubleshooting-first-run.md) for quick diagnosis commands and fixes.

For deeper setup details, use:
- [Compose scenarios](examples/compose/README.md)
- [Configuration precedence](docs/how-to/config-precedence.md)
- [Your first schema tutorial](docs/tutorials/first-schema.md)

## Next steps

- [Tutorials](docs/tutorials/README.md)
- [How-To guides](docs/how-to/README.md)
- [Technical reference](docs/reference/README.md)
- [Architecture and explanation docs](docs/explanation/README.md)

## Contributing and local development

Common development commands:

Notes:
- `make build` outputs `bin/tidb-graphql`.
- `make test-unit` runs fast unit tests in `./internal/...`.
- `make test-integration` requires TiDB credentials via environment variables or `.env.test` (see `.env.test.example`).

## License

Apache-2.0. See [LICENSE](LICENSE).
