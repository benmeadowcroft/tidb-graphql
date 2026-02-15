# Docker Compose Scenarios

Use these compose scenarios based on your goal.

| Scenario | Command | Use when |
|---|---|---|
| `quickstart` | `docker compose -f examples/compose/quickstart/docker-compose.yml up` | You want a complete local demo with TiDB + sample data + GraphQL. |
| `remote-db` | `docker compose --env-file examples/compose/remote-db/.env -f examples/compose/remote-db/docker-compose.yml up` | You already have a running TiDB and only need GraphQL. |
| `quickstart-db-zero` | `scripts/tidb-zero-prepare --invite ... --database tidb_graphql_tutorial && docker compose -f examples/compose/quickstart-db-zero/docker-compose.yml up` | You want TiDB Zero quickstart with remote sample-data seeding. |
| `oidc-roles` | `docker compose -f examples/compose/oidc-roles/docker-compose.yml up` | You want to test OIDC auth and role-based DB authorization locally. |

Notes:
- Root `docker-compose.yml` includes `quickstart` so `docker compose up` still works.
- Podman users can replace `docker compose` with `podman compose`.
- Each scenario keeps its primary app settings in `config/tidb-graphql/tidb-graphql.example.yaml`.
- Use scenario `.env` files for runtime overrides such as image tags and DSN/secrets.
