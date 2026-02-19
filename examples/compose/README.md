# Docker Compose Scenarios

Use these compose scenarios based on your goal.

| Scenario | Guide | Use when |
|---|---|---|
| `quickstart` | [quickstart README](quickstart/README.md) | You want a complete local demo with TiDB + sample data + GraphQL. |
| `remote-db` | [remote-db README](remote-db/README.md) | You already have a running TiDB and only need GraphQL. |
| `quickstart-db-zero` | [quickstart-db-zero README](quickstart-db-zero/README.md) | You want TiDB Zero quickstart with remote sample-data seeding. |
| `oidc-roles` | [oidc-roles README](oidc-roles/README.md) | You want to test OIDC auth and role-based DB authorization locally. |
| `otel` | [otel README](otel/README.md) | You want to test OpenTelemetry (requires OTEL config), includes OIDC auth and role-based DB authorization locally. |

Notes:
- Root `docker-compose.yml` includes `quickstart` so `docker compose up` still works.
- Podman users can replace `docker compose` with `podman compose`.
- Each scenario keeps its primary app settings in `config/tidb-graphql/tidb-graphql.example.yaml`.
- Use scenario `.env` files for runtime overrides such as image tags and DSN/secrets.
