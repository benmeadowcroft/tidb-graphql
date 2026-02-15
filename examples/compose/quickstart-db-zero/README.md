# Quickstart DB Zero scenario

This scenario provisions/reuses a TiDB Zero instance, seeds tutorial sample data into the remote database, and runs `tidb-graphql` locally.

`tidb-graphql` reads MySQL client settings from `.auth/tidb-zero.my.cnf` using `database.mycnf_file`.
That file is the canonical source for connection settings in this scenario.

## First run

```bash
scripts/tidb-zero-prepare --invite <your-invitation-code> --database tidb_graphql_tutorial
docker compose -f examples/compose/quickstart-db-zero/docker-compose.yml up
```

If you set `database.database` in the app config, it must match the `database` value in `.auth/tidb-zero.my.cnf` or startup validation will fail.

## Subsequent runs (reuse until expiry)

```bash
scripts/tidb-zero-prepare --database tidb_graphql_tutorial
docker compose -f examples/compose/quickstart-db-zero/docker-compose.yml up
```

Open [http://localhost:8080/graphql](http://localhost:8080/graphql).

## Useful flags

- `scripts/tidb-zero-prepare --status`
- `scripts/tidb-zero-prepare --refresh --invite <code> --database tidb_graphql_tutorial`
- `scripts/tidb-zero-prepare --clear-local`
