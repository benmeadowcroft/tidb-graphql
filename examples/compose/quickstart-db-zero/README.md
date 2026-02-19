# Quickstart DB Zero scenario

This scenario provisions/reuses a TiDB Zero instance, seeds tutorial sample data into the remote database, and runs `tidb-graphql` locally.

`tidb-graphql` reads MySQL client settings from `.auth/tidb-zero.my.cnf` using `database.mycnf_file`.
That file is the canonical source for connection settings in this scenario.

The seed step loads:
- `docs/tutorials/sample-data.sql`
- `docs/tutorials/sample-data-vectors-auto-embedding.sql`

The vector dataset uses TiDB auto-embedding with model `tidbcloud_free/amazon/titan-embed-text-v2` and a generated `VECTOR(1024)` column.

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

You should see a vector search root field for reviews (for example `searchProductReviewsByEmbeddingVector`) with:
- `vector` (explicit vector mode)
- `queryText` (auto-embedding text mode)

## Admin reload endpoint demo (`X-Admin-Token`)

This scenario enables `/admin/reload-schema` and requires `X-Admin-Token`.

Without token (expected `401`):

```bash
curl -i -X POST http://localhost:8080/admin/reload-schema
```

With token (expected `200`):

```bash
curl -i -X POST http://localhost:8080/admin/reload-schema \
  -H "X-Admin-Token: quickstart-db-zero-admin-token"
```

Optional: change the demo token before startup in:

```bash
examples/compose/quickstart-db-zero/config/tidb-graphql/tidb-graphql.example.yaml
```

## Useful flags

- `scripts/tidb-zero-prepare --status`
- `scripts/tidb-zero-prepare --refresh --invite <code> --database tidb_graphql_tutorial`
- `scripts/tidb-zero-prepare --clear-local`
