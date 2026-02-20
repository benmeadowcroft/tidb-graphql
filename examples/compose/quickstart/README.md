# Quickstart scenario

This scenario starts a complete local development stack:
- TiDB
- tutorial sample data (including vector review add-on data)
- `tidb-graphql`

## Start

From repository root:

```bash
docker compose -f examples/compose/quickstart/docker-compose.yml up
```

Or use the root compose shortcut:

```bash
docker compose up
```

Open [http://localhost:8080/graphql](http://localhost:8080/graphql).

## Admin reload endpoint demo (`X-Admin-Token`)

This scenario enables `/admin/reload-schema` and requires `X-Admin-Token`.

Without token (expected `401`):

```bash
curl -i -X POST http://localhost:8080/admin/reload-schema
```

With token (expected `200`):

```bash
curl -i -X POST http://localhost:8080/admin/reload-schema \
  -H "X-Admin-Token: quickstart-admin-token"
```

Optional: change the demo token before startup in:

```bash
examples/compose/quickstart/config/tidb-graphql/tidb-graphql.example.yaml
```

## Stop

```bash
docker compose -f examples/compose/quickstart/docker-compose.yml down
```

## Reset (remove data)

```bash
docker compose -f examples/compose/quickstart/docker-compose.yml down -v
```

Podman users can replace `docker compose` with `podman compose`.
