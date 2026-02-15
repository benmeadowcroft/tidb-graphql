# Quickstart scenario

This scenario starts a complete local development stack:
- TiDB
- tutorial sample data
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

## Stop

```bash
docker compose -f examples/compose/quickstart/docker-compose.yml down
```

## Reset (remove data)

```bash
docker compose -f examples/compose/quickstart/docker-compose.yml down -v
```

Podman users can replace `docker compose` with `podman compose`.
