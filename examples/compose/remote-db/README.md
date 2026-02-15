# Remote DB scenario

This scenario runs only `tidb-graphql` and connects it to an existing TiDB database.

## Configure

Copy the example environment file:

```bash
cp examples/compose/remote-db/.env.example examples/compose/remote-db/.env
```

Edit `examples/compose/remote-db/.env` and set `TIGQL_DATABASE_DSN`.

## Start

```bash
docker compose --env-file examples/compose/remote-db/.env -f examples/compose/remote-db/docker-compose.yml up
```

Open [http://localhost:8080/graphql](http://localhost:8080/graphql).

## Stop

```bash
docker compose --env-file examples/compose/remote-db/.env -f examples/compose/remote-db/docker-compose.yml down
```

Podman users can replace `docker compose` with `podman compose`.
