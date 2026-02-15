# First-run troubleshooting

Quick checks for common startup issues.

## 1) Service not reachable on `http://localhost:8080/graphql`

Check container status:

```bash
docker compose ps
```

If port `8080` is already in use, stop the conflicting process or change published ports in compose.

## 2) Local code changes are not reflected

You are likely running the remote image tag.

Build and run with a local image:

```bash
docker build -t tidb-graphql:dev -f Containerfile .
TIGQL_IMAGE=tidb-graphql:dev docker compose up
```

## 3) Podman vs Docker Compose behavior differences

Validate compose files with the detected engine:

```bash
make compose-validate
```

If behavior differs, prefer scenario-specific compose files under `examples/compose/*`.

## 4) `quickstart-db-zero` starts with partial schema

Initial introspection can run while seed import is still completing.
This self-heals on periodic schema refresh.

Inspect recent logs:

```bash
docker compose -f examples/compose/quickstart-db-zero/docker-compose.yml logs --tail=200
```

## 5) Verify API is healthy before querying

```bash
curl -sf http://localhost:8080/health
```

If this fails, check compose status/logs first.
