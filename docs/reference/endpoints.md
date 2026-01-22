# HTTP endpoints

All endpoints are served on the configured `server.port` (default `8080`).

## /graphql

- Method: `POST` (GraphQL queries and mutations)
  - Protected by OIDC when `server.oidc_enabled` is true.

- Method: `GET` (GraphiQL UI)
  - Only exposed when `server.graphiql_enabled` is true.
  - Serves the GraphiQL UI.

## /

Redirects to `/graphql`.

## /health

- Method: `GET`
  - Returns JSON and checks database connectivity.
  - Not authenticated.

Example response:

```json
{"status":"healthy","database":"ok"}
```

## /admin/reload-schema

- Method: `POST`
  - Triggers a schema rebuild and atomic swap.
  - Protected by OIDC when `server.oidc_enabled` is true.

## /metrics

- Method: `GET`
  - Prometheus format.
  - Only exposed when `observability.metrics_enabled` is true.
