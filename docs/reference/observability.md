# Observability reference

TiDB GraphQL supports metrics, tracing, logging, and SQL commenter through OpenTelemetry.

## Metrics endpoint

- `/metrics` is exposed when `observability.metrics_enabled` is true.
- Format: Prometheus.

## Custom GraphQL metrics

- `graphql.request.duration` (histogram, ms)
  - labels: `operation_type`, `has_errors`
- `graphql.requests.total` (counter)
  - labels: `operation_type`, `has_errors`
- `graphql.errors.total` (counter)
  - labels: `operation_type`
- `graphql.requests.active` (updown counter)
- `graphql.query.depth` (histogram)
  - labels: `operation_type`
- `graphql.results.count` (histogram)
  - labels: `operation_type`

## Schema refresh metrics

- `schema.refresh.total` (counter)
  - labels: `trigger`, `success`
- `schema.refresh.errors.total` (counter)
  - labels: `trigger`
- `schema.refresh.duration` (histogram, ms)
  - labels: `trigger`, `success`
- `schema.refresh.last_success_unix` (gauge, unix seconds)

## Security metrics

- `security.auth.attempts.total` (counter)
  - labels: `endpoint`
- `security.auth.failures.total` (counter)
  - labels: `endpoint`, `reason`
- `security.auth.successes.total` (counter)
  - labels: `endpoint`, `issuer`
- `security.admin.access.total` (counter)
  - labels: `operation`, `authenticated`, `success`
- `security.unauthorized.attempts.total` (counter)
  - labels: `endpoint`, `reason`
- `security.token.validation_errors.total` (counter)
  - labels: `error_type`

## Tracing

Tracing is enabled with `observability.tracing_enabled`. When enabled, HTTP, GraphQL, and SQL spans are emitted via OTLP.

## SQL commenter

`observability.sqlcommenter_enabled` injects trace context into SQL comments so you can correlate database queries with traces. It requires tracing to be enabled.

## Logging

- Console logs are always emitted.
- OTLP log export is enabled with `observability.logging.exports_enabled`.
