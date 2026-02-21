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
Sampling is controlled with `observability.trace_sample_ratio`:
- `1.0`: sample all traces
- `0.0`: sample no traces
- `(0.0, 1.0)`: parent-aware ratio sampling

Span naming conventions:
- HTTP root spans use `METHOD route` (for example `POST /graphql`, `GET /health`).
- Startup root span: `startup.init` with child spans such as `startup.db_connect`.
- Schema refresh root spans: `schema.refresh.startup`, `schema.refresh.poll`, `schema.refresh.manual`.

Common GraphQL span attributes:
- `graphql.operation.requested_name`: operation name requested by the client payload (if present).
- `graphql.operation.name`: resolved operation name selected from the parsed document.
- `graphql.operation.type`: `query`, `mutation`, or `subscription` when operation selection succeeds.
- `graphql.operation.hash`: deterministic hash of canonical selected operation + dependent fragments.
- `graphql.document.size_bytes`: size of the GraphQL document text in bytes.
- `graphql.query.field_count`, `graphql.query.depth`, `graphql.query.variable_count`: query-shape metadata.
- `auth.role`: active request role when role schemas are enabled.
- `schema.fingerprint`: active schema snapshot fingerprint.

`schema.fingerprint` and `graphql.operation.hash` are orthogonal: one identifies the serving schema version, the other identifies request operation text.

## SQL commenter

`observability.sqlcommenter_enabled` injects trace context into SQL comments so you can correlate database queries with traces. It requires tracing to be enabled.

## Logging

- Console logs are always emitted.
- OTLP log export is enabled with `observability.logging.exports_enabled`.
