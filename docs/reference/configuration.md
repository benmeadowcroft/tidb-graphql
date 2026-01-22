# Configuration reference

Configuration can be set via YAML, environment variables, or flags. This reference lists YAML keys, types, and defaults.

A Configuration file can be set via the `--config` parameter. For example:

```bash
./bin/tidb-graphql --config=./tidb-graphql.local.yaml
```

If no configuration file is passed in as a parameter the file search path (first match wins) is:
- `/etc/tidb-graphql/tidb-graphql.yaml`
- `$HOME/.tidb-graphql/tidb-graphql.yaml`
- `./tidb-graphql.yaml`

## Environment variables

All configuration keys can be set via environment variables using this format:
- Prefix: `TIGQL_`
- Replace `.` with `_`
- Use UPPERCASE

Examples:
- `database.host` → `TIGQL_DATABASE_HOST`
- `database.max_open_conns` → `TIGQL_DATABASE_MAX_OPEN_CONNS`
- `server.graphql_max_depth` → `TIGQL_SERVER_GRAPHQL_MAX_DEPTH`
- `observability.otlp.endpoint` → `TIGQL_OBSERVABILITY_OTLP_ENDPOINT`

See `docs/how-to/config-precedence.md` for precedence rules.

## database

- `database.host` (string, default: `localhost`)
- `database.port` (int, default: `4000`)
- `database.user` (string, default: `tidb_graphql`)
- `database.password` (string, default: empty)
- `database.password_file` (string, default: empty; use `@-` to read from stdin)
- `database.password_prompt` (bool, default: `false`)
- `database.database` (string, default: `test`)
- `database.tls_mode` (string, default: `skip-verify`; values: `skip-verify`, `true`, `false`)
  - YAML booleans (`true`/`false`) are accepted and mapped to the string values.
- `database.max_open_conns` (int, default: `25`)
- `database.max_idle_conns` (int, default: `5`)
- `database.conn_max_lifetime` (duration, default: `5m`)

## server

- `server.port` (int, default: `8080`)
- `server.graphql_max_depth` (int, default: `5`)
- `server.graphql_max_complexity` (int, default: `0` = unlimited)
- `server.graphql_max_rows` (int, default: `0` = unlimited)
- `server.graphql_list_limit_default` (int, default: `100`)
- `server.schema_refresh_min_interval` (duration, default: `30s`)
- `server.schema_refresh_max_interval` (duration, default: `5m`)
- `server.read_timeout` (duration, default: `15s`)
- `server.write_timeout` (duration, default: `15s`)
- `server.idle_timeout` (duration, default: `60s`)
- `server.shutdown_timeout` (duration, default: `30s`)
- `server.health_check_timeout` (duration, default: `2s`)
- `server.graphiql_enabled` (bool, default: `false`)
- `server.oidc_enabled` (bool, default: `false`)
- `server.oidc_issuer_url` (string, default: empty; must be HTTPS)
- `server.oidc_audience` (string, default: empty)
- `server.oidc_clock_skew` (duration, default: `2m`)
- `server.oidc_skip_tls_verify` (bool, default: `false`; dev-only)
- `server.db_role_enabled` (bool, default: `false`)
- `server.db_role_claim_name` (string, default: `db_role`)
- `server.db_role_validation` (bool, default: `true`)
- `server.db_role_introspection_role` (string, default: empty; role used only during schema refresh)
- `server.rate_limit_enabled` (bool, default: `false`)
- `server.rate_limit_rps` (float, default: `0`)
- `server.rate_limit_burst` (int, default: `0`)

CORS:
- `server.cors_enabled` (bool, default: `false`)
- `server.cors_allowed_origins` (list of string, default: empty)
- `server.cors_allowed_methods` (list of string, default: `GET, POST, OPTIONS`)
- `server.cors_allowed_headers` (list of string, default: `Content-Type, Authorization`)
- `server.cors_expose_headers` (list of string, default: empty)
- `server.cors_allow_credentials` (bool, default: `false`)
- `server.cors_max_age` (int seconds, default: `86400`)

TLS/HTTPS:
- `server.tls_enabled` (bool, default: `false`)
- `server.tls_cert_mode` (string, default: `file`; values: `file`, `selfsigned`)
- `server.tls_cert_file` (string, default: empty)
- `server.tls_key_file` (string, default: empty)
- `server.tls_selfsigned_cert_dir` (string, default: `.tls`)

## observability

- `observability.service_name` (string, default: `tidb-graphql`)
- `observability.service_version` (string, default: empty; falls back to build version)
- `observability.environment` (string, default: `development`)
- `observability.metrics_enabled` (bool, default: `true`)
- `observability.tracing_enabled` (bool, default: `false`)
- `observability.sqlcommenter_enabled` (bool, default: `true`; requires tracing)

Logging:
- `observability.logging.level` (string, default: `info`; values: `debug`, `info`, `warn`, `error`)
- `observability.logging.format` (string, default: `json`; values: `json`, `text`)
- `observability.logging.exports_enabled` (bool, default: `false`)

OTLP (applies to all signals unless overridden):
- `observability.otlp.protocol` (string, default: `grpc`)
- `observability.otlp.endpoint` (string, default: `localhost:4317`)
- `observability.otlp.insecure` (bool, default: `false`)
- `observability.otlp.tls_cert_file` (string, default: empty)
- `observability.otlp.tls_client_cert_file` (string, default: empty)
- `observability.otlp.tls_client_key_file` (string, default: empty)
- `observability.otlp.headers` (map, default: `{}`)
- `observability.otlp.timeout` (duration, default: `10s`)
- `observability.otlp.compression` (string, default: `gzip`)
- `observability.otlp.retry_enabled` (bool, default: `true`)
- `observability.otlp.retry_max_attempts` (int, default: `3`)

## schema_filters

- `schema_filters.allow_tables` (list of string, default: `["*"]`)
- `schema_filters.deny_tables` (list of string, default: empty)
- `schema_filters.scan_views` (bool, default: `false`)
- `schema_filters.allow_columns` (map of table => list of glob patterns, default: `{"*": ["*"]}`)
- `schema_filters.deny_columns` (map of table => list of glob patterns, default: empty)

See `docs/reference/schema-filters.md` for behavior details.

## naming

Controls how SQL table names are converted to GraphQL type names (singularization/pluralization).

- `naming.plural_overrides` (map of string => string, default: `{}`)
  Maps singular form to custom plural. Example: `{"person": "people", "status": "statuses"}`
- `naming.singular_overrides` (map of string => string, default: `{}`)
  Maps plural form to custom singular. Example: `{"people": "person", "data": "datum"}`
