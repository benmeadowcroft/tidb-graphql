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
- `database.pool.max_open` → `TIGQL_DATABASE_POOL_MAX_OPEN`
- `server.graphql_max_depth` → `TIGQL_SERVER_GRAPHQL_MAX_DEPTH`
- `server.auth.oidc_enabled` → `TIGQL_SERVER_AUTH_OIDC_ENABLED`
- `observability.otlp.endpoint` → `TIGQL_OBSERVABILITY_OTLP_ENDPOINT`

See `docs/how-to/config-precedence.md` for precedence rules.

## database

You can configure the database connection using either:
1. **DSN (Data Source Name)**: A single connection string
2. **Discrete fields**: Individual settings for host, port, user, etc.

### Connection string (DSN)

- `database.dsn` (string, default: empty) — Complete MySQL DSN in go-sql-driver format: `user:password@tcp(host:port)/database?params`
- `database.dsn_file` (string, default: empty) — Path to file containing DSN (use `@-` for stdin)
- `database.mycnf_file` (string, default: empty) — Path to MySQL defaults file (`.my.cnf` style)

When `dsn` is set, it overrides the discrete connection fields below for connection details.
`database.mycnf_file` is an alternative to DSN and is mutually exclusive with `dsn`/`dsn_file`.

### Effective database resolution

`tidb-graphql` computes one canonical target database used by:
- schema introspection/refresh
- role-aware `USE <database>` behavior

Resolution rules:
1. If `database.database` is set, it is the canonical target.
2. Otherwise, if DSN contains `/database`, that DSN database is canonical.
3. Otherwise, if `mycnf_file` contains `database=...` (usually in `[client]`), that database is canonical.
4. If both are set and differ, startup fails with a validation error.
5. If neither provides a database name, startup fails with a validation error.

This prevents connecting to one database via DSN while introspecting a different one.

In `mycnf_file` mode, supported keys are:
- `[client]` `host`, `port`, `user`, `password`, `database`, `ssl-mode`
- `[mysql]` `database` (fallback if `[client]` does not provide one)

### Discrete connection fields

- `database.host` (string, default: `localhost`)
- `database.port` (int, default: `4000`)
- `database.user` (string, default: `tidb_graphql`)
- `database.password` (string, default: empty)
- `database.password_file` (string, default: empty; use `@-` to read from stdin)
- `database.password_prompt` (bool, default: `false`)
- `database.database` (string, default: `test`)

### TLS/SSL configuration

Configure TLS for secure database connections, including client certificate authentication (mTLS).

- `database.tls.mode` (string, default: empty) — TLS verification mode:
  - `off` — No TLS (plaintext connection)
  - `skip-verify` — TLS without server certificate verification (insecure)
  - `verify-ca` — TLS with CA verification (requires `ca_file`)
  - `verify-full` — TLS with full verification including hostname (requires `ca_file`)
- `database.tls.ca_file` (string, default: empty) — Path to CA certificate for server verification
- `database.tls.ca_file_env` (string, default: empty) — Environment variable containing CA file path
- `database.tls.cert_file` (string, default: empty) — Path to client certificate for mTLS
- `database.tls.cert_file_env` (string, default: empty) — Environment variable containing client cert path
- `database.tls.key_file` (string, default: empty) — Path to client private key for mTLS
- `database.tls.key_file_env` (string, default: empty) — Environment variable containing client key path
- `database.tls.server_name` (string, default: empty) — Override TLS server name for verification

### Connection pool

- `database.pool.max_open` (int, default: `25`) — Maximum open database connections
- `database.pool.max_idle` (int, default: `5`) — Maximum idle connections in pool
- `database.pool.max_lifetime` (duration, default: `5m`) — Connection max lifetime

### Connection behavior

- `database.connection_timeout` (duration, default: `60s`) — Maximum time to wait for database on startup. Set to `0` to fail immediately.
- `database.connection_retry_interval` (duration, default: `2s`) — Initial interval between connection retry attempts. Uses exponential backoff capped at 30s.

### Examples

**Simple DSN:**
```yaml
database:
  dsn: "user:password@tcp(tidb.example.com:4000)/mydb?parseTime=true"
```

**DSN-only canonical database (recommended):**
```yaml
database:
  dsn_file: /run/secrets/database_dsn
  # database.database omitted; canonical database is read from DSN path.
```

**DSN + matching database.database (allowed):**
```yaml
database:
  dsn: "user:password@tcp(tidb.example.com:4000)/mydb?parseTime=true"
  database: mydb
```

**DSN + conflicting database.database (invalid):**
```yaml
database:
  dsn: "user:password@tcp(tidb.example.com:4000)/mydb?parseTime=true"
  database: otherdb # startup validation error: mismatch with DSN database
```

**MySQL defaults file mode (`my.cnf`):**
```yaml
database:
  mycnf_file: /run/secrets/mysql-client.my.cnf
```

Example `my.cnf`:
```ini
[client]
host = gateway.tidbcloud.com
port = 4000
user = graphql_app
password = super-secret
database = tidb_graphql_tutorial
ssl-mode = VERIFY_IDENTITY
```

**DSN with mTLS (TiDB Cloud):**
```yaml
database:
  dsn: "user:password@tcp(gateway.tidbcloud.com:4000)/mydb"
  tls:
    mode: verify-full
    ca_file: /etc/ssl/tidb/ca-cert.pem
    cert_file: /etc/ssl/tidb/client-cert.pem
    key_file: /etc/ssl/tidb/client-key.pem
```

**Discrete fields with TLS:**
```yaml
database:
  host: gateway.tidbcloud.com
  port: 4000
  user: graphql_app
  password_file: /run/secrets/db_password
  database: production
  tls:
    mode: verify-full
    ca_file: /etc/ssl/tidb/ca-cert.pem
    cert_file: /etc/ssl/tidb/client-cert.pem
    key_file: /etc/ssl/tidb/client-key.pem
```

**Container-friendly with env var indirection:**
```yaml
database:
  dsn_file: /run/secrets/database_dsn
  tls:
    mode: verify-full
    ca_file_env: TIDB_CA_CERT_PATH
    cert_file_env: TIDB_CLIENT_CERT_PATH
    key_file_env: TIDB_CLIENT_KEY_PATH
```

**Scenario example (TiDB Zero local prep):**
```yaml
database:
  dsn_file: /run/secrets/tidb-zero.dsn
```

## server

- `server.port` (int, default: `8080`)
- `server.graphql_max_depth` (int, default: `5`)
- `server.graphql_max_complexity` (int, default: `0` = unlimited)
- `server.graphql_max_rows` (int, default: `0` = unlimited)
- `server.graphql_default_limit` (int, default: `100`) - default forward page size (`first` when omitted) for root and relationship connection collection fields
- `server.search.vector_require_index` (bool, default: `true`) - require a vector-search-capable index before exposing vector search root fields
- `server.search.vector_max_top_k` (int, default: `100`) - maximum allowed `first` value for vector search connection fields
- `server.schema_refresh_min_interval` (duration, default: `30s`)
- `server.schema_refresh_max_interval` (duration, default: `5m`)
- `server.read_timeout` (duration, default: `15s`)
- `server.write_timeout` (duration, default: `15s`)
- `server.idle_timeout` (duration, default: `60s`)
- `server.shutdown_timeout` (duration, default: `30s`)
- `server.health_check_timeout` (duration, default: `2s`)
- `server.graphiql_enabled` (bool, default: `false`)

Authentication (under `server.auth`):
- `server.auth.oidc_enabled` (bool, default: `false`)
- `server.auth.oidc_issuer_url` (string, default: empty; must be HTTPS)
- `server.auth.oidc_audience` (string, default: empty)
- `server.auth.oidc_clock_skew` (duration, default: `2m`)
- `server.auth.oidc_skip_tls_verify` (bool, default: `false`; dev-only)
- `server.auth.db_role_enabled` (bool, default: `false`)
- `server.auth.db_role_claim_name` (string, default: `db_role`)
- `server.auth.db_role_validation_enabled` (bool, default: `true`)
- `server.auth.db_role_introspection_role` (string, default: empty; role used only during schema refresh)
- `server.auth.role_schema_include` (list of string, default: `["*"]`; role glob patterns to include for role-specific schemas)
- `server.auth.role_schema_exclude` (list of string, default: empty; role glob patterns to exclude from role-specific schemas)
- `server.auth.role_schema_max_roles` (int, default: `64`; maximum number of role-specific schemas to build)

When `server.auth.db_role_enabled` is true, the server builds role-specific GraphQL schemas
from discovered database roles. Discovery is filtered by `role_schema_include`/`role_schema_exclude`
(case-insensitive glob matching), and startup/refresh fails if selected roles exceed `role_schema_max_roles`.
An explicitly empty include list is treated as `["*"]`.

Rate limiting:
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
- `server.tls_mode` (string, default: `off`; values: `off`, `auto`, `file`)
  - `off` — HTTP only (no TLS)
  - `auto` — Auto-generate self-signed certificates (similar to TiDB's `auto-tls`)
  - `file` — Use provided certificate and key files
- `server.tls_cert_file` (string, default: empty) — Required when `tls_mode: file`
- `server.tls_key_file` (string, default: empty) — Required when `tls_mode: file`
- `server.tls_auto_cert_dir` (string, default: `.tls`) — Directory for auto-generated certs when `tls_mode: auto`

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
- `schema_filters.deny_mutation_tables` (list of string, default: empty)
- `schema_filters.scan_views_enabled` (bool, default: `false`)
- `schema_filters.allow_columns` (map of table => list of glob patterns, default: `{"*": ["*"]}`)
- `schema_filters.deny_columns` (map of table => list of glob patterns, default: empty)
- `schema_filters.deny_mutation_columns` (map of table => list of glob patterns, default: empty)

See `docs/reference/schema-filters.md` for behavior details.

## type_mappings

- `type_mappings.uuid_columns` (map of table => list of column glob patterns, default: empty)
- `type_mappings.tinyint1_boolean_columns` (map of table => list of column glob patterns, default: empty)
- `type_mappings.tinyint1_int_columns` (map of table => list of column glob patterns, default: empty)

`uuid_columns` uses case-insensitive SQL-name pattern matching (table + column), with wildcard merge semantics:
- patterns from `"*"` apply to all tables
- table-specific patterns are merged with wildcard patterns

Example:

```yaml
type_mappings:
  uuid_columns:
    "*": ["*_uuid"]
    "orders": ["id", "customer_uuid"]
  tinyint1_boolean_columns:
    "*": ["is_*", "has_*"]
  tinyint1_int_columns:
    "event_flags": ["is_deleted"] # explicit escape hatch when tinyint(1) is not semantic boolean
```

Mapped columns are exposed as `UUID` in GraphQL (for supported SQL storage types) and use canonical lowercase hyphenated output.
Supported mapped SQL storage types are:
- `BINARY(16)` / `VARBINARY(16)` (canonical RFC byte order, equivalent to `UUID_TO_BIN(x,0)`)
- `CHAR(36)` / `VARCHAR(36)` (canonical text UUID)

`tinyint1_*_columns` is also case-insensitive SQL-name pattern matching (table + column), with wildcard merge semantics. Precedence rules:
- `tinyint1_int_columns` wins over `tinyint1_boolean_columns` when both match.
- both mappings only apply to SQL `TINYINT(1)` columns; other targets are rejected during schema build.

## naming

Controls how SQL table names are converted to GraphQL type names (singularization/pluralization).

- `naming.plural_overrides` (map of string => string, default: `{}`)
  Maps singular form to custom plural. Example: `{"person": "people", "status": "statuses"}`
- `naming.singular_overrides` (map of string => string, default: `{}`)
  Maps plural form to custom singular. Example: `{"people": "person", "data": "datum"}`
