# Flags and environment variables

This server uses the same configuration keys across YAML, environment variables, and CLI flags.

## Environment variables

- Prefix: `TIGQL_`
- Path segments are uppercased and separated by underscores.

Examples:

```bash
export TIGQL_DATABASE_HOST=localhost
export TIGQL_SERVER_GRAPHQL_MAX_DEPTH=5
export TIGQL_OBSERVABILITY_LOGGING_LEVEL=debug
```

List values (CORS, allow/deny lists) are comma-separated:

```bash
export TIGQL_SERVER_CORS_ALLOWED_ORIGINS=http://localhost:3000,https://app.example.com
```

## Command-line flags

- Use dotted keys that mirror YAML.

Examples:

```bash
./bin/tidb-graphql \
  --config=./local_config.yaml \
  --database.host=localhost \
  --database.port=4000 \
  --database.user=root \
  --database.database=app_db
```

Version info:

```bash
./bin/tidb-graphql --version
```

## Password handling shortcuts

- Prompt: `--database.password_prompt`
- Password file: `database.password_file` (or `--database.password_file`)
- Read from stdin: `--database.password_file=@-`

Example with a password manager:

```bash
op read "op://MyVault/TiDB/password" | ./bin/tidb-graphql --database.password=@-
```

## Precedence

1. Flags
2. Environment variables
3. YAML config
