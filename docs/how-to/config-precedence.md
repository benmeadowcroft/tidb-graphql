# Use config precedence correctly

Goal: avoid confusion by knowing which configuration wins.

Order of precedence (highest to lowest):

1. Command-line flags
2. Environment variables
3. Configuration file

## Example

If you set all three, the flag wins:

```bash
./bin/tidb-graphql \
  --database.host=prod-db \
  --database.user=app
```

```bash
export TIGQL_DATABASE_HOST=dev-db
export TIGQL_DATABASE_USER=dev
```

```yaml
database:
  host: localhost
  user: root
```

Result:
- `database.host` is `prod-db`
- `database.user` is `app`

## DSN and database name precedence

`database.database` and DSN path must resolve to one canonical database.

Rules:
1. `database.database` is used when set.
2. Otherwise DSN path database is used.
3. If both are set and different, startup fails.

### Matching values (valid)

```yaml
database:
  dsn: "user:pass@tcp(db.example.com:4000)/appdb?parseTime=true"
  database: appdb
```

### Conflicting values (invalid)

```yaml
database:
  dsn: "user:pass@tcp(db.example.com:4000)/appdb?parseTime=true"
  database: analytics
```

This fails validation to prevent connecting to one DB while introspecting another.

## DSN vs my.cnf mode

You must choose one connection-string mode:
1. `database.dsn` / `database.dsn_file`
2. `database.mycnf_file`

Setting both is invalid and fails startup validation.

---
# Related Docs

## Next steps
- [Add observability](observability.md)
- [Refresh schema safely](schema-refresh.md)

## Reference
- [Configuration reference](../reference/configuration.md)
- [Flags and environment variables](../reference/cli-env.md)

## Further reading
- [Architecture overview](../explanation/architecture.md)

## Back
- [How-to home](README.md)
- [Docs home](../README.md)
