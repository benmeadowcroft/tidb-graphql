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
