# Refresh schema safely

Goal: rebuild the GraphQL schema when the database changes.

## Manual refresh

If auth is enabled, include a token. Otherwise call directly:

```bash
curl -X POST http://localhost:8080/admin/reload-schema
```

The server rebuilds the schema snapshot and swaps it atomically.

## Adaptive polling

Polling is designed to be safe and low-impact. It hashes `INFORMATION_SCHEMA.TABLES` metadata (table name, `CREATE_TIME`, and `UPDATE_TIME`) to detect change.

```yaml
schema_refresh:
  polling_enabled: true
  polling_interval: 1m
```

When a change is detected, the schema rebuilds in the background and swaps in.

## Practical advice

- Use manual reload for one-off migrations.
- Use polling for environments where the schema changes occasionally.
- Expect a short build time after changes; the old schema remains active until swap.

---
# Related Docs

## Next steps
- [Use config precedence correctly](config-precedence.md)
- [Add observability](observability.md)

## Reference
- [Configuration reference](../reference/configuration.md)

## Further reading
- [Schema handling](../explanation/schema-handling.md)

## Back
- [How-to home](README.md)
- [Docs home](../README.md)
