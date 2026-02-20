# Refresh schema safely

Goal: rebuild the GraphQL schema when the database changes.

## Manual refresh

`/admin/reload-schema` is disabled by default. Enable it first:

```yaml
server:
  admin:
    schema_reload_enabled: true
```

If OIDC is enabled, use Bearer auth:

```bash
curl -X POST http://localhost:8080/admin/reload-schema \
  -H "Authorization: Bearer <TOKEN>"
```

If OIDC is disabled, configure `server.admin.auth_token` (or `server.admin.auth_token_file`) and send `X-Admin-Token`:

```bash
curl -X POST http://localhost:8080/admin/reload-schema \
  -H "X-Admin-Token: <ADMIN_TOKEN>"
```

The server rebuilds the schema snapshot and swaps it atomically.

## Adaptive polling

Polling is designed to be safe and low-impact. By default it uses a TiDB-first structural fingerprint
that hashes metadata from:

- `INFORMATION_SCHEMA.TABLES` (base tables and views)
- `INFORMATION_SCHEMA.COLUMNS`
- `INFORMATION_SCHEMA.KEY_COLUMN_USAGE` (primary keys and foreign keys)
- `INFORMATION_SCHEMA.STATISTICS` (indexes)

If structural fingerprinting fails, it falls back to a lightweight table timestamp fingerprint based on
`INFORMATION_SCHEMA.TABLES` (`CREATE_TIME`/`UPDATE_TIME` for base tables).

Polling cadence is controlled by:

```yaml
server:
  schema_refresh_min_interval: 30s
  schema_refresh_max_interval: 5m
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
