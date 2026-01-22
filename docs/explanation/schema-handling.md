# Schema handling: build, snapshot, refresh

The GraphQL schema is built in two stages, then treated as an immutable snapshot. That combination is intentional: it keeps the system predictable while the database continues to evolve.

## Stage 1: introspection

Introspection reads the live TiDB schema from INFORMATION_SCHEMA and produces an internal model. This is the point where the database tells the truth about tables, columns, indexes, and relationships.

## Stage 2: schema construction

The resolver layer uses the introspection model to construct the GraphQL schema. This keeps the API shape separate from the raw database facts and makes it easier to change the GraphQL layer without touching introspection.

## Snapshots and immutability

Once the schema is built, it is treated as a snapshot: built, used, and left alone. When changes are detected, a new snapshot is built and atomically swapped in. The old one stays in place until the new one is ready.

This is the simplest way to keep GraphQL execution consistent. Resolvers always see a complete schema, and callers do not get half-built changes during a refresh.

A snapshot includes:
- the introspected database schema
- the compiled GraphQL schema
- the GraphQL handler

The guiding rule is: never mutate in place. Build a new snapshot and swap.

The refresh manager detects changes by hashing table details from `INFORMATION_SCHEMA.TABLES` and uses adaptive polling to balance freshness and load.

---
# Related Docs

## Next steps
- [Refresh schema safely](../how-to/schema-refresh.md)
- [Architecture overview](architecture.md)

## Reference
- [Configuration reference](../reference/configuration.md)

## Back
- [Explanation home](README.md)
- [Docs home](../README.md)
