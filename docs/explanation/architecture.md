# Architecture overview

At a high level, TiDB GraphQL is a pipeline that turns a live TiDB schema into a GraphQL API, then executes queries through a planning layer.

## The flow

1) **Introspection** reads INFORMATION_SCHEMA and produces an internal schema model.
2) **Resolver schema construction** turns that model into a GraphQL schema and resolver set.
3) **Query planning** takes the GraphQL AST plus the schema model and builds a SQL plan.
4) **Execution** runs the plan and returns results, with batching for one-to-many lookups.
5) **Schema refresh** rebuilds snapshots and swaps them atomically when the database changes.

## Key components

- `internal/introspection`: builds the database schema model.
- `internal/resolver`: constructs the GraphQL schema and resolvers.
- `internal/planner`: validates queries and produces SQL plans.
- `internal/schemarefresh`: detects changes and swaps schema snapshots.
- `internal/observability`: metrics, tracing, and logging.
- `internal/middleware`: auth, role mapping, rate limiting, and request logging.

## Design constraints

- The schema snapshot is immutable once built.
- Resolvers execute plans rather than building SQL directly.
- Indexes gate `where` and `orderBy` to keep query cost predictable.
- Auth is OIDC/JWKS only; issuer URLs must be HTTPS.

If you want a deeper dive, the rest of the explanation docs unpack each of these pieces.

---
# Related Docs

## Next steps
- [Schema handling](schema-handling.md)
- [Query planning](query-planning.md)
- [Resolver batching](resolver-batching.md)

## Reference
- [Configuration reference](../reference/configuration.md)

## Back
- [Explanation home](README.md)
- [Docs home](../README.md)
