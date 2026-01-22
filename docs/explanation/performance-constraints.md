# Performance constraints as safety rails

The API is intentionally constrained. These constraints are not about limiting features, they are about keeping query performance somewhat predictable.

Two examples:
- WHERE clauses must use indexed columns.
- Query depth is capped.

The server also supports limits on estimated rows and query complexity. These are configurable guardrails to keep execution cost predictable.

These guardrails keep requests within a reasonable execution envelope, which matters when the GraphQL schema is auto-generated and you cannot audit every query shape ahead of time.

Indexes are also the lever you have to shape the generated schema. If you add or change indexes, you are effectively telling the system which filters and orderings are safe to expose. If you want `orderBy` on a field, make it an indexed field. If you do not want it, remove the index.

If you need to change the constraints, do it knowingly and measure the impact.

---
# Related Docs

## Next steps
- [Query basics](../tutorials/query-basics.md)
- [Ops guardrails](../tutorials/ops-guardrails.md)

## Reference
- [Configuration reference](../reference/configuration.md)

## Back
- [Explanation home](README.md)
- [Docs home](../README.md)
