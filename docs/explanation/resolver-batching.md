# Resolver batching to avoid N+1

GraphQL makes it easy to express nested data, which makes it easy to accidentally cause N+1 queries.

TiDB GraphQL avoids that by batching one-to-many lookups. When a resolver needs related rows for many parents, it collects the parent keys and performs a single query using an IN clause. That result is then fanned back out to the parent objects.

This is not a new idea, but it is an important one. It keeps the API expressive without letting a single query explode into dozens of database round trips.

The specific batching strategy can evolve (for example, using window functions or other plan shapes). The key invariant is that one-to-many lookups are grouped and executed in bulk.

---
# Related Docs

## Next steps
- [Performance constraints](performance-constraints.md)
- [Query planning](query-planning.md)

## Reference
- [Configuration reference](../reference/configuration.md)

## Back
- [Explanation home](README.md)
- [Docs home](../README.md)
