# Query planning as a seam

The planner is a deliberate seam between GraphQL and SQL. It takes two inputs:

- the GraphQL AST from the incoming request
- the current introspection snapshot

It produces a SQL plan that resolvers execute. Resolvers do not assemble SQL directly.

This separation makes the system easier to reason about. Planning is where you can enforce rules (like indexed filters, order-by constraints, and depth/complexity limits) without spreading logic across resolvers. It also keeps query execution focused on correctness and batching rather than on query construction.

If you want to change how SQL is generated, the planner is where you do it.

---
# Related Docs

## Next steps
- [Performance constraints](performance-constraints.md)
- [Resolver batching](resolver-batching.md)

## Reference
- [Configuration reference](../reference/configuration.md)

## Back
- [Explanation home](README.md)
- [Docs home](../README.md)
