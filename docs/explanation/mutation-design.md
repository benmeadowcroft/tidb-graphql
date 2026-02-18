# Mutation result design and rationale

Mutations in TiDB GraphQL return typed union results instead of returning a bare table object (or `null`).

This is a deliberate design choice to make mutation outcomes explicit, typed, and stable for clients.

## Why unions for mutations

A mutation can succeed, fail due to user input, fail due to constraints, fail due to permissions, or fail unexpectedly.

If all of those were represented as top-level GraphQL execution errors, clients would have to parse ad hoc strings and error extensions.

By returning union members in `data`, clients can:
- switch on `__typename`
- use inline fragments for exhaustive handling
- separate expected business outcomes from exceptional execution failures

## Why a shared `MutationError` interface

Typed error objects are useful, but new error types may be added over time.

The shared `MutationError` interface gives clients a forward-compatible fallback:

```graphql
... on MutationError { message }
```

Clients can still handle specific error types (`ConflictError`, `InputValidationError`, etc.) while keeping a safe default branch.

## Why success is wrapped (`CreateXxxSuccess`, etc.)

Success wrappers and error objects must coexist in the same union.

Wrapping success as a distinct object type:
- keeps union membership unambiguous
- allows per-operation success semantics
- avoids mixing table-object fields with error-object fields

The wrappers also encode operation-specific behavior:
- `CreateXxxSuccess { xxx: Xxx! }`
- `UpdateXxxSuccess { xxx: Xxx }` (nullable when row is not found)
- `DeleteXxxSuccess { id, <pk fields> }`

## Not-found behavior choices

- `update*`: row-not-found is `UpdateXxxSuccess` with a `null` entity field.
- `delete*`: row-not-found is `NotFoundError`.

Rationale:
- update without a matching row is often treated as an idempotent “no row changed” outcome
- delete benefits from explicit absence feedback in many client workflows

## Error detail and safety choices

Mutation errors are normalized into stable, safe error types.

Internal DB details (for example raw MySQL codes) are not exposed on GraphQL error payload types.

Rationale:
- keep API contracts stable
- avoid leaking storage-engine internals to clients
- preserve freedom to change backend mappings without breaking client schemas

## Naming and collision strategy

Mutation unions introduce global GraphQL type names (`MutationError`, `InputValidationError`, `CreateUserResult`, etc.).

To prevent conflicts with table-derived names:
- the schema build checks for collisions early and fails with an actionable message
- `naming.type_overrides` lets operators assign explicit GraphQL type names per table
- reserved mutation type names cannot be used as overrides

This keeps generated schemas predictable and avoids runtime surprises.

---
# Related Docs

## Next steps
- [Schema handling: build, snapshot, refresh](schema-handling.md)
- [Query planning as a seam](query-planning.md)

## Tutorial
- [Mutate data with typed results](../tutorials/mutation-basics.md)

## Reference
- [GraphQL schema mapping](../reference/graphql-schema.md)
- [Configuration reference](../reference/configuration.md)

## Back
- [Explanation home](README.md)
- [Docs home](../README.md)
