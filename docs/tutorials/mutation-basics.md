# Mutate data with typed results

This tutorial walks through create, update, and delete mutations using the typed mutation result unions. The examples use the same schema as the [first schema tutorial](./first-schema.md)

Goal: perform a full object lifecycle and handle mutation outcomes with inline fragments.

## 1) Understand the mutation shape

Mutations return union types, not raw table objects.

For `users`, the root fields look like:

- `createUser(input: CreateUserInput!): CreateUserResult!`
- `updateUser(id: ID!, set: UpdateUserSetInput): UpdateUserResult!`
- `deleteUser(id: ID!): DeleteUserResult!`

Each result can be:
- a success object (`CreateUserSuccess`, `UpdateUserSuccess`, `DeleteUserSuccess`)
- a typed error (`InputValidationError`, `ConflictError`, `ConstraintError`, `PermissionError`, `NotFoundError`, `InternalError`)

Use `__typename` and inline fragments.

## 2) Create a user

```graphql
mutation CreateUser {
  createUser(
    input: {
      fullName: "Casey Doe"
      email: "casey.doe@example.com"
      status: ACTIVE
    }
  ) {
    __typename
    ... on CreateUserSuccess {
      user {
        id
        databaseId
        fullName
        email
        status
      }
    }
    ... on InputValidationError {
      message
      field
    }
    ... on ConflictError {
      message
      conflictingField
    }
    ... on MutationError {
      message
    }
  }
}
```

On success, copy the returned `id` (global Node ID). You will use it for update and delete.

## 3) Update the user

```graphql
mutation UpdateUser($id: ID!) {
  updateUser(id: $id, set: { status: INACTIVE }) {
    __typename
    ... on UpdateUserSuccess {
      user {
        id
        databaseId
        fullName
        email
        status
      }
    }
    ... on InputValidationError {
      message
      field
    }
    ... on ConflictError {
      message
      conflictingField
    }
    ... on MutationError {
      message
    }
  }
}
```

Important behavior:
- `UpdateUserSuccess` can return `user: null` when the row is not found.
- Not-found on update is modeled as a success result with a null entity.

## 4) Delete the user

```graphql
mutation DeleteUser($id: ID!) {
  deleteUser(id: $id) {
    __typename
    ... on DeleteUserSuccess {
      id
      databaseId
    }
    ... on NotFoundError {
      message
    }
    ... on MutationError {
      message
    }
  }
}
```

Important behavior:
- Delete not-found returns `NotFoundError`.
- Delete success returns the deleted row identity (`id` plus primary key fields).

## 5) Recommended client handling pattern

Use this sequence in clients:

1. Read `__typename`.
2. Handle the explicit success type for that mutation.
3. Handle specific error types you care about (`ConflictError`, `InputValidationError`, etc.).
4. Keep a fallback on `MutationError { message }` for future error types.

This gives exhaustive, type-safe handling while keeping expected mutation failures in the GraphQL `data` payload.

---
# Related Docs

## Next steps
- [Query with filters and unique keys](query-basics.md)
- [Vector search with embeddings](vector-search.md)

## Explanation
- [Mutation result design and rationale](../explanation/mutation-design.md)

## Reference
- [GraphQL schema mapping](../reference/graphql-schema.md)
- [Configuration reference](../reference/configuration.md)

## Back
- [Tutorials home](README.md)
- [Docs home](../README.md)
