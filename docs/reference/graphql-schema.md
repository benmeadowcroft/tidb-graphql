# GraphQL schema mapping

This section describes how the GraphQL schema is derived from the TiDB schema.

## Naming conventions

- Tables become GraphQL types using PascalCase.
  - `order_items` -> `OrderItems`
- Columns become fields using camelCase.
  - `created_at` -> `createdAt`
- Pluralization and singularization use the [Inflection library](https://github.com/jinzhu/inflection), with optional overrides from [naming config](./configuration.md#naming).
- Relationship field name collisions get suffixes: many-to-one uses `Ref`, all others use `Rel`.

## Root query fields

For each table `users`:

- List query: `users(limit, offset, where, orderBy)` returns `[User]`.
- Primary key lookup: `user(id: ID!)` returns `User`.
- Unique index lookups: `user_by_email(email: String!)` returns `User`. Composite unique keys are `user_by_colA_colB(...)`.
- Aggregate query: `users_aggregate(...)` returns `UsersAggregate`.

Notes:
- `limit` default is configurable (default `100`, via [`server.graphql_list_limit_default`](./configuration.md#server)).
- `offset` default is `0`.
- `orderBy` only accepts indexed fields and will error otherwise (see [Filters](./filters.md)).

## Root mutation fields

For each table with a primary key (excluding views and pure junction tables):

### Create mutation

- Field name: `create` + SingularTableName (e.g., `createUser`)
- Arguments: `input: CreateUserInput!` (NonNull required)
- Return type: table's GraphQL type (e.g., `User`)
- Behavior: Executes INSERT, re-queries by PK, returns full row with requested fields

Input type includes all insertable columns (non-generated, not in `deny_mutation_columns`).
Required fields (NonNull): columns that are NOT NULL, have no DEFAULT, not auto-increment, not generated.

### Update mutation

- Field name: `update` + SingularTableName (e.g., `updateUser`)
- Arguments: PK columns (one or more, all NonNull) + `set: UpdateUserSetInput` (nullable)
- Return type: table's GraphQL type (e.g., `User`) or null if not found
- Behavior: Executes UPDATE by PK, re-queries, returns updated row

Set input includes updatable columns (non-PK, non-generated, not in `deny_mutation_columns`).
All set fields are nullable (optional).
Empty/null set is a no-op that returns current row.

### Delete mutation

- Field name: `delete` + SingularTableName (e.g., `deleteUser`)
- Arguments: PK columns (one or more, all NonNull)
- Return type: `DeleteUserPayload` (contains only PK fields)
- Behavior: Executes DELETE by PK, returns PK values if deleted, null if not found

Cannot re-query deleted row, so return type only includes PK for confirmation.

### Composite primary keys

For tables with composite PKs, all PK columns become separate arguments:

```graphql
updateProductCategory(
  productId: Int!
  categoryId: Int!
  set: UpdateProductCategorySetInput
): ProductCategory

deleteProductCategory(
  productId: Int!
  categoryId: Int!
): DeleteProductCategoryPayload
```

### Transaction semantics

Each mutation request executes in a transaction. Multiple mutations in one request execute sequentially and commit atomically (all succeed or all roll back).

See [Mutation reference](./mutations.md) for complete details.

## Relationships

Foreign keys create relationship fields on both sides:

- Many-to-one: singularized target table name.
  - `orders.user_id` -> `order.user`
- One-to-many: pluralized source table name.
  - `users` -> `user.orders`

Pluralization uses the [Inflection library](https://github.com/jinzhu/inflection) (with naming overrides).

One-to-many fields accept the same `limit`, `offset`, and `orderBy` arguments as list queries.

## Type mapping

SQL types are mapped to GraphQL scalars:

- `int`, `serial` -> `Int`
- `float`, `double`, `decimal` -> `Float`
- `bool` -> `Boolean`
- `json` -> `JSON` (custom scalar)
- `char`, `text`, `blob`, `binary`, `date`, `time`, `enum`, `set` -> `String`

## Filter inputs

Each table gets a `TableWhere` input type (see [Filters](./filters.md)). JSON columns are excluded from filter inputs.
