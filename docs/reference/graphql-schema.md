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

For each table `users`:

- Create: `createUser(input: CreateUserInput!): User`
- Update: `updateUser(id: ID!, set: UpdateUserSetInput): User`
- Delete: `deleteUser(id: ID!): DeleteUserPayload`

Notes:
- Mutations are not generated for views.
- Update/delete require primary key arguments (composite keys are multiple args).
- Create/update return the row directly using the selection set on the table type.
- Delete returns the primary key fields in `DeleteXPayload`.

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
- `enum` -> GraphQL enum named `<SingularTable><Column>` (e.g., `users.status` -> `UserStatus`)
- `char`, `text`, `blob`, `binary`, `date`, `time`, `set` -> `String`

## Filter inputs

Each table gets a `TableWhere` input type (see [Filters](./filters.md)). JSON columns are excluded from filter inputs.
