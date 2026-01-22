# GraphQL schema mapping

This section describes how the GraphQL schema is derived from the TiDB schema.

## Naming conventions

- Tables become GraphQL types using PascalCase.
  - `order_items` -> `OrderItems`
- Columns become fields using camelCase.
  - `created_at` -> `createdAt`

## Root query fields

For each table `users`:

- List query: `users(limit, offset, where, orderBy)` returns `[User]`.
- Primary key lookup: `users_by_pk(id: ID!)` returns `User`.
- Unique index lookups: `users_by_email(email: String!)` returns `User`.

Notes:
- `limit` default is `100`.
- `offset` default is `0`.
- `orderBy` only accepts indexed fields (see `docs/reference/filters.md`).

## Relationships

Foreign keys create relationship fields on both sides:

- Many-to-one: singularized target table name.
  - `orders.user_id` -> `order.user`
- One-to-many: pluralized source table name.
  - `users` -> `user.orders`

Pluralization is intentionally simple (adds or removes trailing `s`).

One-to-many fields accept the same `limit`, `offset`, and `orderBy` arguments as list queries.

## Type mapping

SQL types are mapped to GraphQL scalars:

- `int`, `serial` -> `Int`
- `float`, `double`, `decimal` -> `Float`
- `bool` -> `Boolean`
- `char`, `text`, `blob`, `binary`, `date`, `time`, `json`, `enum`, `set` -> `String`

## Filter inputs

Each table gets a `TableWhere` input type (see `docs/reference/filters.md`). JSON columns are excluded from filter inputs.
