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

- List query: `users(limit, offset, where, orderBy)` returns `[User!]!`.
- Connection query: `usersConnection(first, after, where, orderBy)` returns `UserConnection`.
- Primary key lookup: `user(id: ID!)` returns `User` (global Node ID).
- Primary key raw lookup: `user_by_databaseId(databaseId: BigInt!)` returns `User` (name depends on PK column).
- Unique index lookups: `user_by_email(email: String!)` returns `User`. Composite unique keys are `user_by_colA_colB(...)`.
- Aggregate query: `users_aggregate(...)` returns `UsersAggregate`.

Notes:
- `limit` default is configurable (default `100`, via [`server.graphql_list_limit_default`](./configuration.md#server)).
- `offset` default is `0`.
- `orderBy` only accepts indexed fields and will error otherwise (see [Filters](./filters.md)).
- `first` defaults to `25` and is capped at `100` for connections.

## Root mutation fields

For each table `users`:

- Create: `createUser(input: CreateUserInput!): User`
- Update: `updateUser(id: ID!, set: UpdateUserSetInput): User`
- Delete: `deleteUser(id: ID!): DeleteUserPayload`

Notes:
- Mutations are not generated for views.
- Update/delete require the global Node `id: ID!`.
- Create/update return the row directly using the selection set on the table type.
- Delete returns the primary key fields and `id` in `DeleteXPayload`.

## Node interface and global IDs

Tables with primary keys implement the `Node` interface and expose an opaque `id: ID!` field.

The schema includes a `node(id: ID!): Node` query that resolves any object by its global ID.

Primary key columns named `id` are renamed to `databaseId` in the GraphQL schema to avoid
conflict with the Node `id` field.

Global IDs are base64-encoded JSON arrays of the form `["TypeName", pk1, pk2, ...]`.

## Relationships

Foreign keys create relationship fields on both sides:

- Many-to-one: singularized target table name.
  - `orders.user_id` -> `order.user`
- One-to-many: pluralized source table name.
  - `users` -> `user.orders`

Pluralization uses the [Inflection library](https://github.com/jinzhu/inflection) (with naming overrides).

Many-to-one fields remain nullable even when the FK column is NOT NULL, because role-based access can hide the related row.

One-to-many fields accept the same `limit`, `offset`, and `orderBy` arguments as list queries.
Relationship connection fields (`<rel>Connection`) are also generated for one-to-many, many-to-many, and edge-list relationships when the related table has a primary key. They accept `first`, `after`, `orderBy`, and `where` (target table for many-to-many, junction table for edge-list).
List fields never return `null` and never contain `null` items.
If role-based restrictions hide a nested type, the nested field error can null out the parent list because list items are non-null.

### Connection types

Each connection provides:

- `edges { cursor node { ... } }`
- `nodes { ... }` (GitHub-style shortcut)
- `pageInfo { hasNextPage hasPreviousPage startCursor endCursor }`
- `totalCount` (lazy; filter-aware, cursor-agnostic)

Connections are forward-only (`first`/`after`) and use stable ordering based on indexed columns (default PK ASC).
For relationship connections, the first page (`after` omitted) is batched across parents to avoid N+1 lookups; cursor pages run per-parent seek queries.

## Type mapping

SQL types are mapped to GraphQL scalars:

- `int`, `serial` -> `Int`
- `bigint` -> `BigInt` (custom scalar, serialized as a string)
- `float`, `double` -> `Float`
- `decimal`, `numeric` -> `Decimal` (custom scalar, serialized as a string)
- `bool` -> `Boolean`
- `json` -> `JSON` (custom scalar)
- `enum` -> GraphQL enum named `<SingularTable><Column>` (e.g., `users.status` -> `UserStatus`)
- `set` -> `[<SingularTable><Column>!]` (list of enum values)
- `date` -> `Date` (YYYY-MM-DD, UTC)
- `datetime`, `timestamp` -> `DateTime` (RFC 3339, UTC)
- `time` -> `Time` (HH:MM:SS[.fraction], TiDB range)
- `year` -> `Year` (YYYY)
- `blob`, `binary`, `varbinary` -> `Bytes` (RFC4648 base64, padded)
- `char`, `text` -> `String`

UUID mapping is explicit via config (`type_mappings.uuid_columns`): matched SQL columns are exposed as `UUID` (canonical lowercase hyphenated form). For binary storage, canonical RFC byte order (`UUID_TO_BIN(x,0)`) is assumed.

## Descriptions

Table and column comments are emitted as GraphQL descriptions on the corresponding types and fields when present.

## Filter inputs

Each table gets a `TableWhere` input type (see [Filters](./filters.md)). JSON columns are excluded from filter inputs.
