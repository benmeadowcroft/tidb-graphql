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

- Collection query: `users(first, after, last, before, where, orderBy, orderByPolicy)` returns `UserConnection`.
- Primary key lookup: `user(id: ID!)` returns `User` (global Node ID).
- Primary key raw lookup: `user_by_databaseId(databaseId: BigInt!)` returns `User` (name depends on PK column).
- Unique index lookups: `user_by_email(email: String!)` returns `User`. Composite unique keys are `user_by_colA_colB(...)`.
- Vector search connections (when enabled and available): `searchUsersByEmbeddingVector(vector, metric, where, first, after)` returns `UserEmbeddingVectorConnection`.

Notes:
- `orderBy` uses clause-list syntax, for example:
  - `orderBy: [{ createdAt: DESC }, { databaseId: ASC }]`
- `orderByPolicy` controls prefix validation (`INDEX_PREFIX_ONLY` default, `ALLOW_NON_PREFIX` to relax prefix checks for exposed indexed fields).
- Missing PK columns are appended internally as ASC tie-breakers for stable pagination.
- `first` defaults to [`server.graphql_default_limit`](./configuration.md#server) (default `100`) when omitted.
- `first` and `last` are capped at `100`.

## Root mutation fields

For each table `users`:

- Create: `createUser(input: CreateUserInput!): CreateUserResult!`
- Update: `updateUser(id: ID!, set: UpdateUserSetInput): UpdateUserResult!`
- Delete: `deleteUser(id: ID!): DeleteUserResult!`

Notes:
- Mutations are not generated for views.
- Update/delete require the global Node `id: ID!`.
- Mutations return per-operation union types with success and typed error members.
- Success payloads are wrapped (`CreateXxxSuccess`, `UpdateXxxSuccess`, `DeleteXxxSuccess`).
- Errors are returned in `data` as union members (for example `InputValidationError`, `ConflictError`, `ConstraintError`, `PermissionError`, `NotFoundError`, `InternalError`), not as top-level GraphQL execution errors.
- All mutation error types implement the shared `MutationError` interface, so clients can use `... on MutationError { message }` as a forward-compatible fallback.

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

Relationship fields are generated for one-to-many, many-to-many, and edge-list relationships when the related table has a primary key. They accept `first`, `after`, `last`, `before`, `orderBy`, `orderByPolicy`, and `where` (target table for many-to-many, junction table for edge-list).
For tables without primary keys, these to-many connection fields are not generated.

Composite-key behavior:
- Many-to-many and edge-list relationship planning supports composite PK/FK mappings (multi-column joins and filters).
- Composite one-to-many reverse relationship generation is currently skipped.
- Skipped unsupported composite mappings emit a warning log during schema build/refresh with table, constraint, and column details.

### Connection types

Each connection provides:

- `edges { cursor node { ... } }`
- `nodes { ... }` (GitHub-style shortcut)
- `pageInfo { hasNextPage hasPreviousPage startCursor endCursor }`
- `totalCount` (lazy; filter-aware, cursor-agnostic)
- `aggregate { count, countDistinct, avg, sum, min, max }` (lazy; filter-aware, cursor-agnostic)

Connections support forward (`first`/`after`) and backward (`last`/`before`) pagination and use stable ordering based on indexed columns (default PK ASC).
`pageInfo` uses lightweight semantics: forward mode sets `hasPreviousPage` when `after` is provided; backward mode sets `hasNextPage` when `before` is provided.
For relationship connections, only forward first-page requests (no `after`, `before`, or `last`) are batched across parents to avoid N+1 lookups; cursor/backward pages run per-parent seek queries.
Cursor compatibility note: cursors encode the active `orderBy` columns and per-column directions. Changing `orderBy` invalidates existing cursors.

## Type mapping

SQL types are mapped to GraphQL scalars:

- `int`, `serial` -> `Int`
- `bigint` -> `BigInt` (custom scalar, serialized as a string)
- `float`, `double` -> `Float`
- `decimal`, `numeric` -> `Decimal` (custom scalar, serialized as a string)
- `bool`, `tinyint(1)` -> `Boolean`
- `json` -> `JSON` (custom scalar)
- `enum` -> GraphQL enum named `<SingularTable><Column>` (e.g., `users.status` -> `UserStatus`)
- `set` -> `[<SingularTable><Column>!]` (list of enum values)
- `date` -> `Date` (YYYY-MM-DD, UTC)
- `datetime`, `timestamp` -> `DateTime` (RFC 3339, UTC)
- `time` -> `Time` (HH:MM:SS[.fraction], TiDB range)
- `year` -> `Year` (YYYY)
- `blob`, `binary`, `varbinary` -> `Bytes` (RFC4648 base64, padded)
- `vector`, `vector(D)` -> `Vector` (list of finite floats)
- `char`, `text` -> `String`

UUID mapping is explicit via config (`type_mappings.uuid_columns`): matched SQL columns are exposed as `UUID` (canonical lowercase hyphenated form). For binary storage, canonical RFC byte order (`UUID_TO_BIN(x,0)`) is assumed.

Tinyint mapping is configurable via `type_mappings.tinyint1_boolean_columns` and `type_mappings.tinyint1_int_columns`.
When both patterns match the same column, `tinyint1_int_columns` takes precedence.

Breaking change note:
- Legacy filters using numeric booleans like `eq: 1` / `eq: 0` on `tinyint(1)` columns must be updated to `eq: true` / `eq: false`.

## Multi-database mode

When [`database.databases`](./configuration.md#multiple-databases) lists more than one entry (or a single entry with an explicit `namespace`), the GraphQL schema changes in the following ways.

### Type naming

Each table's GraphQL type name is prefixed with the PascalCase namespace alias followed by an underscore:

- `shop` database, table `orders` → type `Shop_Order` (singular) / connection `Shop_OrderConnection`
- `auth` database, table `users` → type `Auth_User` / connection `Auth_UserConnection`

Both the singular type name (e.g. `Shop_Order`) and the list/connection name use this form. The `GraphQLTypeName` and `GraphQLSingleTypeName` are identical in multi-db mode to keep mutations and queries consistent.

Per-database `naming.type_overrides` can still override the base name before the namespace prefix is applied.

### Root schema structure

Instead of table queries and mutations appearing directly on `Query`/`Mutation`, each namespace becomes a field that returns a wrapper object containing that namespace's fields:

```graphql
type Query {
  shop: Shop_Query!   # all shop-database queries
  auth: Auth_Query!   # all auth-database queries
  node(id: ID!): Node # global node lookup (always on root)
}

type Mutation {
  shop: Shop_Mutation!
  auth: Auth_Mutation!
}

type Shop_Query {
  orders(first: Int, after: String, where: Shop_OrderWhere, orderBy: [Shop_OrderOrderBy!]): Shop_OrderConnection!
  order(id: ID!): Shop_Order
  # ... other shop tables
}

type Shop_Mutation {
  createShop_Order(input: CreateShop_OrderInput!): CreateShop_OrderResult!
  updateShop_Order(id: ID!, set: UpdateShop_OrderSetInput): UpdateShop_OrderResult!
  deleteShop_Order(id: ID!): DeleteShop_OrderResult!
  # ... other shop tables
}
```

The `node(id: ID!)` global lookup remains on the root `Query` and resolves objects across all namespaces.

A namespace wrapper field resolves to a non-null empty object — child field resolvers are invoked normally.

### Cross-database relationships

Foreign keys that reference a table in a different SQL database produce a read-only many-to-one field on the source type. The inverse one-to-many field is added on the referenced type.

Restrictions apply to cross-database mutations:

| Operation | Cross-database? | Allowed |
|-----------|----------------|---------|
| Many-to-one connect | Yes | ✅ Allowed |
| One-to-many nested create | Yes | ❌ Blocked |
| One-to-many edge-list nested create | Yes | ❌ Blocked |
| Many-to-many connect | Yes | ❌ Blocked |

Blocked operations are silently omitted from the generated input types; no runtime error is produced.

### Single-database with namespace

A single entry in `database.databases` with no `namespace` set preserves the existing flat root schema — queries and mutations appear directly on `Query`/`Mutation` without any wrapper, and type names are not prefixed. This is the backward-compatible default for all existing configurations.

A single entry **with** an explicit `namespace` enables namespace-prefixed type names and the wrapper structure, even with only one database.

## Descriptions

Table and column comments are emitted as GraphQL descriptions on the corresponding types and fields when present.

## Filter inputs

Each table gets a `TableWhere` input type (see [Filters](./filters.md)). JSON columns are excluded from filter inputs.
`TableWhere` includes scalar column filters and single-hop relationship filters:

- To-many: `{ some: RelatedScalarWhere, none: RelatedScalarWhere }`
- To-one: `{ is: RelatedScalarWhere, isNull: Boolean }`
