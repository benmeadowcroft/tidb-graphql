# Filter language

Filters are expressed with a `where` input object on connection collection fields.
JSON columns are excluded from `where` inputs.
If a table has a primary key column named `id`, it is exposed as `databaseId` in filter inputs.

## Logical operators

- `AND`: array of filter objects
- `OR`: array of filter objects

Example:

```graphql
{
  users(where: { AND: [{ status: { eq: ACTIVE } }, { createdAt: { gte: "2024-01-01T00:00:00Z" } }] }) {
    nodes {
      id
      email
    }
  }
}
```

## Column operators

Supported operators depend on the column type:

- Numeric (`Int`, `BigInt`, `Float`): `eq`, `ne`, `lt`, `lte`, `gt`, `gte`, `in`, `notIn`, `isNull`
- String: `eq`, `ne`, `lt`, `lte`, `gt`, `gte`, `in`, `notIn`, `like`, `notLike`, `isNull`
- Boolean: `eq`, `ne`, `isNull`
- Enum: `eq`, `ne`, `in`, `notIn`, `isNull`
- Date: `eq`, `ne`, `lt`, `lte`, `gt`, `gte`, `in`, `notIn`, `isNull`
- DateTime: `eq`, `ne`, `lt`, `lte`, `gt`, `gte`, `in`, `notIn`, `isNull`
- Bytes: `eq`, `ne`, `in`, `notIn`, `isNull` (base64 values)
- UUID: `eq`, `ne`, `in`, `notIn`, `isNull`
- Set: `has`, `hasAnyOf`, `hasAllOf`, `hasNoneOf`, `eq`, `ne`, `isNull`

Example:

```graphql
{
  users(where: { email: { like: "%@example.com" }, status: { ne: INACTIVE } }) {
    nodes {
      id
      email
    }
  }
}
```

Boolean migration note:
- `tinyint(1)` columns are exposed as GraphQL `Boolean`.
- Use boolean literals in filters (`eq: true` / `eq: false`), not numeric values (`1` / `0`).

Bytes example:

```graphql
{
  files(where: { payload: { eq: "SGVsbG8=" } }) {
    nodes {
      id
      payload
    }
  }
}
```

Set examples:

```graphql
{
  products(where: { tags: { has: FEATURED } }) {
    nodes {
      id
      name
      tags
    }
  }
}
```

```graphql
{
  products(where: { tags: { hasAnyOf: [FEATURED, SEASONAL] } }) {
    nodes {
      id
      name
      tags
    }
  }
}
```

```graphql
{
  products(where: { tags: { eq: [FEATURED, NEW] } }) {
    nodes {
      id
      name
      tags
    }
  }
}
```

Empty-list semantics for set operators:

- `hasAnyOf([])` => matches no rows
- `hasAllOf([])` => matches all rows
- `hasNoneOf([])` => matches all rows

## Indexed column requirement

If you use `where`, at least one referenced column must be indexed. This is a guardrail to prevent unbounded scans. The error message lists available indexed columns.
For relationship-aware filters, this validation is applied per referenced table path.

## Relationship operators

Relationship fields in `where` support single-hop traversal:

- To-many relationships: `some`, `none`
- To-one relationships: `is`, `isNull`

Examples:

```graphql
{
  users(where: { posts: { some: { published: { eq: true } } } }) {
    nodes {
      id
      username
    }
  }
}
```

```graphql
{
  posts(where: { user: { is: { username: { eq: "alice" } } } }) {
    nodes {
      id
      title
    }
  }
}
```

```graphql
{
  posts(where: { user: { isNull: true } }) {
    nodes {
      id
    }
  }
}
```

Notes:

- `isNull: true` compiles to `NOT EXISTS`; `isNull: false` compiles to `EXISTS`.
- `is` and `isNull` cannot be used together in the same relationship block.
- Single-hop only in this phase: nested relationship traversal inside relationship filters is not supported.

## orderBy rules

- `orderBy` is a list of single-field clauses: `[{ createdAt: DESC }, { databaseId: ASC }]`.
- `orderByPolicy` controls prefix validation for explicit clauses:
  - `INDEX_PREFIX_ONLY` (default): explicit clauses must match an indexed left-prefix.
  - `ALLOW_NON_PREFIX`: allows non-prefix ordering for currently exposed indexed fields.
- Each clause must contain exactly one field.
- Duplicate fields across clauses are rejected.
- Missing primary key columns are appended internally as deterministic ASC tie-breakers.
- Mixed clause directions are supported, but can require additional sorting depending on optimizer/index behavior.
- Legacy `orderBy: { field: DIR }` syntax is not supported.
- Cursors created before this format change are not compatible with the v2 cursor format.

Example:

```graphql
{
  users(orderByPolicy: ALLOW_NON_PREFIX, orderBy: [{ createdAt: DESC }]) {
    nodes {
      id
      createdAt
    }
  }
}
```
