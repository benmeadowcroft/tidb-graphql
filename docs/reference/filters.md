# Filter language

Filters are expressed with a `where` input object on list fields and relationship connection fields.
JSON columns are excluded from `where` inputs.
If a table has a primary key column named `id`, it is exposed as `databaseId` in filter inputs.

## Logical operators

- `AND`: array of filter objects
- `OR`: array of filter objects

Example:

```graphql
{
  users(where: { AND: [{ status: { eq: "active" } }, { createdAt: { gte: "2024-01-01T00:00:00Z" } }] }) {
    id
    email
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
- Set: `has`, `hasAnyOf`, `hasAllOf`, `hasNoneOf`, `eq`, `ne`, `isNull`

Example:

```graphql
{
  users(where: { email: { like: "%@example.com" }, status: { ne: INACTIVE } }) {
    id
    email
  }
}
```

Set examples:

```graphql
{
  products(where: { tags: { has: FEATURED } }) {
    id
    name
    tags
  }
}
```

```graphql
{
  products(where: { tags: { hasAnyOf: [FEATURED, SEASONAL] } }) {
    id
    name
    tags
  }
}
```

```graphql
{
  products(where: { tags: { eq: [FEATURED, NEW] } }) {
    id
    name
    tags
  }
}
```

Empty-list semantics for set operators:

- `hasAnyOf([])` => matches no rows
- `hasAllOf([])` => matches all rows
- `hasNoneOf([])` => matches all rows

## Indexed column requirement

If you use `where`, at least one referenced column must be indexed. This is a guardrail to prevent unbounded scans. The error message lists available indexed columns.

## orderBy rules

- `orderBy` is a single-field input object with direction `ASC` or `DESC`.
- Allowed fields come from indexed column prefixes.

Example:

```graphql
{
  users(orderBy: { createdAt: DESC }) {
    id
    createdAt
  }
}
```
