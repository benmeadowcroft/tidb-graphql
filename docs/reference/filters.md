# Filter language

Filters are expressed with a `where` input object on list fields and one-to-many relationship fields.
JSON columns are excluded from `where` inputs.

## Logical operators

- `AND`: array of filter objects
- `OR`: array of filter objects

Example:

```graphql
{
  users(where: { AND: [{ status: { eq: "active" } }, { createdAt: { gte: "2024-01-01" } }] }) {
    id
    email
  }
}
```

## Column operators

Supported operators depend on the column type:

- Numeric: `eq`, `ne`, `lt`, `lte`, `gt`, `gte`, `in`, `notIn`, `isNull`
- String: `eq`, `ne`, `lt`, `lte`, `gt`, `gte`, `in`, `notIn`, `like`, `notLike`, `isNull`
- Boolean: `eq`, `ne`, `isNull`

Example:

```graphql
{
  users(where: { email: { like: "%@example.com" }, status: { ne: "inactive" } }) {
    id
    email
  }
}
```

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
