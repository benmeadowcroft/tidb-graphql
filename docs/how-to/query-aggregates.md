# Query aggregates

Goal: use connection aggregates to summarize rows without writing custom SQL.

Aggregates are available through `Connection.aggregate`. Use them on root connections like `orders` and on relationship connections like `user.orders`.

## Count matching rows

Use `count` when you only need the number of rows:

```graphql
query {
  orders(where: { status: { eq: PAID } }) {
    aggregate {
      count
    }
  }
}
```

## Compute totals and averages

Use `sum` and `avg` on numeric fields:

```graphql
query {
  orders(where: { status: { in: [PAID, SHIPPED] } }) {
    aggregate {
      count
      sum {
        total
      }
      avg {
        total
      }
    }
  }
}
```

## Find earliest and latest values

Use `min` and `max` on comparable fields:

```graphql
query {
  orders {
    aggregate {
      min {
        createdAt
        total
      }
      max {
        createdAt
        total
      }
    }
  }
}
```

## Count distinct values

Use `countDistinct` to count unique values for comparable fields:

```graphql
query {
  users {
    aggregate {
      count
      countDistinct {
        status
        email
      }
    }
  }
}
```

## Fetch a page and a summary together

You can request page data and aggregate data in the same connection. The aggregate uses the filtered connection dataset; pagination arguments choose the page, not the aggregate scope.

```graphql
query {
  products(where: { price: { gte: 25 } }, orderBy: [{ price: DESC }], first: 5) {
    nodes {
      sku
      name
      price
    }
    pageInfo {
      hasNextPage
      endCursor
    }
    aggregate {
      count
      min {
        price
      }
      max {
        price
      }
      avg {
        price
      }
    }
  }
}
```

## Aggregate relationship connections

Relationship connections expose the same `aggregate` field:

```graphql
query {
  user_by_email(email: "ava.smith@example.com") {
    fullName
    orders {
      aggregate {
        count
        sum {
          total
        }
        max {
          createdAt
        }
      }
    }
  }
}
```

## When results look missing

- `avg` and `sum` only appear for numeric fields (`Int`, `BigInt`, `Float`, or `Decimal`).
- `countDistinct`, `min`, and `max` only appear for comparable fields. `JSON` and `Vector` fields are excluded.
- Use the GraphiQL Docs panel or schema introspection to see the aggregate fields generated for your current database.

---
# Related Docs

## Next steps
- [Query basics](../tutorials/query-basics.md)
- [Refresh schema safely](schema-refresh.md)

## Reference
- [GraphQL schema mapping](../reference/graphql-schema.md#aggregate-fields)
- [Filter language](../reference/filters.md)

## Further reading
- [Query planning](../explanation/query-planning.md)
- [Resolver batching](../explanation/resolver-batching.md)

## Back
- [How-to home](README.md)
- [Docs home](../README.md)
