# Vector search with review embeddings

This tutorial shows how to use the `Vector` scalar and vector search root fields using the tutorial dataset.

Goal: run semantic search queries over `product_reviews` and paginate results with cursors.

## Prerequisites

- Complete [Your first schema in five minutes](./first-schema.md), or run:

```bash
mysql --host=127.0.0.1 --port=4000 --user=root < docs/tutorials/sample-data.sql
mysql --host=127.0.0.1 --port=4000 --user=root < docs/tutorials/sample-data-vectors.sql
```

Quickstart already loads both files and configures vector search field exposure without requiring a vector index.

## 1) Verify vector search root field exists

```graphql
{
  __schema {
    queryType {
      fields {
        name
      }
    }
  }
}
```

Look for `searchProductReviewsByEmbeddingVector`.

## 2) Run a basic vector search query

`Vector` inputs are numeric lists. In this sample dataset, review embeddings are 8-dimensional.

```graphql
query {
  searchProductReviewsByEmbeddingVector(
    vector: [0.14, 0.18, 0.06, 0.09, 0.21, 0.90, 0.86, 0.08]
    metric: COSINE
    first: 5
  ) {
    nodes {
      databaseId
      rating
      reviewText
      createdAt
      product {
        sku
        name
      }
      user {
        fullName
      }
    }
  }
}
```

This query intentionally uses only `nodes` so it returns just the row objects.

## 3) Add a structured filter (`where`)

You can combine semantic similarity with regular filters:

```graphql
query {
  searchProductReviewsByEmbeddingVector(
    vector: [0.09, 0.78, 0.84, 0.11, 0.04, 0.06, 0.05, 0.05]
    metric: COSINE
    first: 5
    where: { rating: { eq: THUMBS_DOWN } }
  ) {
    nodes {
      databaseId
      rating
      reviewText
    }
  }
}
```

## 4) Page forward with `first` + `after`

When you need cursor and ranking metadata, use `edges` with its `node` field.

Page 1:

```graphql
query {
  searchProductReviewsByEmbeddingVector(
    vector: [0.10, 0.85, 0.90, 0.12, 0.05, 0.07, 0.05, 0.06]
    metric: COSINE
    first: 3
  ) {
    edges {
      cursor
      rank
      node {
        databaseId
        reviewText
      }
    }
    pageInfo {
      hasNextPage
      endCursor
    }
  }
}
```

Page 2:

```graphql
query {
  searchProductReviewsByEmbeddingVector(
    vector: [0.10, 0.85, 0.90, 0.12, 0.05, 0.07, 0.05, 0.06]
    metric: COSINE
    first: 3
    after: "<endCursor-from-page-1>"
  ) {
    edges {
      rank
      distance
      node {
        databaseId
        reviewText
      }
    }
    pageInfo {
      hasNextPage
      endCursor
    }
  }
}
```

## 5) Compare distance metrics

Run the same query with `COSINE` and `L2` to compare ranking behavior:

```graphql
query {
  searchProductReviewsByEmbeddingVector(
    vector: [0.10, 0.85, 0.90, 0.12, 0.05, 0.07, 0.05, 0.06]
    metric: L2
    first: 5
  ) {
    edges {
      rank
      distance
      node {
        databaseId
        reviewText
      }
    }
  }
}
```

## Troubleshooting

- If vector search fields do not appear, confirm:
  - your table has a `VECTOR` column
  - `server.search.vector_require_index` is set appropriately for your environment
- If you see vector dimension errors, verify the input vector length matches the column dimension.

---
# Related Docs

## Next steps
- [Query basics](query-basics.md)
- [Operations with guardrails](ops-guardrails.md)

## Reference
- [GraphQL schema reference](../reference/graphql-schema.md)
- [Configuration reference](../reference/configuration.md)

## Back
- [Tutorials home](README.md)
- [Docs home](../README.md)
