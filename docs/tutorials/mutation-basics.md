# Modifying data with mutations

This tutorial shows how to create, update, and delete data using GraphQL mutations. You'll learn the basics of mutation syntax, transaction behavior, and error handling.

**Goal:** Learn to create, update, and delete data using GraphQL mutations

**Prerequisites:**
- Completed [Your first schema in five minutes](./first-schema.md)
- Sample data loaded from [sample-data.sql](./sample-data.sql)

## 1) Understanding mutations vs queries

In GraphQL:
- **Queries** read data without side effects
- **Mutations** modify data (create, update, delete)

Mutations execute sequentially within a transaction. If any mutation fails, all changes are rolled back.

## 2) Create your first record

Let's create a new category using the `createCategory` mutation:

```graphql
mutation {
  createCategory(input: {
    name: "Home & Garden"
    description: "Home improvement and gardening items"
  }) {
    id
    name
    description
    createdAt
  }
}
```

**What's happening:**
- `input` object contains the fields we want to set
- `id` is auto-generated (auto-increment primary key)
- The mutation returns the created row with all requested fields
- `createdAt` gets its value from the database DEFAULT

**Response:**
```json
{
  "data": {
    "createCategory": {
      "id": 7,
      "name": "Home & Garden",
      "description": "Home improvement and gardening items",
      "createdAt": "2024-01-30T10:15:30Z"
    }
  }
}
```

## 3) Update an existing record

Update a product's price:

```graphql
mutation {
  updateProduct(id: 1, set: {priceCents: 1500}) {
    id
    name
    priceCents
    lastUpdated
  }
}
```

**What's happening:**
- `id: 1` identifies which row to update (primary key)
- `set` contains the fields to update
- Fields not in `set` are not modified
- The mutation returns the updated row with current database values

**Response:**
```json
{
  "data": {
    "updateProduct": {
      "id": 1,
      "name": "Graphite Notebook",
      "priceCents": 1500,
      "lastUpdated": "2024-01-30T10:20:45Z"
    }
  }
}
```

## 4) Update non-existent row

What happens if you try to update a row that doesn't exist?

```graphql
mutation {
  updateProduct(id: 99999, set: {priceCents: 1000}) {
    id
    name
  }
}
```

**Response:**
```json
{
  "data": {
    "updateProduct": null
  }
}
```

**What's happening:**
- No error is returned
- The mutation returns `null` when no rows are affected
- This lets you distinguish between "updated successfully" and "row doesn't exist"

## 5) Delete a record

Delete a category:

```graphql
mutation {
  deleteCategory(id: 7) {
    id
  }
}
```

**What's happening:**
- `id: 7` identifies which row to delete
- Delete mutations return only the primary key fields
- Cannot return other fields because the row has been deleted

**Response:**
```json
{
  "data": {
    "deleteCategory": {
      "id": 7
    }
  }
}
```

**Try deleting a non-existent row:**
```graphql
mutation {
  deleteCategory(id: 99999) {
    id
  }
}
```

Returns `null` (no rows affected).

---
# Related Docs

## Next steps
- [Operations with guardrails](ops-guardrails.md)
- [Handle mutation errors](../how-to/handle-mutation-errors.md)

## Reference
- [GraphQL schema mapping](../reference/graphql-schema.md)

## Further reading
- [Mutation transaction handling](../explanation/mutation-transactions.md)

## Back
- [Tutorials home](README.md)
- [Docs home](../README.md)
