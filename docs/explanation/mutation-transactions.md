# Mutation transaction handling

This document explains why mutations execute within database transactions and the design decisions behind this approach.

## Why transactions

GraphQL mutations are sequential by specification. When multiple mutations appear in a single request, the GraphQL executor runs them in order. If any mutation fails, prior mutations should roll back to prevent partial state changes.

Without transactions, a failed mutation could leave the database in an inconsistent state:

```graphql
mutation {
  category: createCategory(input: {name: "Books"}) { id }
  product: createProduct(input: {
    name: "Novel"
    categoryId: 999  # Invalid FK - does not exist
  }) { id }
}
```

Without transactions:
1. Category created successfully (id=10)
2. Product creation fails (invalid foreign key)
3. **Result:** Orphaned category exists in database

With transactions:
1. Begin transaction
2. Category created (id=10, uncommitted)
3. Product creation fails
4. **Rollback:** Category is not persisted
5. **Result:** Database unchanged

Transactions provide ACID guarantees:
- **Atomicity:** All mutations succeed or all fail
- **Consistency:** Database constraints are enforced
- **Isolation:** Mutations see a consistent snapshot
- **Durability:** Committed changes persist

## How it works

The mutation context (`MutationContext`) wraps a database transaction and tracks error state.

### Transaction lifecycle

**1. Begin transaction**

Middleware detects a mutation operation and begins a database transaction before executing any resolvers:

```go
tx, err := executor.BeginTx(ctx)
mc := resolver.NewMutationContext(tx)
ctx = resolver.WithMutationContext(ctx, mc)
```

**2. Execute mutations sequentially**

Each mutation resolver:
- Retrieves the mutation context from the request context
- Executes SQL using the transaction (`mc.Tx()`)
- Re-queries within the same transaction for return values
- Marks error if mutation fails

```go
result, err := mc.Tx().ExecContext(ctx, query.SQL, query.Args...)
if err != nil {
    mc.MarkError()
    return nil, err
}
```

**3. Re-query within transaction**

For create and update mutations, the system re-queries the database to fetch current values:

```go
row, err := selectRowByPK(ctx, table, pkValues, mc.Tx())
```

Using the same transaction ensures:
- Read-your-own-writes consistency
- Generated columns have current values
- Transaction hasn't committed yet (can still roll back)

**4. Finalize (commit or rollback)**

After all mutations execute, middleware finalizes the transaction:

```go
err := mc.Finalize()
```

If any mutation called `mc.MarkError()`, the transaction rolls back. Otherwise, it commits.

The finalization is deferred to ensure it runs even if a panic occurs.

## Why re-query after mutations?

After an INSERT or UPDATE, the system executes a SELECT to fetch the current row state. This adds an extra database round trip. Why?

### The database is the source of truth

When a mutation executes, several things can happen that change data:

**Generated columns:**
```sql
CREATE TABLE users (
  id BIGINT AUTO_INCREMENT,
  email VARCHAR(255),
  email_domain VARCHAR(255) AS (SUBSTRING_INDEX(email, '@', -1)) VIRTUAL
);
```

After `INSERT INTO users (email) VALUES ('alice@example.com')`, we need to query to get `email_domain`.

**DEFAULT values:**
```sql
CREATE TABLE orders (
  status VARCHAR(32) DEFAULT 'pending',
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

After `INSERT INTO orders (user_id) VALUES (1)`, we need to query to get `status` and `created_at`.

**Triggers:**
```sql
CREATE TRIGGER update_inventory AFTER INSERT ON orders
FOR EACH ROW UPDATE inventory SET quantity = quantity - NEW.quantity;
```

Triggers may modify the inserted row or other rows. Re-querying ensures we return actual database state.

**Type coercion:**
```sql
INSERT INTO products (price) VALUES ('19.99');
```

Database may coerce types. Re-querying ensures we return the stored value, not the input value.

**Constraints:**
```sql
CHECK (price > 0)
```

Constraints validate data. Re-querying ensures constraints passed and data was stored as expected.

### Trade-offs

**Cost:** Extra SELECT query per create/update mutation

**Benefit:** Accuracy and consistency
- Clients receive actual database state
- Generated columns have correct values
- DEFAULT values are visible
- Trigger side effects are accounted for
- Type coercion is applied

The design prioritizes correctness over performance. For most applications, the extra query is negligible compared to network latency and GraphQL parsing overhead.

### Performance optimization

If performance is critical:
- Batch mutations in a single request (one transaction, multiple mutations)
- Use database connection pooling to reduce transaction overhead
- Ensure queries use indexed lookups (primary key)
- Consider database-level optimizations (query cache, read replicas)

Re-querying by primary key is fast (indexed lookup). The overhead is typically < 1ms.

## Why no optimistic locking?

Some GraphQL APIs implement optimistic concurrency control with version fields:

```graphql
mutation {
  updateProduct(
    id: 5
    version: 10
    set: {price: 1999}
  ) {
    id
    version  # Returns 11
  }
}
```

If `version` doesn't match, the update fails (conflict).

TiDB GraphQL does not implement this pattern. Why?

### Design philosophy: database-first

The system relies on database-level mechanisms for concurrency control:
- **Transactions:** Provide isolation guarantees (READ COMMITTED, REPEATABLE READ)
- **Constraints:** Enforce uniqueness, foreign keys, checks
- **Locks:** Row locks prevent concurrent modifications

Adding application-level versioning would:
- Duplicate database functionality
- Increase complexity (version management, conflict detection)
- Limit flexibility (not all use cases need versioning)

### Alternatives for conflict detection

If your application needs optimistic locking:

**1. Database triggers:**
```sql
CREATE TRIGGER check_version BEFORE UPDATE ON products
FOR EACH ROW BEGIN
  IF NEW.version != OLD.version + 1 THEN
    SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Version conflict';
  END IF;
END;
```

**2. Application logic:**
```graphql
query {
  product(id: 5) { id version price }
}

mutation {
  updateProduct(id: 5, set: {
    version: 11  # Include version in update
    price: 1999
  }) {
    id
    version
  }
}
```

Check returned version matches expected.

**3. Database-level locking:**
```sql
SELECT * FROM products WHERE id = 5 FOR UPDATE;
UPDATE products SET price = 1999 WHERE id = 5;
```

Row lock prevents concurrent modifications within transaction.

The system provides low-level database access via transactions. Higher-level patterns (versioning, state machines) are application concerns.

## Consistency model

Mutations execute within the transaction isolation level configured at the database level.

### READ COMMITTED

**Behavior:**
- Each statement sees committed data as of statement start
- Other transactions' uncommitted changes are not visible
- Repeated reads within transaction may see different data (if other transactions commit)

**Use for:** Most applications with moderate concurrency

### REPEATABLE READ

**Behavior:**
- Transaction sees snapshot of database as of transaction start
- Other transactions' changes (committed or not) are not visible
- Repeated reads return consistent results

**Use for:** Applications requiring read consistency within transaction

### Implications for mutations

**Within a transaction:**
```graphql
mutation {
  category: createCategory(input: {name: "Books"}) { id }
  product: createProduct(input: {
    categoryId: ???  # Can reference category.id
  }) { id }
}
```

The second mutation can see the first mutation's changes (same transaction).

**Across transactions:**

Two concurrent requests with separate transactions see isolation based on database configuration.

Request A:
```graphql
mutation {
  createProduct(input: {sku: "ABC"}) { id }
}
```

Request B:
```graphql
mutation {
  createProduct(input: {sku: "ABC"}) { id }
}
```

If both execute concurrently:
- One succeeds
- One fails with `unique_violation` (sku is unique)

The database handles concurrency. The application does not implement additional locking.

## Error handling and rollback

Any error during mutation execution triggers rollback.

### GraphQL errors

If a mutation resolver returns an error:
```go
mc.MarkError()
return nil, err
```

The transaction will roll back during finalization.

### SQL errors

If SQL execution fails:
```go
result, err := mc.Tx().ExecContext(ctx, query.SQL, query.Args...)
if err != nil {
    mc.MarkError()  // Ensures rollback
    return nil, normalizeError(err)
}
```

Database errors (constraint violations, FK errors) trigger rollback.

### Panic recovery

The middleware uses deferred finalization:
```go
defer func() {
    if r := recover(); r != nil {
        mc.MarkError()
        mc.Finalize()
        panic(r)
    } else {
        mc.Finalize()
    }
}()
```

If a panic occurs, the transaction still rolls back safely.

## Design principles

The transaction design reflects several core principles:

**1. Database as source of truth**

Trust the database for data integrity, concurrency control, and constraint enforcement. Don't duplicate database functionality in application logic.

**2. Simplicity over features**

Provide transactions and error handling. Avoid complex patterns (optimistic locking, sagas, compensation) unless there's a clear, common use case.

**3. Composability**

Multiple mutations in one request compose into a single transaction. Clients control atomicity boundaries by grouping mutations in requests.

**4. Fail-safe defaults**

Errors trigger rollback by default. Explicit error handling (via `mc.MarkError()`) ensures no accidental commits.

**5. GraphQL semantics**

Follow GraphQL mutation semantics (sequential execution) while adding database-level atomicity guarantees.

---
# Related Docs

## Next steps
- [Modifying data with mutations](../tutorials/mutation-basics.md)

## Reference
- [GraphQL schema mapping](../reference/graphql-schema.md)

## Further reading
- [Query planning as a seam](query-planning.md)
- [Database-first authorization](database-first-auth.md)

## Back
- [Explanation home](README.md)
- [Docs home](../README.md)
