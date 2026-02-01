# Handle mutation errors

This guide helps you recognize and handle common mutation error scenarios.

**Goal:** Identify mutation error types and implement appropriate error handling

## Error response format

All mutation errors follow this format:

```json
{
  "data": {
    "createProduct": null
  },
  "errors": [{
    "message": "Human-readable error message",
    "extensions": {
      "code": "error_code_here",
      "mysql_code": 1234
    }
  }]
}
```

**Key fields:**
- `data`: The mutation result (often `null` when errors occur)
- `errors`: Array of error objects
- `extensions.code`: Normalized error code (use this for error handling)
- `extensions.mysql_code`: MySQL error number (for detailed handling)

## Unique constraint violations

**Error code:** `unique_violation`
**MySQL code:** 1062

### Scenario

Attempting to insert or update a value that violates a unique constraint.

### Example

```graphql
mutation {
  createCategory(input: {
    name: "stationery"
  }) {
    id
    name
  }
}
```

**Error response:**
```json
{
  "errors": [{
    "message": "Duplicate entry 'stationery' for key 'uq_categories_name'",
    "extensions": {
      "code": "unique_violation",
      "mysql_code": 1062
    }
  }]
}
```

### How to handle

**Before mutation:**
- Query existing records to check for duplicates
- Use unique index lookups: `category_by_name(name: "stationery")`

**After error:**
- Detect `unique_violation` error code
- Show user-friendly message: "A category with this name already exists"
- Suggest alternatives or allow user to retry with different value

**Application code example:**
```javascript
if (result.errors) {
  const uniqueError = result.errors.find(
    e => e.extensions?.code === 'unique_violation'
  );
  if (uniqueError) {
    // Handle duplicate value
    showError('This name is already taken. Please choose another.');
  }
}
```

## Foreign key violations

**Error code:** `foreign_key_violation`
**MySQL codes:** 1452 (insert/update child), 1451 (delete parent)

### Scenario 1: Invalid foreign key reference (1452)

Attempting to create or update a record with a foreign key that doesn't exist.

### Example

```graphql
mutation {
  createOrder(input: {
    userId: 99999
    status: "pending"
    totalCents: 1000
  }) {
    id
  }
}
```

**Error response:**
```json
{
  "errors": [{
    "message": "Cannot add or update a child row: a foreign key constraint fails",
    "extensions": {
      "code": "foreign_key_violation",
      "mysql_code": 1452
    }
  }]
}
```

**How to handle:**
- Validate foreign key exists before mutation
- Query parent record first: `user(id: 99999) { id }`
- Show error: "User not found"

### Scenario 2: Deleting parent with dependent children (1451)

Attempting to delete a record that has dependent child records.

### Example

```graphql
mutation {
  deleteUser(id: 1) {
    id
  }
}
```

**Error response:**
```json
{
  "errors": [{
    "message": "Cannot delete or update a parent row: a foreign key constraint fails",
    "extensions": {
      "code": "foreign_key_violation",
      "mysql_code": 1451
    }
  }]
}
```

**How to handle:**
- Check for dependent records before delete
- Query: `user(id: 1) { orders { id } }`
- Options:
  - Delete dependent records first
  - Warn user: "Cannot delete user with existing orders"
  - Implement soft delete (update status instead of delete)

## Not-null violations

**Error code:** `not_null_violation`
**MySQL codes:** 1048 (column cannot be null), 1364 (no default value)

### Scenario

Missing a required field or explicitly setting a NOT NULL column to null.

### Example

```graphql
mutation {
  createProduct(input: {
    name: "New Product"
    # Missing priceCents (required, no DEFAULT)
  }) {
    id
  }
}
```

**Error response:**
```json
{
  "errors": [{
    "message": "Field 'price_cents' doesn't have a default value",
    "extensions": {
      "code": "not_null_violation",
      "mysql_code": 1364
    }
  }]
}
```

### How to handle

**Prevention:**
- Use GraphiQL or schema introspection to identify required fields
- Check `CreateXInput` type for NonNull (required) fields
- Validate input completeness before sending mutation

**Example schema check:**
```graphql
{
  __type(name: "CreateProductInput") {
    inputFields {
      name
      type {
        kind
        name
      }
    }
  }
}
```

Look for `NON_NULL` type kind to identify required fields.

**Application code:**
- Validate required fields on client side
- Show clear validation errors before attempting mutation

## Access denied

**Error code:** `access_denied`
**MySQL codes:** 1142, 1143, 1370

### Scenario

Insufficient database permissions or schema filter restrictions.

### Example

Attempting to mutate a table that's in `deny_mutation_tables`:

```graphql
mutation {
  createAuditLog(input: {...}) {
    id
  }
}
```

**Error response:**
```json
{
  "errors": [{
    "message": "Access denied for operation",
    "extensions": {
      "code": "access_denied"
    }
  }]
}
```

### How to handle

**Possible causes:**
1. Table is in `deny_mutation_tables` schema filter
2. Column is in `deny_mutation_columns` schema filter
3. Database user lacks INSERT/UPDATE/DELETE permissions
4. Row-level security via database roles denies access

**Debugging steps:**
1. Check schema filters configuration
2. Verify database user permissions
3. Check if using role-based authentication
4. Use GraphiQL introspection to see available mutations

**Prevention:**
- Query schema to see available mutation fields
- Only show UI controls for mutations user can perform

## Invalid input errors

**Error code:** `invalid_input`

### Scenario

Providing unknown columns or columns not allowed in input types.

### Example

```graphql
mutation {
  createProduct(input: {
    name: "Product"
    priceCents: 1000
    createdAt: "2024-01-01"  # Not allowed (has DEFAULT)
  }) {
    id
  }
}
```

**How to handle:**
- Use GraphiQL to explore available input fields
- Check `CreateXInput` and `UpdateXSetInput` types in schema
- Only send fields that exist in the input type

## Debugging checklist

When a mutation fails, follow these steps:

### 1. Check error code
```javascript
const errorCode = result.errors[0]?.extensions?.code;
```

Use error code to identify error category.

### 2. Check MySQL error code
```javascript
const mysqlCode = result.errors[0]?.extensions?.mysql_code;
```

Use for detailed error handling (e.g., distinguish FK errors 1451 vs 1452).

### 3. Inspect input type schema

Use GraphiQL introspection:
```graphql
{
  __type(name: "CreateProductInput") {
    inputFields {
      name
      type {
        kind
        name
        ofType { name }
      }
    }
  }
}
```

Verify:
- Field names are correct
- Required (NonNull) fields are provided
- Field types match expected values

### 4. Check schema filters

Review configuration:
```yaml
schema_filters:
  deny_mutation_tables: [...]
  deny_mutation_columns:
    table_name: [column_list]
```

Verify table and columns are not filtered.

### 5. Verify database permissions

For database-level errors, check:
- User has INSERT/UPDATE/DELETE grants
- Row-level security policies
- Database role permissions (if using role-based auth)

## Error handling patterns

### Client-side validation

```javascript
function validateCreateProduct(input) {
  const errors = [];

  if (!input.name) errors.push('Name is required');
  if (!input.sku) errors.push('SKU is required');
  if (!input.priceCents) errors.push('Price is required');

  return errors;
}

// Before mutation
const validationErrors = validateCreateProduct(input);
if (validationErrors.length > 0) {
  // Show validation errors
  return;
}

// Proceed with mutation
const result = await executeMutation(...);
```

### Error code handling

```javascript
function handleMutationError(error) {
  const code = error.extensions?.code;

  switch (code) {
    case 'unique_violation':
      return 'This value already exists. Please use a different value.';

    case 'foreign_key_violation':
      const mysqlCode = error.extensions?.mysql_code;
      if (mysqlCode === 1451) {
        return 'Cannot delete: this record has dependent data.';
      } else {
        return 'Invalid reference: the related record does not exist.';
      }

    case 'not_null_violation':
      return 'Missing required fields. Please fill in all required information.';

    case 'access_denied':
      return 'You do not have permission to perform this operation.';

    default:
      return 'An error occurred. Please try again.';
  }
}
```

### Transaction rollback awareness

When multiple mutations fail, all are rolled back:

```graphql
mutation {
  category: createCategory(input: {...}) { id }
  product: createProduct(input: {...}) { id }  # Fails
}
```

Both mutations are rolled back. The category is not created.

**Handling:**
- Treat multiple mutations as atomic
- If any fails, none succeed
- Retry entire mutation block, not individual mutations

---
# Related Docs

## Next steps
- [Control mutation access](mutation-access-control.md)
- [Work with auto-generated columns](auto-generated-columns.md)

## Reference
- [Schema filters](../reference/schema-filters.md)

## Further reading
- [Mutation transaction handling](../explanation/mutation-transactions.md)

## Back
- [How-to guides home](README.md)
- [Docs home](../README.md)
