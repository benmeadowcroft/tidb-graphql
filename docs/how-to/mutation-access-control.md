# Control mutation access

This guide shows how to restrict which tables and columns are available for mutations. The two primary mechanisms for restricting this are role-based access control and schema filters.

If you are using [Database Role Authorization](../reference/auth.md#database-role-authorization) then the SQL GRANTS for that role will be enforced by the database.

You can also use schema filters to prevent specific mutations being added to the GraphQL API. This How-to will cover the use of schema filters to enable this.

**Goal:** Configure schema filters to control mutation access

## Overview

Schema filters let you:
- Disable mutations entirely for specific tables (queries still work)
- Make specific columns read-only (visible in queries, excluded from mutations)
- Control access without changing database permissions

**Note:** Generated columns are automatically excluded from mutations and don't require explicit schema filter configuration.

## Disable mutations for specific tables

Use `deny_mutation_tables` to suppress create, update, and delete mutations for specific tables.

### Configuration

```yaml
schema_filters:
  deny_mutation_tables:
    - "audit_log"
    - "system_events"
```

Or use wildcards:
```yaml
schema_filters:
  deny_mutation_tables:
    - "audit_*"
    - "system_*"
    - "*_log"
```

### Effect

For table `audit_log`:
- **Mutations removed:** `createAuditLog`, `updateAuditLog`, `deleteAuditLog`
- **Queries remain:** `auditLogs`, `auditLog`, `auditLog_by_*`, `auditLogs_aggregate`

### Use cases

**Audit tables:**
Tables that should only be written by application code or triggers, not via GraphQL.

```yaml
schema_filters:
  deny_mutation_tables:
    - "audit_log"
    - "login_history"
    - "data_changes"
```

**System tables:**
Internal tables managed by application logic.

```yaml
schema_filters:
  deny_mutation_tables:
    - "system_*"
    - "schema_migrations"
    - "background_jobs"
```

**Reporting tables:**
Materialized views or summary tables populated by batch processes.

```yaml
schema_filters:
  deny_mutation_tables:
    - "daily_reports"
    - "analytics_*"
```

## Make columns read-only

Use `deny_mutation_columns` to exclude columns from mutation input types while keeping them queryable.

### Configuration

```yaml
schema_filters:
  deny_mutation_columns:
    "*":
      - "created_at"
      - "last_updated"
    "users":
      - "status"
      - "role"
```

**Wildcard `*`:** Applies to all tables
**Table-specific:** Applies to named table only

### Effect

For `users` table with config above:
- **Excluded from CreateUserInput:** `created_at`, `last_updated`, `status`, `role`
- **Excluded from UpdateUserSetInput:** `created_at`, `last_updated`, `status`, `role`
- **Still queryable:** All columns visible in `users` query type

### Use cases

**Timestamp columns with DEFAULTs:**

```yaml
schema_filters:
  deny_mutation_columns:
    "*":
      - "created_at"
      - "updated_at"
      - "last_updated"
      - "modified_at"
```

Columns with `DEFAULT CURRENT_TIMESTAMP` or `ON UPDATE CURRENT_TIMESTAMP` should be managed by the database, not GraphQL mutations.

**Application-managed status fields:**

```yaml
schema_filters:
  deny_mutation_columns:
    "orders":
      - "status"
      - "payment_status"
      - "fulfillment_status"
```

Status transitions controlled by application business logic, not direct mutations.

**Internal identifiers:**

```yaml
schema_filters:
  deny_mutation_columns:
    "users":
      - "internal_id"
      - "legacy_user_id"
    "products":
      - "external_system_id"
```

IDs managed by data integration or migration processes.

**Computed totals:**

```yaml
schema_filters:
  deny_mutation_columns:
    "orders":
      - "total_amount"
      - "tax_amount"
      - "discount_amount"
```

Totals that should be calculated by application logic, not set directly.

## Combining table and column filters

You can use both filters together:

```yaml
schema_filters:
  # Read-only tables
  deny_mutation_tables:
    - "audit_log"
    - "system_events"

  # Read-only columns
  deny_mutation_columns:
    "*":
      - "created_at"
      - "last_updated"
    "users":
      - "status"
      - "email_verified"
    "orders":
      - "total_cents"
```

## Using deny_columns for complete exclusion

If you want to hide columns from both queries and mutations, use `deny_columns`:

```yaml
schema_filters:
  deny_columns:
    "users":
      - "password_hash"
      - "internal_notes"
      - "ssn"
```

**Effect:**
- Column not visible in GraphQL type
- Column not in mutation input types
- Column cannot be queried or mutated

**Use for:** Sensitive data that should never be exposed via GraphQL.

## Audit table example

Complete configuration for read-only audit log:

```yaml
schema_filters:
  # Disable all mutations
  deny_mutation_tables:
    - "audit_log"

  # Hide sensitive columns even from queries
  deny_columns:
    "audit_log":
      - "internal_notes"

  # Schema will generate:
  # - auditLogs(...): [AuditLog]  ✓
  # - auditLog(id: ID!): AuditLog  ✓
  # - createAuditLog(...)  ✗ (disabled)
  # - updateAuditLog(...)  ✗ (disabled)
  # - deleteAuditLog(...)  ✗ (disabled)
```

## Verification with introspection

Use GraphiQL to verify your schema filters are working correctly.

### Check available mutations

```graphql
{
  __schema {
    mutationType {
      fields {
        name
      }
    }
  }
}
```

Verify that filtered tables don't have `createX`, `updateX`, `deleteX` mutations.

### Check input type fields

For `CreateUserInput`:
```graphql
{
  __type(name: "CreateUserInput") {
    inputFields {
      name
    }
  }
}
```

Verify that filtered columns are not in the list.

For `UpdateUserSetInput`:
```graphql
{
  __type(name: "UpdateUserSetInput") {
    inputFields {
      name
    }
  }
}
```

Verify that filtered columns are not in the list.

### Check query type fields

```graphql
{
  __type(name: "User") {
    fields {
      name
    }
  }
}
```

Verify that columns in `deny_mutation_columns` are still present (queryable).
Verify that columns in `deny_columns` are absent (hidden).

## Impact on create mutations

If you filter required columns (NOT NULL, no DEFAULT), the create mutation may not be generated.

### Example

```sql
CREATE TABLE users (
  id BIGINT PRIMARY KEY AUTO_INCREMENT,
  email VARCHAR(255) NOT NULL,  -- Required
  status VARCHAR(32) NOT NULL,  -- Required
  name VARCHAR(255)
);
```

**Config:**
```yaml
schema_filters:
  deny_mutation_columns:
    "users":
      - "email"
      - "status"
```

**Effect:**
- `createUser` mutation **not generated** (all required fields are filtered)
- `updateUser` mutation **generated** (can update `name`)
- `deleteUser` mutation **generated**

**Recommendation:** If you filter all required insertable columns, add the table to `deny_mutation_tables` to be explicit.

## Best practices

### 1. Use wildcards for common patterns

```yaml
schema_filters:
  deny_mutation_columns:
    "*":
      - "created_at"
      - "updated_at"
      - "created_by"
      - "modified_by"
```

Apply to all tables, then override per-table if needed.

### 2. Be explicit about read-only tables

```yaml
schema_filters:
  deny_mutation_tables:
    - "audit_log"    # Clear intent: no mutations allowed
```

Better than filtering all columns individually.

### 3. Document why columns are filtered

```yaml
schema_filters:
  deny_mutation_columns:
    "orders":
      - "status"           # Managed by order state machine
      - "total_cents"      # Computed from order_items
      - "payment_status"   # Updated by payment webhook
```

Add comments explaining business logic.

### 4. Validate with introspection

After config changes:
- Query schema to verify mutations exist/don't exist
- Check input types to verify column exclusions
- Test mutations to ensure they work as expected

### 5. Consider database-level permissions

Schema filters control GraphQL API access. For defense in depth:
- Use database GRANT/REVOKE for read-only tables
- Use database triggers to enforce column immutability
- Use database roles for row-level security

---
# Related Docs

## Next steps
- [Handle mutation errors](handle-mutation-errors.md)
- [Work with auto-generated columns](auto-generated-columns.md)

## Reference
- [Schema filters](../reference/schema-filters.md)

## Further reading
- [Database-first authorization](../explanation/database-first-auth.md)

## Back
- [How-to guides home](README.md)
- [Docs home](../README.md)
