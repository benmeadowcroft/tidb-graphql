# Schema filters

Schema filters let you explicitly allow or deny tables and columns using glob patterns. Deny rules take precedence over allow rules. This is enforced by the TiDB GraphQL server and is in addition to any restrictions applied by TiDB RBAC.

Patterns support:
- `*` for any sequence
- `?` for a single character

Matching is case-insensitive against raw table and column names.

## Configuration

```yaml
schema_filters:
  allow_tables: ["*"]
  deny_tables: ["*_intern"]
  deny_mutation_tables: ["audit_*"]
  allow_columns:
    "*": ["*"]
  deny_columns:
    "*": ["*_secret"]
  deny_mutation_columns:
    "*": ["*_readonly"]
```

## Behavior

- Missing `schema_filters` defaults to allow-all (subject to DB grants).
- Deny rules always win over allow rules.
- Column filtering applies before index-driven features.
- Indexes are only exposed when all indexed columns remain allowed.
- Unique lookup queries are only generated for remaining unique indexes.
- Mutation deny lists only affect mutation input fields; query visibility is unchanged.
- Tables/views filtered out by schema filters are not available for mutations.

## Examples

Allow all but internal tables:

```yaml
schema_filters:
  allow_tables: ["*"]
  deny_tables: ["*_intern", "tmp_*"]
  allow_columns:
    "*": ["*"]
```

Allow only specific tables:

```yaml
schema_filters:
  allow_tables: ["users", "orders"]
  allow_columns:
    "*": ["*"]
```

Allow all tables, deny sensitive columns:

```yaml
schema_filters:
  allow_tables: ["*"]
  allow_columns:
    "*": ["*"]
  deny_columns:
    "users": ["password_*", "ssn"]
    "*": ["*_secret"]
```

Disable mutations for audit tables and read-only columns:

```yaml
schema_filters:
  allow_tables: ["*"]
  allow_columns:
    "*": ["*"]
  deny_mutation_tables: ["audit_*"]
  deny_mutation_columns:
    "*": ["*_readonly"]
```

## Notes on DB permissions

Schema filters do not grant access to data. The GraphQL service can only expose what the SQL user is allowed to read. Use DB-level grants for coarse control, and schema filters for fine-grained API shaping.
