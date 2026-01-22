# Test Suite

This directory contains integration tests for the TiDB GraphQL project.

## Directory Structure

```
tests/
├── fixtures/                       # Test data and schemas
│   ├── scripture_schema.sql       # Multi-level FK relationships
│   ├── scripture_seed.sql         # Seed data for scripture schema
│   ├── simple_schema.sql          # Basic test schema
│   └── simple_seed.sql            # Seed data for simple schema
│   ├── schema_refresh_v1.sql      # Base schema for refresh tests
│   └── schema_refresh_v2.sql      # ALTER schema for refresh tests
├── integration/                    # Integration test files
│   └── auth_test.go                # JWT auth integration tests
├── integration/                    # Integration test files
│   └── introspection_test.go      # Database introspection tests
└── README.md                       # This file
```

## Quick Start

### Run Unit Tests (Fast)

```bash
make test-unit
```

Unit tests are located in `internal/*/` directories alongside the source code.

### Run Integration Tests

Integration tests require TiDB Cloud Serverless credentials.

1. **Setup credentials:**

   ```bash
   cp ../.env.test.example ../.env.test
   # Edit .env.test with your TiDB Cloud credentials
   ```

2. **Run tests:**

   ```bash
   make test-integration
   ```

### Run All Tests

```bash
make test
```

## Integration Test Helper

The `internal/testutil/tidbcloud` package provides utilities for integration testing:

- **`NewTestDB(t)`** - Creates an isolated test database
- **`LoadSchema(t, path)`** - Loads SQL schema from file
- **`LoadFixtures(t, path)`** - Loads seed data from file
- **Automatic cleanup** - Test databases are dropped after each test

## Role-Based Authorization Integration Tests

Role-based authorization tests create and grant database roles, then validates GraphQL access through OIDC + DB role middleware. These tests create a temporary runtime user and require database credentials with:

- `CREATE USER`
- `DROP USER`
- `CREATE ROLE`
- `GRANT`
- `SET ROLE`
- `DROP ROLE`
- `GRANT OPTION`
- `CREATE/DROP DATABASE`

Fixtures:

- `tests/fixtures/role_test_schema.sql`
- `tests/fixtures/role_test_seed.sql`

## Test Fixtures

### scripture_schema.sql

Hierarchical schema for testing foreign key relationships:

```
volumes (1)
  └─ books (n)
      └─ chapters (n)
          └─ verses (n)
```

Use this for testing:
- Multi-level foreign keys
- Relationship traversal
- Complex queries

### simple_schema.sql

Basic blog-like schema:

```
users (1) ─┬─> posts (n)
           │       └─> comments (n)
           └─────────> comments (n)
```

Use this for testing:
- Basic foreign keys
- Multiple FKs to same table
- Simple CRUD operations

## Writing New Integration Tests

```go
package integration

import (
    "testing"
    "github.com/stretchr/testify/assert"
    "github.com/stretchr/testify/require"
    "tidb-graphql/internal/testutil/tidbcloud"
)

func TestMyFeature(t *testing.T) {
    if testing.Short() {
        t.Skip("Skipping integration test in short mode")
    }

    // Create isolated test database
    testDB := tidbcloud.NewTestDB(t)

    // Load schema
    testDB.LoadSchema(t, "../fixtures/simple_schema.sql")

    // Optionally load seed data
    testDB.LoadFixtures(t, "../fixtures/simple_seed.sql")

    // Run your tests
    // testDB.DB is a *sql.DB connection

    // Cleanup happens automatically
}
```

## OIDC/JWKS Auth Testing

For auth tests, generate a local keypair, run a JWKS server, and mint a token:

```bash
go run ./scripts/jwt-generate-keys
go run ./scripts/jwks-server --addr :9000 --issuer https://localhost:9000
go run ./scripts/jwt-mint --issuer https://localhost:9000 --audience tidb-graphql --kid local-key
```

Configure the server with:

```yaml
server:
  oidc_enabled: true
  oidc_issuer_url: "https://localhost:9000"
  oidc_audience: "tidb-graphql"
  oidc_skip_tls_verify: true
```
