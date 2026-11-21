# Test Suite

This directory contains integration tests for the TiDB GraphQL project.

## Directory Structure

```
tests/
├── fixtures/           # SQL schemas and seed data used by integration tests
├── integration/        # Integration test files
└── README.md           # This file
```

## Quick Start

### Run Unit Tests (Fast)

```bash
just test-unit
```

Unit tests are located in `internal/*/` directories alongside the source code.

### Run Integration Tests

Integration tests require TiDB Cloud Serverless credentials.

1. **Setup credentials:**

   ```bash
   cp .env.test.example .env.test
   # Edit .env.test with your TiDB Cloud credentials
   ```

2. **Run tests:**

   ```bash
   just test-integration
   ```

### Against a local TiDB container

The tests also run against a standalone TiDB, e.g. `docker run -d -p 4000:4000 pingcap/tidb:v8.5.5`.
Give `root` a password first (the helpers skip when `TIDB_PASSWORD` is empty), then:

```bash
TIDB_HOST=127.0.0.1 TIDB_USER=root TIDB_PASSWORD=<password> TIDB_TLS_MODE=false just test-integration
```

### In CI

The `integration` job in [`container-and-platforms.yml`](../.github/workflows/container-and-platforms.yml)
runs the suite this way on every PR, push to `main` and release tag, against a `pingcap/tidb`
service container. The container image is only published if it passes. The `@asOf` tests skip
there (see below).

### Run All Tests

```bash
just test
```

## Integration Test Helper

The `internal/testutil/tidbcloud` package provides utilities for integration testing:

- **`NewTestDB(t)`** - Creates an isolated test database
- **`LoadSchema(t, path)`** - Loads SQL schema from file
- **`LoadFixtures(t, path)`** - Loads seed data from file
- **Automatic cleanup** - Test databases are dropped after each test

Some integration tests create multiple temporary databases in the same test run to exercise multi-database configuration, namespace wrappers, cross-database relationships, and schema refresh behavior.

## Snapshot Read Integration Tests

The `@asOf` integration tests in [`tests/integration/asof_test.go`](./integration/asof_test.go) require a TiDB environment backed by TiKV/PD so historical snapshot reads are available.

Lightweight or single-process TiDB environments can lack the `tikv_gc_safe_point` metadata required for `@@tidb_snapshot`. In those environments, the `@asOf` tests intentionally skip with a clear message instead of failing the full integration suite.

TiDB Cloud Starter and TiDB Cloud Zero support snapshot reads but deny `SELECT` on `mysql.tidb`. The probe treats that denial as "unknown" and goes on to try `@@tidb_snapshot` directly, so the `@asOf` tests can run there.

For full `@asOf` coverage, use TiDB Cloud or a complete TiDB topology such as the Kubernetes example or another deployment that includes TiDB, PD, and TiKV.

To run only the snapshot integration coverage:

```bash
go test -tags=integration ./tests/integration -run AsOf
```

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
