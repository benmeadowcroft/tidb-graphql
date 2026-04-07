# Run integration tests locally

Goal: run the integration suite against TiDB Cloud.

## 1) Create `.env.test`

Copy the example file and fill in your database credentials:

```bash
cp .env.test.example .env.test
```

## 2) Run the tests

```bash
make test-integration
```

## 3) Troubleshoot quickly

- Verify the database is reachable from your machine.
- Double-check credentials and cluster region.
- Expect longer runtimes than unit tests.

## Role-based authorization tests

Role-based authorization integration tests create a temporary runtime user to ensure access is enforced through database roles. These tests still use the admin credentials from `.env.test` but require additional privileges:

- `CREATE USER`, `DROP USER`
- `CREATE ROLE`, `DROP ROLE`
- `GRANT OPTION`
- `CREATE/DROP DATABASE`

Other integration tests use the same admin credentials from `.env.test`. Some tests also create multiple temporary databases to exercise multi-database schema assembly and cross-database relationships.

The role-based tests also create a dedicated introspection role and set it only while building the schema, so runtime queries remain constrained to request roles. The runtime connection is opened against `information_schema` and the executor switches databases with `USE <db>` during query execution.

---
# Related Docs

## Next steps
- [Use config precedence correctly](config-precedence.md)
- [Add observability](observability.md)

## Reference
- [Flags and environment variables](../reference/cli-env.md)
- [Configuration reference](../reference/configuration.md)

## Further reading
- [Deployment overview](../explanation/deployment-overview.md)

## Back
- [How-to home](README.md)
- [Docs home](../README.md)
