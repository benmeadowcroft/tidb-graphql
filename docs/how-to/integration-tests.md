# Run integration tests locally

Goal: run the integration suite against TiDB Cloud.

## 1) Create `.env.test`

Follow `tests/README.md` and add your TiDB Cloud credentials.

## 2) Run the tests

```bash
go test ./tests/integration/...
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

Other integration tests are unchanged and continue to use the single admin account.

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
