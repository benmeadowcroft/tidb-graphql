# TiDB GraphQL

TiDB GraphQL allows you to provide a rapid GraphQL API for your TiDB database.

*This project is experimental!*

TiDB GraphQL is a SQL-first GraphQL server for TiDB. It introspects a live TiDB schema and exposes a GraphQL API over that schema.

The goal of the project is to explore a SQL-first approach to GraphQL with practical guardrails, so teams can get useful access quickly without handcrafting every GraphQL resolver.

## Key features

- **Database-first GraphQL**: schema is derived from live TiDB introspection, not hand-written types and resolvers.
- **Immutable schema snapshots**: schema builds are atomic and refreshed safely without partial updates.
- **Database-enforced authorization**: JWT claims map to TiDB Role Based Access Control, ensuring consistent access control.
- **Built-in observability**: OpenTelemetry metrics, tracing, and logs with SQL commenter support.
- **Guardrails by default**: indexed filter/order-by requirements and configurable query limits.

## Getting started

### Quick start

The [Your first schema in five minutes](docs/tutorials/first-schema.md) tutorial is a quick way to get going with TiDB GraphQL

If you are looking for how to run it, configure it, or explore the schema, start with the docs:

- [Documentation home](docs/README.md)
  - [Tutorials](docs/tutorials/README.md)
  - [How-To guides](docs/how-to/README.md)
  - [Technical reference](docs/reference/README.md)
  - [Explanation](docs/explanation/README.md)

## Development workflow

This project uses `make` targets for common tasks:

### How to build

To build the project use `make build` from your terminal. The output of the build is the binary `bin/tidb-graphql`.

### How to run tests

The project provides both unit tests (using `make test-unit`) and integration tests (using `make test-integration`);

Notes:
- `make test-unit` runs fast unit tests in `./internal/...`.
- `make test-integration` requires credentials to a TiDB Database via environment variables or `.env.test` (see `.env.test.example`). You can use the free tier of [TiDB Cloud Starter](https://www.pingcap.com/tidb-cloud-starter/) to quickly provision a TiDB instance for integration testing.

## License

Apache-2.0. See [LICENSE](LICENSE).
