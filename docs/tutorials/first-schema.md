# Your first schema in five minutes

Goal: start TiDB GraphQL with a sample database and run a query in GraphiQL.

## Prerequisites

- Docker or Podman with Compose support

## 1) Clone and start

```bash
git clone https://github.com/benmeadowcroft/tidb-graphql.git
cd tidb-graphql
docker compose up
```

This starts a local TiDB instance with sample data and the TiDB GraphQL server with GraphiQL enabled. Wait for the log output to show that the server has started.

> **Podman users:** replace `docker compose` with `podman compose`, or use `make compose-up`.

## 2) Open GraphiQL

Visit [http://localhost:8080/graphql](http://localhost:8080/graphql) in your browser.

You should see a page that looks like this:

![GraphiQL user interface screenshot.](./images/graphiql.png)

The **Docs** link in the top right opens the GraphiQL document explorer for browsing the generated schema.

## 3) Run a query

Paste the following query into GraphiQL. It fetches orders with their line items, product details, and customer information — all resolved automatically from the database foreign keys.

```graphql
query {
  orders {
    id status totalCents
    orderItems {
      quantity
      product { name sku }
    }
    user { fullName email }
  }
}
```

That's it! You have a working GraphQL API backed by TiDB. No manual schema files, no resolver boilerplate — TiDB GraphQL introspects your database and generates the GraphQL schema automatically.

## Stopping and resetting

```bash
docker compose down      # stop containers (data persists)
docker compose down -v   # stop and delete all data (fresh start)
```

## Customizing

The development environment can be customized with environment variables. Copy `.env.example` to `.env` and uncomment the values you want to change. Common overrides:

| Variable | Default | Description |
|----------|---------|-------------|
| `TIGQL_SERVER_PORT` | `8080` | HTTP server port |
| `TIGQL_OBSERVABILITY_LOGGING_LEVEL` | `info` | Log level (`debug`, `info`, `warn`, `error`) |
| `TIGQL_DATABASE_DATABASE` | `tidb_graphql_tutorial` | Database to expose via GraphQL |

See the [Configuration reference](../reference/configuration.md) for all available settings.

## Connecting to your own database

To connect to an existing TiDB instance instead of the bundled one, set the database environment variables in your `.env` file or directly in `docker-compose.yml`:

```bash
TIGQL_DATABASE_HOST=gateway01.us-west-2.prod.aws.tidbcloud.com
TIGQL_DATABASE_PORT=4000
TIGQL_DATABASE_USER=prefix.root
TIGQL_DATABASE_PASSWORD=your-password
TIGQL_DATABASE_DATABASE=your_database
```

For more advanced setups (building from source, TLS/mTLS, DSN connection strings), see the [Build from source](../how-to/build-from-source.md) and [Database authentication](../how-to/database-auth.md) guides.

---
# Related Docs

## Next steps
- [Query basics](query-basics.md)
- [Local OIDC](local-oidc.md)

## Reference
- [Configuration reference](../reference/configuration.md)
- [Flags and environment variables](../reference/cli-env.md)

## Further reading
- [Schema handling](../explanation/schema-handling.md)
- [Query planning](../explanation/query-planning.md)

## Back
- [Tutorials home](README.md)
- [Docs home](../README.md)
