# Build from source

Build and run tidb-graphql as a standalone binary, without Docker.

## Prerequisites

- Go 1.25 or later
- `make`
- A running TiDB instance (local or [TiDB Cloud](https://docs.pingcap.com/tidbcloud/dev-guide-build-cluster-in-cloud/?plan=starter))
- `mysql` client (optional, for loading sample data)

## 1) Build the binary

```bash
make build
```

This creates the server binary at `./bin/tidb-graphql`.

## 2) Load sample data (optional)

Load the tutorial dataset into your TiDB instance. Replace the host and user with your TiDB connection details:

```bash
mysql --comments -u 'prefix.root' -h gateway01.us-east-1.prod.aws.tidbcloud.com -P 4000 -p < docs/tutorials/sample-data.sql
```

This creates a `tidb_graphql_tutorial` database with sample tables and data.

## 3) Create a config file

Create `tidb-graphql.yaml` in the project root. Set the database connection values for your TiDB instance:

```yaml
database:
  host: gateway01.us-west-2.prod.aws.tidbcloud.com
  port: 4000
  user: prefix.root
  database: tidb_graphql_tutorial

server:
  port: 8080
  graphiql_enabled: true
```

If you are using TiDB Cloud, find the connection details in the **Connect** section of your cluster dashboard.

See the [Configuration reference](../reference/configuration.md) for all available settings.

## 4) Start the server

Use `--database.password_prompt` for secure password entry:

```bash
./bin/tidb-graphql --database.password_prompt
```

Alternatively, set the password via environment variable:

```bash
TIGQL_DATABASE_PASSWORD=your-password ./bin/tidb-graphql
```

## 5) Open GraphiQL

Visit [http://localhost:8080/graphql](http://localhost:8080/graphql) in your browser. You should see the GraphiQL interactive explorer with your database schema available for querying.

## Building a container image

To build the container image locally without Docker Compose:

```bash
make container-build
```

This creates a `tidb-graphql:local` image. The build auto-detects `podman` or `docker`. To specify one explicitly:

```bash
CONTAINER_TOOL=docker make container-build
```

---
# Related Docs

## Next steps
- [Your first schema in five minutes](../tutorials/first-schema.md) (Docker Compose quickstart)
- [Query basics](../tutorials/query-basics.md)

## Reference
- [Configuration reference](../reference/configuration.md)
- [Flags and environment variables](../reference/cli-env.md)

## Back
- [How-To guides](README.md)
- [Docs home](../README.md)
