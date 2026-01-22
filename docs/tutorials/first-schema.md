# Your first schema in five minutes

Let's get up and running with TiDB GraphQL with a quick example.

Goal: point TiDB GraphQL at a database, let it introspect, then run a query in GraphiQL.

## Prerequisites
- TiDB instance - If you don't have a TiDB instance avaialble already, then TiDB Cloud has a generous free tier and you can [setup TiDB Cloud Starter in a few moments](https://docs.pingcap.com/tidbcloud/dev-guide-build-cluster-in-cloud/?plan=starter).
- `make` & `go` - these tools are used to build the project.
- `mysql` client (optional) - this client is used to load sample data to your database.

## 1) Build the server

To be the server run the following command

```bash
make build
```

This will create the TiDB GraphQL server binary located at `./bin/tidb-graphql`

## 2) Load sample data (optional)

If you want a predictable dataset for the tutorials, load the sample schema into your TiDB instance.

Replace the hostname (`gateway01.us-west-2.prod.aws.tidbcloud.com`) and user name (`prefix.root`) in the example below with information for your TiDB instance:

```bash
mysql --comments -u 'prefix.root' -h gateway01.us-east-1.prod.aws.tidbcloud.com -P 4000 -p < docs/tutorials/sample-data.sql
```

Enter your TiDB instance's password when prompted.

When completed, this SQL script creates a `tidb_graphql_tutorial` database with a few tables and indexes.

## 3) Create a minimal config

Create `tidb-graphql.yaml`, set the TiDB configuration values (`host`, `port`, `user`) appropriately for your TiDB deployment. If you are using TiDB Cloud, you can find this information in the `Connect` section of your cluster details.

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

Note:
- If you already have other services running on port 8080 you can adjust this to another open port as needed.
- The [Configuration reference documentation](./../reference/configuration.md) includes full details of the other configuration options.

## 4) Start the server

Now, let's start the TiDB GraphQL server.

You can use the `--database.password_prompt` to securely enter the database password:

```bash
./bin/tidb-graphql --database.password_prompt
```

You should see a schema build at startup.

## 5) Open GraphiQL

After starting up, the server provides a simple GraphQL UI called GraphiQL.

Visit [http://localhost:8080/graphql](http://localhost:8080/graphql) using your web browser.

You should see a page that looks like this:

![GraphiQL user interface screenshot.](./images/graphiql.png)

## 6 A simple introspection query:

The TiDB GraphQL server publishes a GraphQL schema, you can query this directly using GraphQL itself. To do this enter a simple GraphQL introspection query to see what types TiDB GraphQL has published:

```graphql
{
  __schema {
    types {
      name
    }
  }
}
```

At this point you have a live GraphQL schema generated from your TiDB database. No manual schema files, no resolver boilerplate.

Note:
- The `Docs` link in the top right will open the GraphiQL document explorer that provides easy access to explore the GraphQL schema.

## 7) A quick data query

Pick a table from your schema and query it, you can use the GraphiQL autocomplete to show what fields are available.

For example, usign the sample dataset provided the following will query order information, including associated order item and user details.

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

If your schema is empty, create a small table in TiDB and refresh (or restart) and run the query again. (See step 2 above for details on how to do this)

That is it! You now have a working GraphQL API that tracks your database schema.

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
