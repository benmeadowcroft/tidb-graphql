# Multi-database queries

This tutorial shows how to query two databases through a single tidb-graphql endpoint using namespace-wrapped GraphQL fields. It uses the [multi-db compose scenario](../../examples/compose/multi-db/README.md), which seeds a tutorial store database and a separate shipping database linked by a cross-database foreign key.

## Prerequisites

Start the multi-database scenario:

```bash
docker compose -f examples/compose/multi-db/docker-compose.yml up
```

Open GraphiQL at http://localhost:8080/graphql.

## How multi-database mode works

When `database.databases` lists more than one entry (or a single entry with an explicit `namespace`), each database becomes a **namespace field** on the root `Query` and `Mutation` types.

```yaml
database:
  host: tidb
  port: 4000
  user: root
  databases:
    - name: tidb_graphql_tutorial
      namespace: store
    - name: tidb_graphql_shipping
      namespace: shipping
```

The resulting schema looks like:

```graphql
type Query {
  store:    Store_Query!    # all tidb_graphql_tutorial queries
  shipping: Shipping_Query! # all tidb_graphql_shipping queries
  node(id: ID!): Node       # global lookup across all namespaces
}
```

GraphQL type names are prefixed with the namespace: `Store_Order`, `Shipping_Shipment`, etc.

## Querying within a namespace

Wrap every query in the namespace field that owns the data.

### List orders from the tutorial store

```graphql
{
  store {
    orders(
      first: 10
      orderBy: [{ createdAt: DESC }]
    ) {
      totalCount
      nodes {
        databaseId
        status
        total
        createdAt
      }
    }
  }
}
```

### Filter shipments by status

```graphql
{
  shipping {
    shipments(
      where: { status: { eq: delivered } }
      first: 5
    ) {
      nodes {
        trackingNumber
        estimatedDelivery
        deliveredAt
        carrier {
          name
          trackingUrlTemplate
        }
      }
    }
  }
}
```

## Cross-database traversal

The `shipments` table has a foreign key `order_id` that references `tidb_graphql_tutorial.orders.id`. The server detects this cross-database FK automatically from `INFORMATION_SCHEMA` and generates relationship fields in both directions.

### Shipment → order (many-to-one)

```graphql
{
  shipping {
    shipments(
      where: { status: { eq: in_transit } }
      first: 5
    ) {
      nodes {
        trackingNumber
        status
        shippedAt
        carrier { name }
        order {
          databaseId
          total
          status
          user {
            fullName
            email
          }
        }
      }
    }
  }
}
```

### Order → shipments (one-to-many)

```graphql
{
  store {
    orders(
      where: { status: { eq: shipped } }
      first: 5
    ) {
      nodes {
        databaseId
        total
        shipments {
          nodes {
            trackingNumber
            status
            carrier { code name }
            shipmentTracking(
              orderBy: [{ eventTime: DESC }]
              first: 1
            ) {
              nodes {
                eventTime
                location
                status
                message
              }
            }
          }
        }
      }
    }
  }
}
```

## Querying both namespaces in a single request

GraphQL allows you to select from multiple namespaces in one round-trip:

```graphql
{
  store {
    orders(where: { status: { eq: shipped } }) {
      totalCount
    }
    users(where: { status: { eq: active } }) {
      totalCount
    }
  }
  shipping {
    shipments(where: { status: { eq: delivered } }) {
      totalCount
    }
    carriers {
      nodes { code name }
    }
  }
}
```

## Global node lookup

The `node(id:)` field on the root `Query` resolves any object by its opaque global ID, regardless of which namespace or database it belongs to:

```graphql
{
  node(id: "<paste a Store_Order id here>") {
    id
    ... on Store_Order {
      total
      status
      createdAt
    }
  }
}

# Works across namespaces too:
{
  node(id: "<paste a Shipping_Shipment id here>") {
    id
    ... on Shipping_Shipment {
      trackingNumber
      status
    }
  }
}
```

## Aggregates within a namespace

Aggregate functions work the same as in single-database mode, scoped to the namespace:

```graphql
{
  shipping {
    shipments {
      aggregate {
        count
      }
    }
    shipmentsByStatus: shipments(where: { status: { eq: delivered } }) {
      aggregate {
        count
      }
    }
  }
}
```

## Cross-database mutations

Mutations within a namespace follow the same pattern as single-database mode. The type names include the namespace prefix:

```graphql
mutation {
  shipping {
    createShipping_Carrier(input: {
      code: "NEWCO"
      name: "NewCo Logistics"
    }) {
      ... on CreateShipping_CarrierSuccess {
        carrier {
          databaseId
          code
          name
        }
      }
      ... on MutationError {
        message
      }
    }
  }
}
```

**Cross-database write restrictions**: For transactional safety, nested creates that span databases are blocked. Many-to-one connects (updating a local FK column to point at a row in another database) are allowed because only local columns change.

## Next steps

- See the [GraphQL schema reference](../reference/graphql-schema.md#multi-database-mode) for full details on type naming, namespace wrapper structure, and cross-database relationship restrictions.
- See the [configuration reference](../reference/configuration.md#multiple-databases) for per-database schema filters and naming overrides.
