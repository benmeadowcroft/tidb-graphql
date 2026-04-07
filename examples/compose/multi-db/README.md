# Multi-Database Compose Scenario

Demonstrates **multi-database mode**: two SQL databases exposed through a single GraphQL endpoint using namespace-wrapped query and mutation fields. A cross-database foreign key is automatically detected and surfaces as a traversable relationship in both directions.

## What this scenario sets up

| Database | GraphQL namespace | Tables |
|---|---|---|
| `tidb_graphql_tutorial` | `store` | users, products, categories, orders, order_items |
| `tidb_graphql_shipping` | `shipping` | carriers, shipments, shipment_tracking |

`shipments.order_id` is a foreign key to `tidb_graphql_tutorial.orders.id`. The server detects this cross-database relationship automatically — no extra configuration is required.

## Quick start

```bash
docker compose -f examples/compose/multi-db/docker-compose.yml up
```

Open **GraphiQL** at http://localhost:8080/graphql once all services are healthy.

## Example queries

### List shipments with carrier and status

```graphql
{
  shipping {
    shipments(first: 10, orderBy: [{ createdAt: DESC }]) {
      nodes {
        databaseId
        trackingNumber
        status
        estimatedDelivery
        carrier {
          code
          name
        }
      }
    }
  }
}
```

### Cross-database traversal: shipment → order details

The `order` field on `Shipping_Shipment` traverses the cross-database FK back to `tidb_graphql_tutorial.orders`.

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

### Cross-database traversal in reverse: order → shipments

The `shipments` field on `Store_Order` traverses back to `tidb_graphql_shipping`.

```graphql
{
  store {
    orders(first: 5, orderBy: [{ createdAt: ASC }]) {
      nodes {
        databaseId
        total
        status
        shipments {
          nodes {
            trackingNumber
            status
            carrier { name }
          }
        }
      }
    }
  }
}
```

### Query both namespaces in one request

```graphql
{
  store {
    orders(where: { status: { eq: shipped } }, first: 3) {
      totalCount
      nodes { databaseId total }
    }
  }
  shipping {
    shipments(where: { status: { eq: delivered } }, first: 3) {
      totalCount
      nodes { trackingNumber deliveredAt }
    }
  }
}
```

### Global node lookup across namespaces

The `node(id:)` field on the root query resolves any object by its opaque global ID, regardless of which namespace it belongs to.

```graphql
{
  node(id: "<paste an id from a previous query>") {
    id
    ... on Store_Order { total status }
    ... on Shipping_Shipment { trackingNumber status }
  }
}
```

## Configuration

Settings are in [`config/tidb-graphql/tidb-graphql.example.yaml`](config/tidb-graphql/tidb-graphql.example.yaml). The key section is:

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

See the [multi-database configuration reference](../../../docs/reference/configuration.md#multiple-databases) for the full set of options including per-database schema filters and naming overrides.

## Seeded data

| Table | Rows |
|---|---|
| `tidb_graphql_tutorial.users` | 100 |
| `tidb_graphql_tutorial.orders` | 200 |
| `tidb_graphql_shipping.carriers` | 5 |
| `tidb_graphql_shipping.shipments` | ~36 (for non-canceled orders) |
| `tidb_graphql_shipping.shipment_tracking` | ~90 |
