# Kubernetes quickstart (TiDB Operator + tidb-graphql)

This scenario starts a complete local development stack:
- a local TiDB cluster managed by TiDB Operator (`v2.0.0`)
- tutorial sample data (including vector review add-on data)
- `tidb-graphql`

Use `kubectl port-forward` for local access:

- GraphQL UI/API: `8080`
- TiDB SQL: `4000`

## Files

- `manifests/kustomization.yaml`

## Prerequisites

- Kubernetes cluster is running.
- TiDB Operator v2.0.0 CRDs are installed on that cluster.
- `kubectl` is configured for your local cluster.

## Start (quickstart)

From repository root:

```bash
export NAMESPACE=tidb-graphql-quickstart
export CLUSTER_NAME=quickstart

# Keep CLUSTER_NAME aligned with the `Cluster.metadata.name` in manifests/10-tidb-cluster.yaml.

kubectl apply --server-side -k examples/k8s/quickstart/manifests
```

Wait for TiDB component readiness and the seed job:

```bash
kubectl -n "${NAMESPACE}" get cluster
kubectl -n "${NAMESPACE}" get group
kubectl -n "${NAMESPACE}" wait --for=condition=Ready pod -l "pingcap.com/cluster=${CLUSTER_NAME}" --timeout=10m
kubectl -n "${NAMESPACE}" wait --for=condition=Complete job/"${CLUSTER_NAME}"-seed --timeout=10m
kubectl -n "${NAMESPACE}" logs job/"${CLUSTER_NAME}"-seed --tail=200
```

If the seed logs are empty, check the seed job and SQL configmap:

```bash
kubectl -n "${NAMESPACE}" get configmap "${CLUSTER_NAME}"-tutorial-sql
kubectl -n "${NAMESPACE}" get pods -l "app.kubernetes.io/name=tidb-graphql-seed,app.kubernetes.io/instance=${CLUSTER_NAME}"
kubectl -n "${NAMESPACE}" describe job "${CLUSTER_NAME}"-seed
```

If you see `FailedMount` for `quickstart-tutorial-sql`, recreate the seed job so the new pod re-runs with the kustomize-generated ConfigMap:

```bash
kubectl -n "${NAMESPACE}" delete job "${CLUSTER_NAME}"-seed
kubectl -n "${NAMESPACE}" apply -k examples/k8s/quickstart/manifests
kubectl -n "${NAMESPACE}" wait --for=condition=Complete job/"${CLUSTER_NAME}"-seed --timeout=10m
```

Forward ports:

```bash
kubectl -n "${NAMESPACE}" port-forward svc/tidb-graphql 8080:8080
```

```bash
kubectl -n "${NAMESPACE}" port-forward svc/"${CLUSTER_NAME}"-tidb 4000:4000
```

If the TiDB service name differs from `${CLUSTER_NAME}-tidb`, inspect services:

```bash
kubectl -n "${NAMESPACE}" get svc | rg -i tidb
kubectl -n "${NAMESPACE}" get pods -l "app.kubernetes.io/name=tidb,pingcap.com/cluster=${CLUSTER_NAME}" -o jsonpath='{.items[0].metadata.name}'
```

Then port-forward the matching pod:

```bash
export TIDB_POD=$(kubectl -n "${NAMESPACE}" get pods -l "app.kubernetes.io/name=tidb,pingcap.com/cluster=${CLUSTER_NAME}" -o jsonpath='{.items[0].metadata.name}')
kubectl -n "${NAMESPACE}" port-forward "${TIDB_POD}" 4000:4000
```

## Verify

Open GraphiQL:

http://localhost:8080/graphql

Run a basic GraphQL check:

```bash
curl -s -X POST http://localhost:8080/graphql \
  -H "content-type: application/json" \
  -d '{"query":"query getUsers { users(first: 3) { nodes { id fullName email } } }"}'
```

Connect to TiDB with MySQL client:

```bash
mysql --protocol=TCP --host=127.0.0.1 --port=4000 --user=root
```

Try:

```sql
SELECT 1;
```

## Admin reload endpoint

```bash
curl -i -X POST http://localhost:8080/admin/reload-schema \
  -H "X-Admin-Token: quickstart-admin-token"
```

## Optional local image override

When you build a local image as `tidb-graphql:dev`:

```bash
kubectl -n "${NAMESPACE}" set image deployment/tidb-graphql tidb-graphql=tidb-graphql:dev
```

## Clean up

```bash
kubectl -n "${NAMESPACE}" delete -k examples/k8s/quickstart/manifests
kubectl delete namespace "${NAMESPACE}"
```

## Notes

- The TiDB password is configured in `tidb-graphql` Deployment environment variables.
- Update secret `quickstart-db-credentials` if you use a different DB user or password.
- TiDB is exposed by the v2 operator service name `tidb-tidb` in this quickstart.
- `kubectl get pods -l app.kubernetes.io/name=tidb-graphql -n "${NAMESPACE}"` is the correct pod selector for this scenario.
- This quickstart uses local seed SQL only (`sample-data.sql` + `sample-data-vectors.sql`).
- SQL inputs come from `manifests/sql/sample-data.sql` and `manifests/sql/sample-data-vectors.sql` to keep kustomize generation in a single directory.
