# Add observability

Goal: enable metrics, tracing, and SQL commenter with a minimal config.

## 1) Enable observability

```yaml
observability:
  service_name: tidb-graphql
  environment: development

  metrics_enabled: true
  tracing_enabled: true
  sqlcommenter_enabled: true

  logging:
    exports_enabled: true

  otlp:
    endpoint: localhost:4317
```

## 2) Start an OTLP endpoint

For local testing:

```bash
docker run -d --name lgtm \
  -p 3000:3000 -p 4317:4317 \
  grafana/otel-lgtm:latest
```

## 3) Verify outputs

- Metrics: `http://localhost:8080/metrics`
- Traces/logs: check your OTLP backend
- SQL commenter: check TiDB slow query log for trace comments

Tip: start with metrics only, then add tracing once you are happy with the signal volume.

---
# Related Docs

## Next steps
- [Use config precedence correctly](config-precedence.md)
- [Run integration tests locally](integration-tests.md)

## Reference
- [Configuration reference](../reference/configuration.md)

## Further reading
- [Observability architecture](../explanation/observability-architecture.md)

## Back
- [How-to home](README.md)
- [Docs home](../README.md)
