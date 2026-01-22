# Operations with guardrails

Goal: turn on a few operational safeguards without turning your config into a novel.

## 1) Add health checks

Health endpoint is enabled by default:

```
GET /health
```

Use this for load balancers and orchestration readiness probes.

## 2) Cap query depth

```yaml
server:
  graphql_max_depth: 5
```

This prevents pathological query shapes from walking your schema too deep.

## 3) Rate limit the API

```yaml
server:
  rate_limit_enabled: true
  rate_limit_rps: 10
  rate_limit_burst: 20
```

This applies globally to all endpoints.

## 4) Enable CORS (only if needed)

```yaml
server:
  cors_enabled: true
  cors_allowed_origins:
    - "http://localhost:3000"
  cors_allowed_methods: ["GET", "POST", "OPTIONS"]
  cors_allowed_headers: ["Content-Type", "Authorization"]
  cors_allow_credentials: true
```

If you can solve CORS at the edge, that is usually cleaner.

## 5) Graceful shutdown

The server already handles SIGTERM/SIGINT. The point here is not to configure it, but to remember it exists when you wire it into orchestration.

---
# Related Docs

## Next steps
- [Observability](../how-to/observability.md)
- [Use config precedence correctly](../how-to/config-precedence.md)

## Reference
- [Configuration reference](../reference/configuration.md)
- [Flags and environment variables](../reference/cli-env.md)

## Further reading
- [Performance constraints](../explanation/performance-constraints.md)
- [Observability architecture](../explanation/observability-architecture.md)

## Back
- [Tutorials home](README.md)
- [Docs home](../README.md)
