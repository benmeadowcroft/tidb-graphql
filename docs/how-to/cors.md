# Configure CORS for browser clients

Goal: allow a browser app to call the GraphQL endpoint.

## 1) Decide where to handle CORS

If you have an API gateway or reverse proxy, set CORS there. Otherwise, enable it in the server.

## 2) Enable CORS in the server

```yaml
server:
  cors_enabled: true
  cors_allowed_origins:
    - "http://localhost:3000"
  cors_allowed_methods: ["GET", "POST", "OPTIONS"]
  cors_allowed_headers: ["Content-Type", "Authorization"]
  cors_allow_credentials: true
```

## 3) Use environment variables (if preferred)

```bash
export TIGQL_SERVER_CORS_ENABLED=true
export TIGQL_SERVER_CORS_ALLOWED_ORIGINS=http://localhost:3000
```

## 4) Verify from the browser

A failed preflight is almost always a missing origin, method, or header.

---
# Related Docs

## Next steps
- [Use config precedence correctly](config-precedence.md)
- [Add observability](observability.md)

## Reference
- [Configuration reference](../reference/configuration.md)

## Further reading
- [Middleware architecture](../explanation/middleware-architecture.md)

## Back
- [How-to home](README.md)
- [Docs home](../README.md)
