# Deployment overview

At a high level, TiDB GraphQL sits between a TiDB database and a web client. It exposes a GraphQL API backed by the live TiDB schema, so the frontend can query the data without talking to the database directly.

## Typical shape

- **Frontend** (web UI or internal tooling)
- **API gateway / reverse proxy** (optional but common)
- **TiDB GraphQL server**
- **TiDB database**

## Why an API gateway helps

An API gateway in front of the GraphQL server is a good default in production because it centralizes a few cross-cutting concerns:

- **TLS termination** and standard request routing
- **CORS policy** for browser clients
- **Rate limiting** and burst control at the edge
- **Auth integration** (token validation or header normalization)
- **Observability** (request logging and tracing at the edge)

This keeps the GraphQL server focused on schema, planning, and execution while the gateway handles traffic management and perimeter concerns. The project does include middleware for these concerns, but the implementations are intentionally simple and focused on the basics.

## The core idea

The goal is simple: keep the database behind the GraphQL server, keep the GraphQL server behind a boundary you control, and give front-end teams a safe, flexible way to query data.

---
# Related Docs

## Next steps
- [Ops guardrails](../tutorials/ops-guardrails.md)
- [Add observability](../how-to/observability.md)

## Reference
- [Configuration reference](../reference/configuration.md)

## Back
- [Explanation home](README.md)
- [Docs home](../README.md)
