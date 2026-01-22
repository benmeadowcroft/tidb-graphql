# Observability architecture

Observability is a first-class feature in this project because it sits between GraphQL and a production database. If something goes wrong, you want to know which query, which resolver, and which SQL statement caused the issue.

Personally, I also wanted a practical way to dive deeper into tracing with this project. OpenTelemetry was the obvious choice: broad ecosystem coverage, easy backend switching, and a consistent API across signals. It let me experiment with tracing end-to-end without committing to a single vendor.

## What we expose and why

- **Metrics** provide low-cost, always-on visibility into request volume, latency, depth, and error rates.
- **Tracing** gives you a causal path from HTTP request to GraphQL execution to SQL query.
- **SQL commenter** adds trace context directly into SQL so you can correlate slow queries back to the originating request.
- **Logs** stay structured to make correlation easier, even if you are not using a tracing backend.

## Design choices

- OpenTelemetry is the common spine so metrics, traces, and logs use the same resource identity.
- Metrics are on by default because they are lightweight and essential for guardrails.
- Tracing is opt-in because it is more expensive and not always needed.
- SQL commenter is opt-in and depends on tracing.
- Log export is opt-in, logs to stdout are always on

## Practical implication

This setup lets you start with low-friction metrics, then add tracing when you need deeper insight, without changing the rest of the system.

---
# Related Docs

## Next steps
- [Add observability](../how-to/observability.md)
- [Ops guardrails](../tutorials/ops-guardrails.md)

## Reference
- [Configuration reference](../reference/configuration.md)

## Back
- [Explanation home](README.md)
- [Docs home](../README.md)
