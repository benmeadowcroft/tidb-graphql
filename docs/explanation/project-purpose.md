# Project purpose and point of view

This project started as an exploratory prototype. I wanted to see how far I could get with a SQL-first way of exposing GraphQL operations, with some guardrails in place.

I have liked GraphQL conceptually since my first real exposure to it at Rubrik. It is a great fit for building rich web UIs, especially when you want to shape responses carefully and avoid over-fetching. At the same time, I also really like the power and flexibility of SQL. It is proven, expressive, and already sits behind a lot of production systems.

That tension is part of the motivation here. In many GraphQL stacks, the design starts with GraphQL and then translates to SQL behind the scenes. I wanted to try the inverse: start from SQL, introspect the schema, and let that drive the GraphQL API. There is a lot of SQL already in the world, and it seemed worth exploring whether a SQL-first approach can give teams useful GraphQL access quickly, without handcrafting every resolver.

This project was also inspired by Simon Willison's work on datasette, which showed how far you can get by leaning into the database as the source of truth.

TiDB is the database I know best, and it is designed for scale-out workloads at large companies. In those environments, guardrails matter. I have seen organizations put more constrained clients in front of SQL to avoid expensive queries and keep performance predictable. That idea shows up here too: the system is intentionally constrained, not because I want to limit capability, but because I want the API to be safe enough to use quickly.

I still think handcrafted GraphQL to SQL is the right choice for many production systems. This project is not trying to replace that. It is an attempt to provide rapid access to data with sensible defaults, while exploring what a SQL-first GraphQL interface can look like.

---
# Related Docs

## Next steps
- [Architecture overview](architecture.md)
- [Operations with guardrails](../tutorials/ops-guardrails.md)
- [Your first schema in five minutes](../tutorials/first-schema.md)

## Reference
- [Docs home](../README.md)

## Back
- [Explanation home](README.md)
- [Docs home](../README.md)
