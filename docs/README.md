# TiDB GraphQL Documentation

This documentation is organized into four types of content. Pick the one that matches your intent and you will get what you need faster.

## Compose scenarios
If you need Docker/Podman startup patterns, use the scenario guide:

- [Compose scenarios](../examples/compose/README.md)

## Tutorials
If you want to get something working end-to-end and understand it as you go.

- [Your first schema in five minutes](tutorials/first-schema.md)
- [Secure local dev with OIDC/JWKS](tutorials/local-oidc.md)
- [Query with filters and unique keys](tutorials/query-basics.md)
- [Mutate data with typed results](tutorials/mutation-basics.md)
- [Operations with guardrails](tutorials/ops-guardrails.md)

## How-To Guides
If you already know what you want to do, and just need the steps.

- [Configure OIDC/JWKS auth](how-to/oidc-jwks-auth.md)
- [DB user for roles](how-to/db-user-for-roles.md)
- [Refresh schema safely](how-to/schema-refresh.md)
- [Authenticate to TiDB](how-to/database-auth.md)
- [Add observability](how-to/observability.md)
- [Configure CORS for browser clients](how-to/cors.md)
- [Run integration tests locally](how-to/integration-tests.md)
- [Use config precedence correctly](how-to/config-precedence.md)
- [Troubleshoot first run](how-to/troubleshooting-first-run.md)

## Technical Reference
If you need authoritative, detailed, and complete information.

- [Reference index](reference/README.md)
- [Configuration reference](reference/configuration.md)
- [Flags and environment variables](reference/cli-env.md)
- [GraphQL schema mapping](reference/graphql-schema.md)
- [Filter language](reference/filters.md)
- [Schema filters](reference/schema-filters.md)
- [HTTP endpoints](reference/endpoints.md)
- [Auth reference](reference/auth.md)
- [Observability reference](reference/observability.md)

## Explanation
If you want the "why" behind the design choices.

- [Project purpose and point of view](explanation/project-purpose.md)
- [Architecture overview](explanation/architecture.md)
- [Observability architecture](explanation/observability-architecture.md)
- [Middleware architecture](explanation/middleware-architecture.md)
- [Deployment overview](explanation/deployment-overview.md)
- [Schema handling: build, snapshot, refresh](explanation/schema-handling.md)
- [Query planning as a seam](explanation/query-planning.md)
- [Mutation result design and rationale](explanation/mutation-design.md)
- [Resolver batching to avoid N+1](explanation/resolver-batching.md)
- [Performance constraints as safety rails](explanation/performance-constraints.md)
- [Database-first authorization](explanation/database-first-auth.md)
- [Auth architecture](explanation/auth-architecture.md)
