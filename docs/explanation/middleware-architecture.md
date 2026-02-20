# Middleware architecture

Middleware is where we enforce cross-cutting behavior that should be consistent for every request. It sits between the HTTP server and the GraphQL handler so that auth, rate limiting, logging, and instrumentation are applied in a predictable order.

## What lives in middleware

- **OIDC/JWKS auth**: validate JWTs for `/graphql` and admin endpoints.
- **DB role activation**: map a JWT claim to `SET ROLE` on the database.
- **Rate limiting**: guardrail against overload or accidental abuse.
- **CORS**: explicit, opt-in browser access.
- **Logging**: consistent request logging with context.

## Design choices

- Middleware keeps policy separate from business logic in resolvers.
- Authentication is centralized, but admin endpoint auth mode can differ from GraphQL when OIDC is disabled (shared admin token).
- Role mapping is explicit so database permissions remain the source of truth.
- DB role activation requires OIDC to be enabled so requests carry a validated JWT.

## Practical implication

By keeping these concerns in middleware, the resolver layer stays focused on data shape and query planning, and the system stays easier to reason about.

---
# Related Docs

## Next steps
- [Configure OIDC/JWKS auth](../how-to/oidc-jwks-auth.md)
- [DB user for roles](../how-to/db-user-for-roles.md)

## Reference
- [Auth reference](../reference/auth.md)

## Further reading
- [Auth architecture](auth-architecture.md)

## Back
- [Explanation home](README.md)
- [Docs home](../README.md)
