# Phase 6 Auth Flow and Contracts

## Scope
Phase 6 secures query execution and Flight retrieval paths by propagating RBAC context over gRPC metadata, enforcing worker-side authorization, using signed Flight tickets, and applying bounded-staleness token revocation checks.

## Metadata Contract
The following metadata keys are propagated from warehouse dispatch to worker query execution:

- `authorization`: `Bearer <jwt_token>`
- `session_id`: active session id
- `rbac_user`: authenticated RBAC principal
- `rbac_role`: effective RBAC role
- `auth_scope`: server-computed trusted scope string (for example `select:db.schema.table`)
- `query_id`: correlation id for tracing

## Query Dispatch Flow
1. Warehouse request interceptor validates JWT and sets `rbac_user`.
2. Request context captures token/user/session/role.
3. Server computes a trusted scope from query namespace (`select:db.schema.table`).
4. Server dispatch helper forwards auth metadata to worker `execute_task`.
5. Worker validates token, subject binding, role/session consistency, and revocation cache.
6. Worker enforces scope before executing query operators.

## Flight Retrieval Flow
1. Worker issues signed ticket claims for query handles and FlightInfo responses.
2. Signed ticket claims include: `sid`, `tid`, `wid`, `sub`, `role`, `scope`, `iat`, `exp`.
3. Flight proxy forwards auth metadata to worker Flight endpoints when present.
4. Worker Flight `DoGet` verifies signed ticket and validates metadata-bound principal/session when metadata is supplied.

## Revocation Strategy
- Provider: in-memory backend interface (`InMemoryAuthBackendProvider`) with session-role and revoked-token state.
- Cache: bounded-staleness revocation cache in worker authorizer.
- TTL control: `AUTH_REVOCATION_CACHE_TTL_SECONDS` (default 15s).

## Configuration
- `JWT_SECRET`: signing/verification secret for worker token checks and signed Flight tickets.
- `FLIGHT_TICKET_TTL_SECONDS`: signed Flight ticket expiry window.
- `AUTH_REVOCATION_CACHE_TTL_SECONDS`: revocation cache TTL.

## Notes
- Query and Flight paths are covered in this phase.
- Transaction RPC auth enforcement remains out of scope for Phase 6.
