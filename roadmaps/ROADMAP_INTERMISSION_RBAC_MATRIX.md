# Intermission RBAC Completion Matrix

## Scope
Intermission scope from [roadmaps/ROADMAP.md](roadmaps/ROADMAP.md): "The RBAC system."

## Completion Matrix

| Item | Status | Evidence | Notes |
|---|---|---|---|
| RBAC command routing from server direct-command path | Done | [server/src/statement_handler/mod.rs](server/src/statement_handler/mod.rs), [server/src/statement_handler/rbac.rs](server/src/statement_handler/rbac.rs) | RBAC commands are intercepted before SQL AST parse fallback paths. |
| Metastore contract and action dispatch for RBAC operations | Done | [kionas/proto/metastore_service.proto](kionas/proto/metastore_service.proto), [metastore/src/services/metastore_service.rs](metastore/src/services/metastore_service.rs), [metastore/src/services/actions/mod.rs](metastore/src/services/actions/mod.rs) | Additive RPC actions/results for create/delete user/group, create/drop role, and grant role. |
| Metastore persistence schema for RBAC | Done | [scripts/metastore/init.sql](scripts/metastore/init.sql) | RBAC tables/indexes and bootstrap principal/role binding are present. |
| Create and grant happy-path runtime validation | Done | [scripts/client_workflow.sql](scripts/client_workflow.sql), User runtime logs from manual CLI session on 2026-03-19 | Verified CREATE USER/GROUP/ROLE and GRANT ROLE to USER/GROUP all return SUCCESS. |
| Duplicate and not-found validation responses | Done | [metastore/src/services/provider/postgres.rs](metastore/src/services/provider/postgres.rs), User runtime logs from manual CLI session on 2026-03-19 | Verified deterministic VALIDATION responses: already exists, already granted, and not found cases. |
| DELETE ROLE compatibility alias (DROP ROLE equivalent) | Done | [server/src/statement_handler/rbac.rs](server/src/statement_handler/rbac.rs), User runtime logs from manual CLI session on 2026-03-19 | `delete role test_role;` accepted and applied; follow-up `drop role test_role;` returns not found as expected. |
| Query/Flight authorization enforcement integration | Deferred | [roadmaps/ROADMAP.md](roadmaps/ROADMAP.md) | Explicitly deferred to Phase 6 authentication/authorization revamp. |

Status values:
- `Done`
- `Deferred`
- `Blocked`
- `Not Started`

## Signoff Decision
- Intermission RBAC signoff: `Accepted`
- Blocking items:
  - None

## Gate Checklist
1. Intermission mandatory criteria are marked `Done`.
2. Each `Done` item includes concrete evidence reference.
3. `Deferred` items include rationale and are explicitly non-blocking.
4. Final signoff decision is recorded in this file.

## Environment and Parameters
- Environment variables expected for this intermission:
  - Existing runtime connectivity and service configuration in [docker/docker-compose.yaml](docker/docker-compose.yaml) for server/metastore/worker stack.
- Parameters expected for this intermission:
  - Server direct-command RBAC syntax variants:
    - `CREATE USER <username>`
    - `DELETE USER <username>`
    - `CREATE GROUP <group_name>`
    - `DELETE GROUP <group_name>`
    - `CREATE ROLE <role_name>`
    - `DROP ROLE <role_name>`
    - `DELETE ROLE <role_name>`
    - `GRANT ROLE <role_name> TO USER <username>`
    - `GRANT ROLE <role_name> TO GROUP <group_name>`
