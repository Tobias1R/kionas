## Plan: RBAC Intermission Scaffold

Build a functional RBAC scaffold as an intermission before Phase 6, with metastore-owned persistence and server command routing. Scope includes SQL command handling for CREATE USER, DELETE USER, CREATE GROUP, DELETE GROUP, CREATE ROLE, DROP ROLE, and GRANT ROLE (to users and groups). Design is intentionally minimal but production-shaped so Phase 6 can layer authentication context propagation and enforcement without rework.

**Steps**
1. Phase A: Contract and Schema Baseline (blocks later phases)
1.1 Define RBAC domain model and naming conventions: user, group, role, principal_type (user/group), and role bindings. Depends on current metastore model patterns in metastore service proto and provider trait.
1.2 Extend metastore RPC contract with additive oneof actions/results for CreateUser, DeleteUser, CreateGroup, DeleteGroup, CreateRole, DropRole, GrantRole. Depends on 1.1.
1.3 Design and add metastore SQL schema tables for RBAC with idempotent CREATE TABLE statements, unique constraints, and indexes. Depends on 1.1.
1.4 Regenerate protobuf bindings and ensure server/metastore compile against additive contract without changing existing operations. Depends on 1.2.

2. Phase B: Metastore RBAC Implementation (depends on Phase A)
2.1 Extend metastore provider trait with RBAC CRUD/grant methods and response models matching new proto messages. Depends on 1.2.
2.2 Implement RBAC SQL operations in postgres provider with clear error messages for duplicate/missing entities and invalid grants. Depends on 2.1 and 1.3.
2.3 Add metastore action handlers for each RBAC operation and route them in Execute dispatcher. Depends on 2.1.
2.4 Add lightweight validation rules in metastore layer: reserved names, principal existence checks, and no duplicate role grants. Depends on 2.2 and 2.3.

3. Phase C: Server SQL Routing and Orchestration (depends on Phase B)
3.1 Add new statement handler modules in server for RBAC commands and register them in central statement router. Depends on parser support for target statements.
3.2 Implement command-to-metastore orchestration path (no worker task dispatch) for CREATE USER, DELETE USER, CREATE GROUP, DELETE GROUP, CREATE ROLE, DROP ROLE, GRANT ROLE. Depends on 3.1 and 2.3.
3.3 Introduce consistent structured outcome mapping (SUCCESS/VALIDATION/INFRA with stable codes) aligned with current server response contract. Depends on 3.2.
3.4 Add fallback direct-command support only if parser coverage is missing for any RBAC statement variant. Depends on parser capability checks in 3.1.

4. Phase D: Tie-In to Upcoming Phase 6 Authentication (parallel with late Phase C)
4.1 Define auth context seam: how session creation can fetch role/group assignments from metastore without enabling full enforcement yet. Depends on 2.2.
4.2 Add read-side helper contract in server auth/auth_setup components for future role hydration during login. Depends on 4.1.
4.3 Document explicit boundary: this intermission ships RBAC management and persistence only; query/flight enforcement remains Phase 6 work. Depends on 4.2.

5. Phase E: Validation and Signoff Artifacts
5.1 Add focused tests for metastore RBAC operations (create/delete/grant happy paths and expected failures).
5.2 Add server statement-handler tests for each new command route and outcome-code behavior.
5.3 Run quality gates required by repo process for touched scope (fmt, clippy, check, tests).
5.4 Create and maintain roadmap matrix artifacts for this intermission and its tie-in acceptance, including environment variables and parameters.

**Relevant files**
- c:/code/kionas/server/src/statement_handler/mod.rs — add RBAC command routing arms and module exports.
- c:/code/kionas/server/src/statement_handler/helpers.rs — reuse session/metastore access patterns and shared outcome utilities.
- c:/code/kionas/server/src/services/metastore_client.rs — invoke additive metastore Execute actions for RBAC.
- c:/code/kionas/server/src/services/warehouse_service_server.rs — preserve structured response mapping behavior.
- c:/code/kionas/server/src/auth_setup.rs — define near-future role-hydration seam for Phase 6.
- c:/code/kionas/server/src/auth/rbac.rs — host future role/group-aware auth model extensions.
- c:/code/kionas/kionas/proto/metastore_service.proto — additive RBAC request/response and oneof action/result wiring.
- c:/code/kionas/metastore/src/services/metastore_service.rs — route RBAC actions in Execute dispatcher.
- c:/code/kionas/metastore/src/services/provider/mod.rs — extend MetastoreProvider trait for RBAC methods.
- c:/code/kionas/metastore/src/services/provider/postgres.rs — implement RBAC CRUD and grants in SQL.
- c:/code/kionas/metastore/src/services/actions/ — add RBAC action handlers.
- c:/code/kionas/scripts/metastore/init.sql — add RBAC tables and indexes.
- c:/code/kionas/scripts/metastore/init_db_docker.sh — ensure idempotent initialization flow remains valid.
- c:/code/kionas/roadmaps/ROADMAP.md — intermission progress tracking.

**Metastore RBAC Schema Scaffold**
1. users_rbac: id, username (unique), can_login, created_at, updated_at.
2. groups: id, group_name (unique), created_at, updated_at.
3. roles: id, role_name (unique), description, created_at, updated_at.
4. group_memberships: user_id FK, group_id FK, unique(user_id, group_id).
5. role_bindings: principal_type (user/group), principal_id, role_id FK, granted_by, created_at, unique(principal_type, principal_id, role_id).
6. User kionas is reserved and cannot be created or deleted via SQL commands; it is bootstrapped in init.sql with ADMIN role.
7. RBAC users will be tied to authentication context in Phase 6, but for this scaffold, they are purely for role management and have no direct auth implications yet.

**Verification**
1. Contract verification
1.1 Proto generation succeeds and server/metastore compile with additive RPCs.
1.2 Existing metastore actions remain backward compatible.
2. Metastore behavior verification
2.1 CREATE USER/GROUP/ROLE creates rows with uniqueness enforced.
2.2 DELETE USER/GROUP/ROLE removes rows with deterministic handling of dependent bindings.
2.3 GRANT ROLE validates principal and role existence and prevents duplicates.
3. Server behavior verification
3.1 Each target command routes through statement handler and reaches metastore operation.
3.2 Structured outcome codes map correctly for success, duplicate, not-found, and invalid grant cases.
4. Intermission acceptance run
4.1 Manual command sequence: CREATE USER, CREATE GROUP, CREATE ROLE, GRANT ROLE TO USER/GROUP, DELETE USER, DELETE GROUP.
4.2 Capture evidence in matrix with concrete file and output references.

**Decisions**
- Include CREATE ROLE and DROP ROLE in scope now.
- GRANT ROLE supports both user and group principals in this scaffold.
- RBAC persistence is metastore-only; no server-side parquet persistence for RBAC state.
- Server acts as command router/orchestrator; metastore is source of truth.
- Enforcement in query and Flight paths is explicitly excluded from this intermission and reserved for Phase 6.

**Further Considerations**
1. Principal deletion semantics: hard delete with cascading bindings versus soft delete. Recommendation: hard delete + ON DELETE CASCADE for scaffold simplicity.
2. Bootstrap role strategy: create default ADMIN role and bootstrap admin user during init versus first-command setup. Recommendation: bootstrap ADMIN role in init.sql.
3. Parser fallback policy: if SQL parser lacks one or more RBAC statements, introduce direct-command parser only for missing statements while keeping AST-first approach as default.
