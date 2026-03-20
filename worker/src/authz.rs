use jsonwebtoken::{Algorithm, DecodingKey, EncodingKey, Header, Validation, decode, encode};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tonic::metadata::MetadataMap;

const DEFAULT_REVOCATION_TTL_SECONDS: u64 = 15;
const DEFAULT_FLIGHT_TICKET_TTL_SECONDS: i64 = 300;

/// What: Structured auth metadata propagated from the warehouse service.
///
/// Inputs:
/// - Values are read from incoming gRPC metadata headers.
///
/// Output:
/// - Parsed and normalized auth context used for worker authorization.
///
/// Details:
/// - This context is trusted as server-issued metadata once worker has authenticated the server hop.
#[derive(Debug, Clone)]
pub(crate) struct DispatchAuthContext {
    pub(crate) session_id: String,
    pub(crate) rbac_user: String,
    pub(crate) rbac_role: String,
    pub(crate) auth_scope: String,
    pub(crate) query_id: String,
}

/// What: Claims embedded into signed internal Flight tickets.
///
/// Inputs:
/// - Issued from worker query or FlightInfo paths.
///
/// Output:
/// - Deterministic signed claims consumed by DoGet verification.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub(crate) struct FlightTicketClaims {
    pub(crate) sid: String,
    pub(crate) tid: String,
    pub(crate) wid: String,
    pub(crate) sub: String,
    pub(crate) role: String,
    pub(crate) scope: String,
    pub(crate) exp: usize,
    pub(crate) iat: usize,
}

/// What: In-memory provider for role/session bindings and token revocation state.
///
/// Inputs:
/// - Session-role and revoked-token values are managed in-memory.
///
/// Output:
/// - Async checks used by worker authorization logic.
///
/// Details:
/// - Designed as a Phase 6 interface seam; can be replaced by external providers later.
#[derive(Debug, Default)]
pub(crate) struct InMemoryAuthBackendProvider {
    session_roles: Arc<RwLock<HashMap<String, String>>>,
    revoked_tokens: Arc<RwLock<HashSet<String>>>,
}

impl InMemoryAuthBackendProvider {
    /// What: Record or update an effective role for a session.
    ///
    /// Inputs:
    /// - `session_id`: Session identifier.
    /// - `role`: Effective RBAC role.
    ///
    /// Output:
    /// - Persists role mapping in-memory.
    pub(crate) async fn upsert_session_role(&self, session_id: &str, role: &str) {
        let mut guard = self.session_roles.write().await;
        guard.insert(session_id.to_string(), role.to_string());
    }

    /// What: Check whether token is revoked.
    ///
    /// Inputs:
    /// - `token`: Bearer token string.
    ///
    /// Output:
    /// - `true` when revoked.
    pub(crate) async fn is_token_revoked(&self, token: &str) -> bool {
        let guard = self.revoked_tokens.read().await;
        guard.contains(token)
    }

    /// What: Validate session-role mapping if one is already known.
    ///
    /// Inputs:
    /// - `session_id`: Session identifier.
    /// - `role`: Role asserted by request metadata.
    ///
    /// Output:
    /// - `true` when role is accepted.
    ///
    /// Details:
    /// - Unknown sessions are treated as acceptable for this phase to avoid false denials.
    pub(crate) async fn validate_session_role(&self, session_id: &str, role: &str) -> bool {
        let guard = self.session_roles.read().await;
        match guard.get(session_id) {
            Some(expected) => expected == role,
            None => true,
        }
    }
}

#[derive(Debug, Default)]
struct RevocationCache {
    entries: RwLock<HashMap<String, (bool, Instant)>>,
}

impl RevocationCache {
    async fn get(&self, token: &str, ttl: Duration) -> Option<bool> {
        let guard = self.entries.read().await;
        guard.get(token).and_then(|(value, stamp)| {
            if stamp.elapsed() <= ttl {
                Some(*value)
            } else {
                None
            }
        })
    }

    async fn set(&self, token: &str, revoked: bool) {
        let mut guard = self.entries.write().await;
        guard.insert(token.to_string(), (revoked, Instant::now()));
    }
}

/// What: Runtime authorizer used by worker query and Flight entrypoints.
///
/// Inputs:
/// - Token/session metadata and scope strings.
///
/// Output:
/// - Authorization decisions and signed ticket utilities.
#[derive(Debug, Default)]
pub(crate) struct WorkerAuthorizer {
    provider: InMemoryAuthBackendProvider,
    revocation_cache: RevocationCache,
}

impl WorkerAuthorizer {
    /// What: Construct worker authorizer with in-memory provider.
    ///
    /// Inputs:
    /// - None.
    ///
    /// Output:
    /// - Ready-to-use authorizer instance.
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// What: Parse and validate dispatch auth context from request metadata.
    ///
    /// Inputs:
    /// - `metadata`: Incoming gRPC metadata map.
    /// - `fallback_session_id`: Session id from protobuf request body.
    ///
    /// Output:
    /// - Parsed dispatch auth context.
    ///
    /// Details:
    /// - Fails when required headers are missing or malformed.
    pub(crate) fn extract_dispatch_context(
        &self,
        metadata: &MetadataMap,
        fallback_session_id: &str,
    ) -> Result<DispatchAuthContext, tonic::Status> {
        let session_id = metadata
            .get("session_id")
            .and_then(|v| v.to_str().ok())
            .map(str::trim)
            .filter(|v| !v.is_empty())
            .unwrap_or(fallback_session_id)
            .to_string();

        if session_id.trim().is_empty() {
            return Err(tonic::Status::unauthenticated(
                "missing session_id metadata",
            ));
        }

        let rbac_user = metadata
            .get("rbac_user")
            .and_then(|v| v.to_str().ok())
            .map(str::trim)
            .filter(|v| !v.is_empty())
            .ok_or_else(|| tonic::Status::permission_denied("missing rbac_user metadata"))?
            .to_string();

        let rbac_role = metadata
            .get("rbac_role")
            .and_then(|v| v.to_str().ok())
            .map(str::trim)
            .filter(|v| !v.is_empty())
            .ok_or_else(|| tonic::Status::permission_denied("missing rbac_role metadata"))?
            .to_string();

        let auth_scope = metadata
            .get("auth_scope")
            .and_then(|v| v.to_str().ok())
            .map(str::trim)
            .filter(|v| !v.is_empty())
            .ok_or_else(|| tonic::Status::permission_denied("missing auth_scope metadata"))?
            .to_string();

        let query_id = metadata
            .get("query_id")
            .and_then(|v| v.to_str().ok())
            .map(str::to_string)
            .unwrap_or_default();

        Ok(DispatchAuthContext {
            session_id,
            rbac_user,
            rbac_role,
            auth_scope,
            query_id,
        })
    }

    /// What: Validate server-propagated dispatch identity and role mapping.
    ///
    /// Inputs:
    /// - `ctx`: Parsed dispatch context.
    ///
    /// Output:
    /// - `Ok(())` when context is accepted.
    pub(crate) async fn validate_dispatch_context(
        &self,
        ctx: &DispatchAuthContext,
    ) -> Result<(), tonic::Status> {
        if ctx.rbac_user.trim().is_empty() {
            return Err(tonic::Status::permission_denied(
                "rbac_user cannot be empty",
            ));
        }

        if !self
            .provider
            .validate_session_role(&ctx.session_id, &ctx.rbac_role)
            .await
        {
            return Err(tonic::Status::permission_denied(
                "session role does not match propagated role",
            ));
        }

        let ttl = Duration::from_secs(revocation_ttl_seconds());
        let cache_key = format!("{}:{}", ctx.session_id, ctx.rbac_role);
        if self.revocation_cache.get(&cache_key, ttl).await.is_none() {
            let revoked = self.provider.is_token_revoked(&cache_key).await;
            self.revocation_cache.set(&cache_key, revoked).await;
            if revoked {
                return Err(tonic::Status::unauthenticated(
                    "dispatch context is revoked",
                ));
            }
        }

        self.provider
            .upsert_session_role(&ctx.session_id, &ctx.rbac_role)
            .await;

        Ok(())
    }

    /// What: Validate that server-computed query scope authorizes table access.
    ///
    /// Inputs:
    /// - `scope`: Scope string propagated by warehouse server.
    /// - `database`: Query database.
    /// - `schema`: Query schema.
    /// - `table`: Query table.
    ///
    /// Output:
    /// - `Ok(())` when scope grants access.
    pub(crate) fn validate_query_scope(
        &self,
        scope: &str,
        database: &str,
        schema: &str,
        table: &str,
    ) -> Result<(), tonic::Status> {
        let expected = format!("select:{}.{}.{}", database, schema, table);
        if scope == expected {
            return Ok(());
        }

        if scope == "select:*" || scope == "select:*.*.*" {
            return Ok(());
        }

        Err(tonic::Status::permission_denied(format!(
            "scope does not authorize query target: expected '{}'",
            expected
        )))
    }

    /// What: Issue a signed Flight ticket with scoped claims.
    ///
    /// Inputs:
    /// - `ctx`: Dispatch auth context.
    /// - `task_id`: Task identifier.
    /// - `worker_id`: Worker identifier.
    ///
    /// Output:
    /// - Signed compact JWT ticket.
    pub(crate) fn issue_signed_flight_ticket(
        &self,
        ctx: &DispatchAuthContext,
        task_id: &str,
        worker_id: &str,
    ) -> Result<String, tonic::Status> {
        let now = chrono::Utc::now().timestamp();
        let exp = now.saturating_add(flight_ticket_ttl_seconds());
        let claims = FlightTicketClaims {
            sid: ctx.session_id.clone(),
            tid: task_id.to_string(),
            wid: worker_id.to_string(),
            sub: ctx.rbac_user.clone(),
            role: ctx.rbac_role.clone(),
            scope: ctx.auth_scope.clone(),
            iat: now as usize,
            exp: exp as usize,
        };

        encode(
            &Header::new(Algorithm::HS256),
            &claims,
            &EncodingKey::from_secret(signing_secret().as_bytes()),
        )
        .map_err(|e| tonic::Status::internal(format!("failed to sign flight ticket: {}", e)))
    }

    /// What: Verify a signed Flight ticket and return trusted claims.
    ///
    /// Inputs:
    /// - `ticket`: Compact JWT ticket string.
    ///
    /// Output:
    /// - Verified ticket claims.
    pub(crate) fn verify_signed_flight_ticket(
        &self,
        ticket: &str,
    ) -> Result<FlightTicketClaims, tonic::Status> {
        decode::<FlightTicketClaims>(
            ticket,
            &DecodingKey::from_secret(signing_secret().as_bytes()),
            &Validation::new(Algorithm::HS256),
        )
        .map(|d| d.claims)
        .map_err(|_| tonic::Status::unauthenticated("invalid or expired flight ticket"))
    }
}

fn configured_jwt_secret() -> Option<String> {
    std::env::var("INTERNAL_DISPATCH_SIGNING_SECRET")
        .ok()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
        .or_else(|| {
            std::env::var("JWT_SECRET")
                .ok()
                .map(|v| v.trim().to_string())
                .filter(|v| !v.is_empty())
        })
}

fn signing_secret() -> String {
    configured_jwt_secret().unwrap_or_else(|| "phase6-worker-ticket-secret".to_string())
}

fn revocation_ttl_seconds() -> u64 {
    std::env::var("AUTH_REVOCATION_CACHE_TTL_SECONDS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(DEFAULT_REVOCATION_TTL_SECONDS)
}

fn flight_ticket_ttl_seconds() -> i64 {
    std::env::var("FLIGHT_TICKET_TTL_SECONDS")
        .ok()
        .and_then(|v| v.parse::<i64>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(DEFAULT_FLIGHT_TICKET_TTL_SECONDS)
}

#[cfg(test)]
mod tests {
    use super::WorkerAuthorizer;
    use tonic::metadata::MetadataValue;

    #[tokio::test]
    async fn rejects_missing_auth_metadata() {
        let authz = WorkerAuthorizer::new();
        let metadata = tonic::metadata::MetadataMap::new();
        let err = authz
            .extract_dispatch_context(&metadata, "s1")
            .expect_err("must reject missing metadata");
        assert_eq!(err.code(), tonic::Code::PermissionDenied);
    }

    #[tokio::test]
    async fn validates_signed_ticket_round_trip() {
        let authz = WorkerAuthorizer::new();
        let mut metadata = tonic::metadata::MetadataMap::new();
        metadata.insert("session_id", MetadataValue::from_static("s1"));
        metadata.insert("rbac_user", MetadataValue::from_static("alice"));
        metadata.insert("rbac_role", MetadataValue::from_static("ADMIN"));
        metadata.insert("auth_scope", MetadataValue::from_static("select:*"));

        let ctx = authz
            .extract_dispatch_context(&metadata, "s1")
            .expect("must extract context");

        let ticket = authz
            .issue_signed_flight_ticket(&ctx, "t1", "worker1")
            .expect("ticket must sign");
        let claims = authz
            .verify_signed_flight_ticket(&ticket)
            .expect("ticket must verify");

        assert_eq!(claims.sid, "s1");
        assert_eq!(claims.tid, "t1");
        assert_eq!(claims.wid, "worker1");
    }
}
