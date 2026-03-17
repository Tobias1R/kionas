use chrono::{DateTime, Utc};
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::services::metastore_client::MetastoreClient;
use crate::services::metastore_client::metastore_service as ms;
use crate::warehouse::state::SharedData;
use tokio::time::sleep;
use tokio::time::{Duration, timeout};

/// Transaction states for the Maestro coordinator.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TransactionState {
    Preparing,
    Prepared,
    Committing,
    Committed,
    Aborted,
}

/// Participant in a distributed transaction (typically a worker)
#[derive(Clone, Debug)]
pub struct Participant {
    pub id: String,
    /// Worker pool key or address
    pub target: String,
    /// Staging prefix where the participant should write staged objects
    pub staging_prefix: String,
}

/// Lightweight transaction descriptor
#[derive(Clone, Debug)]
pub struct Transaction {
    pub tx_id: String,
    pub state: TransactionState,
    pub participants: Vec<Participant>,
    pub created_at: DateTime<Utc>,
}

/// Maestro: the server-side coordinator for 2PC-lite transactions.
///
/// Strategy: staging + manifest promotion. Maestro instructs participants
/// to `prepare` (stage objects under `staging_prefix`), waits for all
/// to report success, persists a transaction record in the metastore,
/// then instructs participants to `commit` (promote staged objects by
/// updating the manifest/pointer object). On any failure Maestro aborts.
pub struct Maestro {
    shared: SharedData,
    // In-memory active transactions; persisted state should live in metastore.
    active: Arc<RwLock<Vec<Transaction>>>,
}

impl Maestro {
    /// Create a new Maestro instance bound to the shared server state.
    pub fn new(shared: SharedData) -> Self {
        Self {
            shared,
            active: Arc::new(RwLock::new(Vec::new())),
        }
    }

    /// High-level execution: begin -> prepare (with retries/backoff) -> commit
    /// Returns tx_id on success.
    pub async fn execute_transaction(
        &self,
        participants: Vec<Participant>,
        max_retries: u8,
        overall_timeout_secs: u64,
    ) -> Result<String, String> {
        // Begin transaction and persist PREPARING
        let tx_id = self.begin_transaction(participants.clone()).await?;

        // Clone tx_id for use inside the async move closure to avoid moving original
        let tx_id_for_closure = tx_id.clone();

        // Wrap prepare+commit in an overall timeout
        let overall = Duration::from_secs(overall_timeout_secs);
        let this = self;
        let op = async move {
            // Attempt prepare with retries
            let mut attempt: u8 = 0;
            loop {
                attempt += 1;
                match this.prepare_participants(&tx_id_for_closure).await {
                    Ok(_) => break,
                    Err(e) => {
                        if attempt > max_retries {
                            // give up and abort
                            let _ = this.abort(&tx_id_for_closure).await;
                            return Err(format!(
                                "prepare failed after {} attempts: {}",
                                attempt - 1,
                                e
                            ));
                        } else {
                            // backoff (exponential)
                            let backoff_ms = 250u64.checked_shl(attempt as u32).unwrap_or(1000);
                            sleep(Duration::from_millis(backoff_ms)).await;
                            continue;
                        }
                    }
                }
            }

            // If prepared, commit
            match this.commit(&tx_id_for_closure).await {
                Ok(_) => Ok(tx_id_for_closure.clone()),
                Err(e) => {
                    // on commit failure, attempt abort
                    let _ = this.abort(&tx_id_for_closure).await;
                    Err(format!("commit failed: {}", e))
                }
            }
        };

        match timeout(overall, op).await {
            Ok(res) => res,
            Err(_) => {
                let _ = self.abort(&tx_id).await;
                Err("transaction timed out".to_string())
            }
        }
    }

    /// Begin a new transaction with the given participants.
    /// This creates an in-memory Transaction and returns the tx id.
    pub async fn begin_transaction(
        &self,
        participants: Vec<Participant>,
    ) -> Result<String, String> {
        let tx_id = uuid::Uuid::new_v4().to_string();
        let tx = Transaction {
            tx_id: tx_id.clone(),
            state: TransactionState::Preparing,
            participants,
            created_at: Utc::now(),
        };
        {
            let mut active = self.active.write().await;
            active.push(tx);
        }
        // Persist a transaction record to metastore (PREPARING)
        let mut client = match MetastoreClient::connect_with_shared(&self.shared).await {
            Ok(c) => c,
            Err(e) => {
                // remove from active list on failure
                let mut active = self.active.write().await;
                active.retain(|t| t.tx_id != tx_id);
                return Err(format!("failed to connect to metastore: {}", e));
            }
        };

        // Build proto participants
        let ms_participants: Vec<ms::Participant> = {
            let active = self.active.read().await;
            // find the transaction we just added
            match active.iter().find(|t| t.tx_id == tx_id) {
                Some(t) => t
                    .participants
                    .iter()
                    .map(|p| ms::Participant {
                        id: p.id.clone(),
                        target: p.target.clone(),
                        staging_prefix: p.staging_prefix.clone(),
                    })
                    .collect(),
                None => Vec::new(),
            }
        };

        let mreq = ms::MetastoreRequest {
            action: Some(ms::metastore_request::Action::CreateTransaction(
                ms::CreateTransactionRequest {
                    tx_id: tx_id.clone(),
                    participants: ms_participants,
                },
            )),
        };

        match client.execute(mreq).await {
            Ok(_resp) => {
                // If metastore accepted, return tx id. We could update in-memory state from response.transaction
                Ok(tx_id)
            }
            Err(e) => {
                let mut active = self.active.write().await;
                active.retain(|t| t.tx_id != tx_id);
                Err(format!("metastore create_transaction failed: {}", e))
            }
        }
    }

    /// Instruct participants to prepare (stage) their changes.
    /// Returns Ok(()) if all participants prepared successfully.
    pub async fn prepare_participants(&self, tx_id: &str) -> Result<(), String> {
        // Lookup transaction
        let participants = {
            let active = self.active.read().await;
            match active.iter().find(|t| t.tx_id == tx_id) {
                Some(t) => t.participants.clone(),
                None => return Err(format!("transaction {} not found", tx_id)),
            }
        };

        // For each participant, acquire pooled connection and call Prepare
        for p in participants.iter() {
            // Resolve or create pool
            let pool = {
                let state = self.shared.lock().await;
                match state.get_or_create_pool_for_key(&p.target).await {
                    Ok(pool) => pool,
                    Err(e) => return Err(format!("failed to get pool for {}: {}", p.target, e)),
                }
            };

            // Acquire a connection (validates heartbeat)
            let conn = match crate::workers::acquire_channel_with_heartbeat(&pool, &p.target, 10)
                .await
            {
                Ok(c) => c,
                Err(e) => return Err(format!("failed to acquire conn for {}: {}", p.target, e)),
            };

            // Build PrepareRequest
            let prep_req = crate::services::worker_service_client::worker_service::PrepareRequest {
                tx_id: tx_id.to_string(),
                session_id: String::new(),
                staging_prefix: p.staging_prefix.clone(),
                tasks: Vec::new(),
            };

            // Call prepare with timeout
            match crate::workers::send_prepare_to_worker(conn, prep_req, 20).await {
                Ok(resp) => {
                    if !resp.success {
                        // Abort all participants on first failure
                        let _ = self.abort(tx_id).await;
                        return Err(format!(
                            "participant {} prepare failed: {}",
                            p.id, resp.message
                        ));
                    }
                }
                Err(e) => {
                    let _ = self.abort(tx_id).await;
                    return Err(format!("participant {} prepare RPC error: {}", p.id, e));
                }
            }
        }

        // Persist PREPARED state in metastore
        let mut client = MetastoreClient::connect_with_shared(&self.shared)
            .await
            .map_err(|e| format!("metastore connect failed: {}", e))?;
        let mreq = ms::MetastoreRequest {
            action: Some(ms::metastore_request::Action::UpdateTransactionState(
                ms::UpdateTransactionStateRequest {
                    tx_id: tx_id.to_string(),
                    state: ms::TransactionState::Prepared as i32,
                },
            )),
        };

        match client.execute(mreq).await {
            Ok(_) => {
                // update in-memory
                let mut active = self.active.write().await;
                if let Some(t) = active.iter_mut().find(|t| t.tx_id == tx_id) {
                    t.state = TransactionState::Prepared;
                }
                Ok(())
            }
            Err(e) => Err(format!("failed to persist PREPARED state: {}", e)),
        }
    }

    /// Commit the transaction: persist commit marker in metastore and
    /// instruct participants to promote staged objects.
    pub async fn commit(&self, tx_id: &str) -> Result<(), String> {
        // Ensure transaction exists and is Prepared
        let participants = {
            let active = self.active.read().await;
            match active.iter().find(|t| t.tx_id == tx_id) {
                Some(t) => {
                    if t.state != TransactionState::Prepared {
                        return Err("transaction not in Prepared state".to_string());
                    }
                    t.participants.clone()
                }
                None => return Err(format!("transaction {} not found", tx_id)),
            }
        };

        // Persist COMMITTING state first
        let mut client = MetastoreClient::connect_with_shared(&self.shared)
            .await
            .map_err(|e| format!("metastore connect failed: {}", e))?;
        let mreq_committing = ms::MetastoreRequest {
            action: Some(ms::metastore_request::Action::UpdateTransactionState(
                ms::UpdateTransactionStateRequest {
                    tx_id: tx_id.to_string(),
                    state: ms::TransactionState::Committing as i32,
                },
            )),
        };
        client
            .execute(mreq_committing)
            .await
            .map_err(|e| format!("failed to persist COMMITTING: {}", e))?;

        // Instruct participants to commit
        for p in participants.iter() {
            let pool = {
                let state = self.shared.lock().await;
                match state.get_or_create_pool_for_key(&p.target).await {
                    Ok(pool) => pool,
                    Err(e) => return Err(format!("failed to get pool for {}: {}", p.target, e)),
                }
            };
            let conn = match crate::workers::acquire_channel_with_heartbeat(&pool, &p.target, 10)
                .await
            {
                Ok(c) => c,
                Err(e) => return Err(format!("failed to acquire conn for {}: {}", p.target, e)),
            };

            let creq = crate::services::worker_service_client::worker_service::CommitRequest {
                tx_id: tx_id.to_string(),
                staging_prefix: p.staging_prefix.clone(),
            };

            match crate::workers::send_commit_to_worker(conn, creq, 20).await {
                Ok(resp) => {
                    if !resp.success {
                        // If commit failed for participant, abort remaining and mark ABORTED
                        let _ = self.abort(tx_id).await;
                        return Err(format!(
                            "participant {} commit failed: {}",
                            p.id, resp.message
                        ));
                    }
                }
                Err(e) => {
                    let _ = self.abort(tx_id).await;
                    return Err(format!("participant {} commit RPC error: {}", p.id, e));
                }
            }
        }

        // Persist COMMITTED state
        let mreq_committed = ms::MetastoreRequest {
            action: Some(ms::metastore_request::Action::UpdateTransactionState(
                ms::UpdateTransactionStateRequest {
                    tx_id: tx_id.to_string(),
                    state: ms::TransactionState::Committed as i32,
                },
            )),
        };
        client
            .execute(mreq_committed)
            .await
            .map_err(|e| format!("failed to persist COMMITTED: {}", e))?;

        // update in-memory
        let mut active = self.active.write().await;
        if let Some(t) = active.iter_mut().find(|t| t.tx_id == tx_id) {
            t.state = TransactionState::Committed;
        }
        Ok(())
    }

    /// Abort the transaction: instruct participants to clean up staging and
    /// mark transaction aborted in metastore.
    pub async fn abort(&self, tx_id: &str) -> Result<(), String> {
        // Find transaction
        let participants = {
            let active = self.active.read().await;
            match active.iter().find(|t| t.tx_id == tx_id) {
                Some(t) => t.participants.clone(),
                None => return Err(format!("transaction {} not found", tx_id)),
            }
        };

        // Try to abort each participant (best-effort)
        for p in participants.iter() {
            if let Ok(pool) = {
                let state = self.shared.lock().await;
                state.get_or_create_pool_for_key(&p.target).await
            } {
                if let Ok(conn) =
                    crate::workers::acquire_channel_with_heartbeat(&pool, &p.target, 5).await
                {
                    let areq =
                        crate::services::worker_service_client::worker_service::AbortRequest {
                            tx_id: tx_id.to_string(),
                            staging_prefix: p.staging_prefix.clone(),
                        };
                    let _ = crate::workers::send_abort_to_worker(conn, areq, 10).await;
                }
            }
        }

        // Persist ABORTED state
        let mut client = MetastoreClient::connect_with_shared(&self.shared)
            .await
            .map_err(|e| format!("metastore connect failed: {}", e))?;
        let mreq = ms::MetastoreRequest {
            action: Some(ms::metastore_request::Action::UpdateTransactionState(
                ms::UpdateTransactionStateRequest {
                    tx_id: tx_id.to_string(),
                    state: ms::TransactionState::Aborted as i32,
                },
            )),
        };
        match client.execute(mreq).await {
            Ok(_) => {
                let mut active = self.active.write().await;
                if let Some(t) = active.iter_mut().find(|t| t.tx_id == tx_id) {
                    t.state = TransactionState::Aborted;
                }
                Ok(())
            }
            Err(e) => Err(format!("failed to persist ABORTED state: {}", e)),
        }
    }

    /// Recovery routine invoked at server startup to reconcile in-flight transactions
    /// by reading persisted transaction records from metastore and continuing commit/abort.
    pub async fn recover_inflight(&self) -> Result<(), String> {
        // Best-effort recovery for in-memory active transactions.
        // NOTE: full recovery from metastore (querying persisted transactions)
        // requires a list API on the metastore service; currently we resume
        // only transactions present in the in-memory `active` list.
        let active_tx = {
            let active = self.active.read().await;
            active.clone()
        };

        for tx in active_tx.into_iter() {
            // Skip terminal states
            match tx.state {
                TransactionState::Committed | TransactionState::Aborted => continue,
                _ => {}
            }

            let tx_id = tx.tx_id.clone();
            let _participants = tx.participants.clone();
            let this = self.clone_for_recovery();

            // Spawn a background task to resume the transaction
            tokio::spawn(async move {
                log::info!(
                    "recover_inflight: resuming transaction {} state={:?}",
                    tx_id,
                    tx.state
                );
                // If Preparing -> try prepare then commit
                if tx.state == TransactionState::Preparing {
                    if let Err(e) = this.prepare_participants(&tx_id).await {
                        log::error!(
                            "recover_inflight: prepare failed for {}: {} — aborting",
                            tx_id,
                            e
                        );
                        let _ = this.abort(&tx_id).await;
                        return;
                    }
                }

                // If Prepared or Preparing succeeded -> commit
                if tx.state == TransactionState::Prepared || tx.state == TransactionState::Preparing
                {
                    if let Err(e) = this.commit(&tx_id).await {
                        log::error!(
                            "recover_inflight: commit failed for {}: {} — aborting",
                            tx_id,
                            e
                        );
                        let _ = this.abort(&tx_id).await;
                    }
                } else if tx.state == TransactionState::Committing {
                    // If already committing, attempt to finish commit
                    if let Err(e) = this.commit(&tx_id).await {
                        log::error!(
                            "recover_inflight: commit(retry) failed for {}: {} — aborting",
                            tx_id,
                            e
                        );
                        let _ = this.abort(&tx_id).await;
                    }
                }
            });
        }

        Ok(())
    }
}

impl Maestro {
    /// Helper to create a lightweight clone of `Maestro` for spawning tasks.
    fn clone_for_recovery(&self) -> Self {
        Maestro {
            shared: self.shared.clone(),
            active: self.active.clone(),
        }
    }
}
