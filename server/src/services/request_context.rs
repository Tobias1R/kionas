use crate::warehouse::state::SharedData;
use tonic::Request;
use tonic::Status;

#[derive(Clone)]
#[allow(dead_code)]
pub struct RequestContext {
    pub query_id: String,
    pub session_id_opt: Option<String>,
    pub session_id: String,
    pub rbac_user: String,
    pub role: String,
    pub warehouse_name: String,
    pub worker_uuid: String,
}

impl RequestContext {
    pub async fn from_request<T>(
        shared_data: SharedData,
        request: &Request<T>,
    ) -> Result<Self, Status> {
        let session_id_opt = request
            .metadata()
            .get("session_id")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());
        let rbac_user = request
            .metadata()
            .get("rbac_user")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string())
            .unwrap_or_else(|| "anonymous".to_string());

        let session_id = session_id_opt.clone().unwrap_or_default();
        let mut role: String = "worker".to_string();
        let mut warehouse_name = "".to_string();

        if let Some(sid) = session_id_opt.clone() {
            let sd = shared_data.lock().await;
            if let Some(sess) = sd.session_manager.get_session(sid.clone()).await {
                role = sess.get_role().to_string();
                warehouse_name = sess.get_warehouse();
            }
        }

        let worker_uuid = if !warehouse_name.is_empty() {
            if let Some(wh) =
                crate::session::find_warehouse_by_name(&shared_data, &warehouse_name).await
            {
                wh.get_uuid()
            } else {
                // return Err(Status::not_found(format!("warehouse not found: {}", warehouse_name)));
                // lets bypass this for now and just return an empty string if the warehouse is not found, since the worker uuid is not critical for query execution at this point.
                "somecooluuid-to-be-removed".to_string()
            }
        } else {
            "".to_string()
        };

        let _query_id = kionas::gen_random_uuid();

        let query_id = kionas::gen_random_uuid();

        Ok(Self {
            query_id,
            session_id_opt,
            session_id,
            rbac_user,
            role,
            warehouse_name,
            worker_uuid,
        })
    }
}
