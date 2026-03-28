use crate::warehouse::state::SharedData;

pub async fn handle_use_warehouse(
    query: &str,
    session_id: &str,
    shared_data: &SharedData,
) -> Result<String, String> {
    let q_trim = query.trim();
    // Expect forms like: USE WAREHOUSE <name>;
    let parts: Vec<&str> = q_trim.split_whitespace().collect();
    if parts.len() < 3 {
        return Err("invalid USE WAREHOUSE command".to_string());
    }

    let mut wh = parts[2].trim().to_string();
    if wh.ends_with(';') {
        wh = wh.trim_end_matches(';').to_string();
    }

    log::info!(
        "handle_use_warehouse: session_id={}, warehouse={}",
        session_id,
        wh
    );

    // Update the session's warehouse via SessionManager
    let session_opt = {
        let state = shared_data.lock().await;
        state
            .session_manager
            .get_session(session_id.to_string())
            .await
    };

    match session_opt {
        Some(mut s) => {
            s.set_warehouse(wh.clone());
            log::info!(
                "handle_use_warehouse: looking up pool members for warehouse: {}",
                wh
            );
            let state = shared_data.lock().await;
            let pool_members = state.get_pool_members(&wh).await;
            log::info!(
                "handle_use_warehouse: get_pool_members result: {:?}",
                pool_members
            );
            let pool_members = pool_members.unwrap_or_default();
            log::info!(
                "handle_use_warehouse: setting pool_members on session: {:?}",
                pool_members
            );
            s.set_pool_members(pool_members);
            state
                .session_manager
                .update_session(session_id.to_string(), &s)
                .await
                .map_err(|error| format!("failed to persist updated session: {}", error))?;
            Ok(format!("Using warehouse {}", wh))
        }
        None => Err("session not found".to_string()),
    }
}
