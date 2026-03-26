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
            let state = shared_data.lock().await;
            state
                .session_manager
                .update_session(session_id.to_string(), &mut s)
                .await;
            Ok(format!("Using warehouse {}", wh))
        }
        None => Err("session not found".to_string()),
    }
}
