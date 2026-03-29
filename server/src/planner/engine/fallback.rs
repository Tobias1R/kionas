use kionas::sql::query_model::{QueryFromSpec, SelectQueryModel};

pub(crate) fn is_fallback_eligible_datafusion_error(message: &str) -> bool {
    let lower = message.to_ascii_lowercase();
    (lower.contains("table") && lower.contains("not found"))
        || (lower.contains("no suitable object store found")
            || lower.contains("register_object_store"))
}

pub(crate) fn is_object_store_registration_error(message: &str) -> bool {
    let lower = message.to_ascii_lowercase();
    lower.contains("no suitable object store found") || lower.contains("register_object_store")
}

pub(crate) fn requires_strict_datafusion_planning(model: &SelectQueryModel) -> bool {
    !matches!(model.from, QueryFromSpec::Table { .. }) || !model.ctes.is_empty()
}
