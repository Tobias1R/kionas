type Task = crate::services::worker_service_server::worker_service::StagePartitionExecution;

use crate::transactions::maestro::{InsertScalar, ParsedInsertPayload};
use base64::Engine;
use prost::Message;
use std::collections::HashMap;

#[derive(Clone, PartialEq, Message)]
struct InsertEnvelopeProto {
    #[prost(uint32, tag = "1")]
    contract_version: u32,
    #[prost(string, tag = "2")]
    table_name: String,
    #[prost(string, repeated, tag = "3")]
    columns: Vec<String>,
    #[prost(message, repeated, tag = "4")]
    rows: Vec<InsertRowProto>,
}

#[derive(Clone, PartialEq, Message)]
struct InsertRowProto {
    #[prost(message, repeated, tag = "1")]
    values: Vec<InsertScalarProto>,
}

#[derive(Clone, PartialEq, Message)]
struct InsertScalarProto {
    #[prost(oneof = "insert_scalar_proto::Value", tags = "1, 2, 3, 4")]
    value: Option<insert_scalar_proto::Value>,
}

mod insert_scalar_proto {
    use prost::Oneof;

    #[derive(Clone, PartialEq, Oneof)]
    pub(crate) enum Value {
        #[prost(int64, tag = "1")]
        Int(i64),
        #[prost(bool, tag = "2")]
        Bool(bool),
        #[prost(string, tag = "3")]
        Str(String),
        #[prost(bool, tag = "4")]
        Null(bool),
    }
}

fn normalize_identifier(raw: &str) -> String {
    raw.trim()
        .trim_matches('"')
        .trim_matches('`')
        .trim_matches('[')
        .trim_matches(']')
        .to_ascii_lowercase()
}

pub(crate) fn parse_insert_payload(task: &Task) -> Result<ParsedInsertPayload, String> {
    let bytes = if !task.raw_payload.is_empty() {
        task.raw_payload.clone()
    } else {
        let raw = task.input.trim();
        if raw.is_empty() {
            return Err(
                "insert payload contract missing: task input or raw_payload must contain protobuf payload"
                    .to_string(),
            );
        }

        base64::engine::general_purpose::STANDARD
            .decode(raw)
            .map_err(|e| {
                format!(
                    "insert payload contract malformed: base64 decode failed: {}",
                    e
                )
            })?
    };
    let payload = InsertEnvelopeProto::decode(bytes.as_slice()).map_err(|e| {
        format!(
            "insert payload contract malformed: protobuf decode failed: {}",
            e
        )
    })?;

    if payload.contract_version != 1 {
        return Err(format!(
            "insert payload contract malformed: unsupported contract_version={} (expected=1)",
            payload.contract_version
        ));
    }

    let column_count = payload.rows.first().map(|r| r.values.len()).unwrap_or(0);
    if column_count == 0 {
        return Err("INSERT VALUES produced zero columns".to_string());
    }
    if payload.rows.iter().any(|r| r.values.len() != column_count) {
        return Err("INSERT VALUES row width mismatch".to_string());
    }
    if payload.columns.len() != column_count {
        return Err("INSERT payload columns width mismatch".to_string());
    }

    let rows = payload
        .rows
        .into_iter()
        .map(|row| {
            row.values
                .into_iter()
                .map(|scalar| match scalar.value {
                    Some(insert_scalar_proto::Value::Int(v)) => Ok(InsertScalar::Int(v)),
                    Some(insert_scalar_proto::Value::Bool(v)) => Ok(InsertScalar::Bool(v)),
                    Some(insert_scalar_proto::Value::Str(v)) => Ok(InsertScalar::Str(v)),
                    Some(insert_scalar_proto::Value::Null(_)) => Ok(InsertScalar::Null),
                    None => Err(
                        "insert payload contract malformed: scalar oneof value missing".to_string(),
                    ),
                })
                .collect::<Result<Vec<_>, _>>()
        })
        .collect::<Result<Vec<_>, _>>()?;

    Ok(ParsedInsertPayload {
        table_name: payload.table_name,
        columns: payload.columns,
        rows,
    })
}

pub(crate) fn parse_insert_column_type_hints(
    task: &crate::services::worker_service_server::worker_service::StagePartitionExecution,
) -> Result<HashMap<String, String>, String> {
    let strict_contract = task
        .params
        .get("datatype_contract_version")
        .map(|value| !value.trim().is_empty() && value.trim() != "0")
        .unwrap_or(false);

    let Some(raw) = task.params.get("column_type_hints_json") else {
        if strict_contract {
            return Err(
                "INSERT_TYPE_HINTS_MALFORMED: missing column_type_hints_json for datatype contract"
                    .to_string(),
            );
        }
        return Ok(HashMap::new());
    };

    let hints = serde_json::from_str::<HashMap<String, String>>(raw)
        .map(|value| {
            value
                .into_iter()
                .map(|(k, v)| (normalize_identifier(&k), v))
                .filter(|(k, _)| !k.is_empty())
                .collect::<HashMap<_, _>>()
        })
        .map_err(|e| {
            format!(
                "INSERT_TYPE_HINTS_MALFORMED: invalid column_type_hints_json: {}",
                e
            )
        })?;

    if strict_contract && hints.is_empty() {
        return Err(
            "INSERT_TYPE_HINTS_MALFORMED: column_type_hints_json cannot be empty when datatype contract is enabled"
                .to_string(),
        );
    }

    Ok(hints)
}
