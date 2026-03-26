use datafusion::arrow::datatypes::DataType;

/// What: Map metastore type strings to Arrow/DataFusion datatypes for provider schemas.
///
/// Inputs:
/// - `data_type`: Raw metastore column type string.
///
/// Output:
/// - Best-effort Arrow datatype.
///
/// Details:
/// - Unsupported or complex forms degrade to `Utf8` in this phase.
pub fn map_metastore_type_to_arrow(data_type: &str) -> DataType {
    let normalized = data_type.trim().to_ascii_lowercase();

    if normalized.contains("bigint") || normalized == "int8" {
        return DataType::Int64;
    }
    if normalized == "int" || normalized == "integer" || normalized == "int4" {
        return DataType::Int32;
    }
    if normalized == "smallint" || normalized == "int2" {
        return DataType::Int16;
    }
    if normalized == "tinyint" {
        return DataType::Int8;
    }
    if normalized == "bool" || normalized == "boolean" {
        return DataType::Boolean;
    }
    if normalized == "float" || normalized == "float4" || normalized == "real" {
        return DataType::Float32;
    }
    if normalized == "double" || normalized == "float8" || normalized == "double precision" {
        return DataType::Float64;
    }
    if normalized == "date" {
        return DataType::Date32;
    }
    if normalized.starts_with("timestamp") || normalized == "datetime" {
        return DataType::Timestamp(datafusion::arrow::datatypes::TimeUnit::Millisecond, None);
    }

    DataType::Utf8
}
