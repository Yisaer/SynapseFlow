//! Decoder abstractions for turning raw bytes into RecordBatch collections.

use crate::model::{CollectionError, Column, RecordBatch};
use datatypes::{ConcreteDatatype, ListValue, StructField, StructType, StructValue, Value};
use serde_json::{Map as JsonMap, Value as JsonValue};
use std::{collections::BTreeSet, sync::Arc};

/// Errors that can occur while decoding payloads.
#[derive(thiserror::Error, Debug)]
pub enum CodecError {
    /// Payload was not valid UTF-8 (used by the simple string decoder).
    #[error("invalid utf8: {0}")]
    Utf8(#[from] std::string::FromUtf8Error),
    /// Payload was not valid JSON.
    #[error("invalid json: {0}")]
    Json(#[from] serde_json::Error),
    /// RecordBatch construction failed.
    #[error("collection error: {0}")]
    Collection(#[from] CollectionError),
    /// Custom decoder-specific failure.
    #[error("{0}")]
    Other(String),
}

/// Trait implemented by all record decoders.
pub trait RecordDecoder: Send + Sync + 'static {
    /// Convert raw bytes into a RecordBatch.
    fn decode(&self, payload: &[u8]) -> Result<RecordBatch, CodecError>;
}

/// Minimal decoder that wraps each payload as a one-row, single-column batch.
pub struct RawStringDecoder {
    source_name: String,
    column_name: String,
}

impl RawStringDecoder {
    pub fn new(source_name: impl Into<String>, column_name: impl Into<String>) -> Self {
        Self {
            source_name: source_name.into(),
            column_name: column_name.into(),
        }
    }
}

impl RecordDecoder for RawStringDecoder {
    fn decode(&self, payload: &[u8]) -> Result<RecordBatch, CodecError> {
        let value = String::from_utf8(payload.to_vec())?;
        let column = Column::new(
            self.source_name.clone(),
            self.column_name.clone(),
            vec![Value::String(value)],
        );
        Ok(RecordBatch::new(vec![column])?)
    }
}

/// Decoder that converts JSON documents (object or array) into a RecordBatch.
pub struct JsonDecoder {
    source_name: String,
    value_column: String,
}

impl JsonDecoder {
    pub fn new(source_name: impl Into<String>) -> Self {
        Self::with_value_column(source_name, "value")
    }

    pub fn with_value_column(
        source_name: impl Into<String>,
        value_column: impl Into<String>,
    ) -> Self {
        Self {
            source_name: source_name.into(),
            value_column: value_column.into(),
        }
    }

    fn decode_value(&self, json: JsonValue) -> Result<RecordBatch, CodecError> {
        match json {
            JsonValue::Object(map) => self.build_from_object_rows(vec![map]),
            JsonValue::Array(items) => self.decode_array(items),
            other => self.build_scalar_column(vec![other]),
        }
    }

    fn decode_array(&self, items: Vec<JsonValue>) -> Result<RecordBatch, CodecError> {
        if items.is_empty() {
            return Ok(RecordBatch::empty());
        }

        if items.iter().all(|v| v.is_object()) {
            let rows: Vec<JsonMap<String, JsonValue>> = items
                .into_iter()
                .map(|v| match v {
                    JsonValue::Object(map) => map,
                    _ => unreachable!("validated object rows"),
                })
                .collect();
            return self.build_from_object_rows(rows);
        }

        self.build_scalar_column(items)
    }

    fn build_from_object_rows(
        &self,
        rows: Vec<JsonMap<String, JsonValue>>,
    ) -> Result<RecordBatch, CodecError> {
        if rows.is_empty() {
            return Ok(RecordBatch::empty());
        }

        let mut keys = BTreeSet::new();
        for row in &rows {
            for key in row.keys() {
                keys.insert(key.clone());
            }
        }

        if keys.is_empty() {
            let values = rows
                .into_iter()
                .map(JsonValue::Object)
                .collect::<Vec<JsonValue>>();
            return self.build_scalar_column(values);
        }

        let mut columns = Vec::with_capacity(keys.len());
        for key in keys {
            let mut col_values = Vec::with_capacity(rows.len());
            for row in &rows {
                if let Some(value) = row.get(&key) {
                    col_values.push(json_to_value(value));
                } else {
                    col_values.push(Value::Null);
                }
            }
            columns.push(Column::new(self.source_name.clone(), key, col_values));
        }

        Ok(RecordBatch::new(columns)?)
    }

    fn build_scalar_column(&self, values: Vec<JsonValue>) -> Result<RecordBatch, CodecError> {
        if values.is_empty() {
            return Ok(RecordBatch::empty());
        }

        let column_values = values.iter().map(json_to_value).collect::<Vec<_>>();
        let column = Column::new(
            self.source_name.clone(),
            self.value_column.clone(),
            column_values,
        );
        Ok(RecordBatch::new(vec![column])?)
    }
}

impl RecordDecoder for JsonDecoder {
    fn decode(&self, payload: &[u8]) -> Result<RecordBatch, CodecError> {
        let json = serde_json::from_slice(payload)?;
        self.decode_value(json)
    }
}

fn json_to_value(value: &JsonValue) -> Value {
    match value {
        JsonValue::Null => Value::Null,
        JsonValue::Bool(b) => Value::Bool(*b),
        JsonValue::Number(n) => {
            if let Some(i) = n.as_i64() {
                Value::Int64(i)
            } else if let Some(u) = n.as_u64() {
                Value::Uint64(u)
            } else if let Some(f) = n.as_f64() {
                Value::Float64(f)
            } else {
                Value::Null
            }
        }
        JsonValue::String(s) => Value::String(s.clone()),
        JsonValue::Array(items) => {
            let converted: Vec<Value> = items.iter().map(json_to_value).collect();
            let element_type = converted
                .iter()
                .find(|v| !matches!(v, Value::Null))
                .map(Value::datatype)
                .unwrap_or(ConcreteDatatype::Null);
            Value::List(ListValue::new(converted, Arc::new(element_type)))
        }
        JsonValue::Object(map) => {
            let mut fields = Vec::with_capacity(map.len());
            let mut values = Vec::with_capacity(map.len());

            for (key, val) in map {
                let converted = json_to_value(val);
                let datatype = converted.datatype();
                fields.push(StructField::new(key.clone(), datatype, true));
                values.push(converted);
            }

            Value::Struct(StructValue::new(values, StructType::new(Arc::new(fields))))
        }
    }
}
