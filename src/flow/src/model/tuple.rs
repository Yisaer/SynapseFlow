use super::row::Row;
use datatypes::{Value};
use serde_json::Value as JsonValue;
use std::collections::HashMap;

/// Tuple represents a row of data with HashMap storage.
///
/// A Tuple uses a HashMap indexed by (source_name, column_name) for efficient
/// source+column name based access.
#[derive(Clone, Debug, PartialEq)]
pub struct Tuple {
    /// HashMap storage: (source_name, column_name) -> Value
    data: HashMap<(String, String), Value>,
}

impl Tuple {
    /// Create a new tuple with the given data
    pub fn new(data: HashMap<(String, String), Value>) -> Self {
        Self { data }
    }

    /// Create a new tuple from a schema and a vector of values (for compatibility)
    /// This converts the Vec<Value> to the internal HashMap format
    pub fn from_values(schema: datatypes::Schema, values: Vec<Value>) -> Self {
        assert_eq!(
            schema.column_schemas().len(),
            values.len(),
            "Values length must match schema column count"
        );
        
        let mut data = HashMap::new();
        for (col_schema, value) in schema.column_schemas().iter().zip(values.into_iter()) {
            data.insert((col_schema.source_name().to_string(), col_schema.name.clone()), value);
        }
        
        Self::new(data)
    }

    /// Get the internal data HashMap (for advanced usage)
    pub fn data(&self) -> &HashMap<(String, String), Value> {
        &self.data
    }

    /// Get the number of columns in this tuple
    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// Returns true if the tuple contains no columns
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Consume the tuple and return the data
    pub fn into_parts(self) -> HashMap<(String, String), Value> {
        self.data
    }

    /// Create a new tuple from JSON bytes
    ///
    /// # Arguments
    ///
    /// * `json_bytes` - A byte slice containing a JSON object
    ///
    /// # Returns
    ///
    /// Returns a `Result` with the created `Tuple` or an error if JSON parsing fails.
    pub fn new_from_json(json_bytes: &[u8]) -> Result<Self, JsonError> {
        let json_value: JsonValue =
            serde_json::from_slice(json_bytes).map_err(|e| JsonError::ParseError(e.to_string()))?;

        let json_obj = json_value
            .as_object()
            .ok_or_else(|| JsonError::InvalidFormat("Expected JSON object".to_string()))?;

        let mut data = HashMap::new();
        for (key, json_val) in json_obj {
            // Parse key as "source.column" or just "column"
            let parts: Vec<&str> = key.split('.').collect();
            let (source_name, column_name) = if parts.len() == 2 {
                (parts[0].to_string(), parts[1].to_string())
            } else {
                ("default".to_string(), key.clone())
            };

            // For now, assume all values are strings for simplicity
            let value = match json_val {
                JsonValue::String(s) => Value::String(s.clone()),
                JsonValue::Number(n) => {
                    if let Some(i) = n.as_i64() {
                        Value::Int64(i)
                    } else if let Some(f) = n.as_f64() {
                        Value::Float64(f)
                    } else {
                        return Err(JsonError::TypeMismatch {
                            expected: "number".to_string(),
                            actual: format!("{}", n),
                        });
                    }
                }
                JsonValue::Bool(b) => Value::Bool(*b),
                JsonValue::Null => Value::Null,
                _ => return Err(JsonError::TypeMismatch {
                    expected: "simple type".to_string(),
                    actual: format!("{:?}", json_val),
                }),
            };

            data.insert((source_name, column_name), value);
        }

        Ok(Self::new(data))
    }
}

impl Row for Tuple {
    fn get_by_name(&self, name: &str) -> Option<&Value> {
        // Try to find by column name alone (assuming any source)
        for ((_source_name, column_name), value) in &self.data {
            if column_name == name {
                return Some(value);
            }
        }
        None
    }
    
    fn get_by_source_column(&self, source_name: &str, column_name: &str) -> Option<&Value> {
        self.data.get(&(source_name.to_string(), column_name.to_string()))
    }
    
    fn len(&self) -> usize {
        self.data.len()
    }
}

/// Error type for JSON parsing and conversion
#[derive(Debug, Clone, PartialEq)]
pub enum JsonError {
    /// JSON parsing error
    ParseError(String),
    /// Invalid JSON format
    InvalidFormat(String),
    /// Type mismatch error
    TypeMismatch {
        expected: String,
        actual: String,
    },
}

impl std::fmt::Display for JsonError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            JsonError::ParseError(msg) => write!(f, "JSON parse error: {}", msg),
            JsonError::InvalidFormat(msg) => write!(f, "Invalid JSON format: {}", msg),
            JsonError::TypeMismatch { expected, actual } => {
                write!(f, "Type mismatch: expected {}, got {}", expected, actual)
            }
        }
    }
}

impl std::error::Error for JsonError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tuple_with_hashmap_storage() {
        let mut data = HashMap::new();
        data.insert(("users".to_string(), "id".to_string()), Value::Int64(1));
        data.insert(("users".to_string(), "name".to_string()), Value::String("Alice".to_string()));
        data.insert(("users".to_string(), "age".to_string()), Value::Int64(25));

        let tuple = Tuple::new(data);

        // Test Row trait implementation
        assert_eq!(tuple.len(), 3);
        assert_eq!(tuple.get_by_name("name"), Some(&Value::String("Alice".to_string())));
        assert_eq!(tuple.get_by_source_column("users", "age"), Some(&Value::Int64(25)));
        assert_eq!(tuple.get_by_source_column("users", "email"), None);
        assert_eq!(tuple.get_by_name("email"), None);
    }

    #[test]
    fn test_new_from_json() {
        let json = br#"{"users.id": 1, "users.name": "Alice", "users.age": 25}"#;
        let tuple = Tuple::new_from_json(json).unwrap();

        assert_eq!(tuple.len(), 3);
        assert_eq!(tuple.get_by_source_column("users", "id"), Some(&Value::Int64(1)));
        assert_eq!(tuple.get_by_name("name"), Some(&Value::String("Alice".to_string())));
        assert_eq!(tuple.get_by_source_column("users", "age"), Some(&Value::Int64(25)));

        // Test with simple column names (no source)
        let json2 = br#"{"id": 2, "name": "Bob"}"#;
        let tuple2 = Tuple::new_from_json(json2).unwrap();

        assert_eq!(tuple2.get_by_source_column("default", "id"), Some(&Value::Int64(2)));
        assert_eq!(tuple2.get_by_name("name"), Some(&Value::String("Bob".to_string())));
    }
}