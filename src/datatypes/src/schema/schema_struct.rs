use std::collections::HashMap;

use super::ColumnSchema;

/// Schema containing column definitions
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Schema {
    /// Column schemas
    column_schemas: Vec<ColumnSchema>,
    /// Mapping from column name to index
    name_to_index: HashMap<String, usize>,
}

impl Schema {
    /// Create a new schema from a vector of column schemas
    pub fn new(column_schemas: Vec<ColumnSchema>) -> Self {
        let name_to_index: HashMap<String, usize> = column_schemas
            .iter()
            .enumerate()
            .map(|(index, column_schema)| (column_schema.name.clone(), index))
            .collect();

        Schema {
            column_schemas,
            name_to_index,
        }
    }

    /// Get column schema by name
    pub fn column_schema_by_name(&self, name: &str) -> Option<&ColumnSchema> {
        self.name_to_index
            .get(name)
            .map(|index| &self.column_schemas[*index])
    }

    /// Get all column schemas
    pub fn column_schemas(&self) -> &[ColumnSchema] {
        &self.column_schemas
    }

    /// Check if schema contains a column with the given name
    pub fn contains_column(&self, name: &str) -> bool {
        self.name_to_index.contains_key(name)
    }

    /// Get column index by name if it exists.
    pub fn column_index(&self, name: &str) -> Option<usize> {
        self.name_to_index.get(name).copied()
    }
}
