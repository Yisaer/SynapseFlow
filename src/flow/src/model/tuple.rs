use datatypes::Value;
use std::collections::HashMap;

/// Tuple represents a single row of data decoded from a source.
///
/// The ordered list of fully qualified column identifiers and their values is
/// captured via the `(source_name, column_name)` index pointing to entries in
/// `values`.
#[derive(Debug, Clone, PartialEq)]
pub struct Tuple {
    pub values: Vec<Value>,
    index: HashMap<(String, String), usize>,
}

impl Tuple {
    /// Build a new tuple, assuming the caller provides matching column/value
    /// lengths.
    pub fn new(index: HashMap<(String, String), usize>, values: Vec<Value>) -> Self {
        debug_assert!(
            index.len() == values.len(),
            "Tuple index and values must have the same length"
        );
        Self { values, index }
    }

    /// Return number of fields stored in this tuple.
    pub fn len(&self) -> usize {
        self.index.len()
    }

    /// Check whether this tuple contains any fields.
    pub fn is_empty(&self) -> bool {
        self.index.is_empty()
    }

    pub fn column_pairs(&self) -> Vec<(String, String)> {
        self.index.keys().cloned().collect()
    }

    pub fn entries(&self) -> impl Iterator<Item = (&(String, String), &Value)> {
        self.index
            .iter()
            .map(|(key, idx)| (key, &self.values[*idx]))
    }

    pub fn values(&self) -> &[Value] {
        &self.values
    }

    pub fn value_by_name(&self, source_name: &str, column_name: &str) -> Option<&Value> {
        self.index
            .get(&(source_name.to_string(), column_name.to_string()))
            .and_then(|idx| self.values.get(*idx))
    }

    pub fn value_by_column(&self, column_name: &str) -> Option<&Value> {
        self.index
            .iter()
            .find(|((_, name), _)| name == column_name)
            .and_then(|(_, idx)| self.values.get(*idx))
    }

    pub fn rewrite_sources(&mut self, source_name: &str) {
        let mut new_index = HashMap::with_capacity(self.index.len());
        let new_source = source_name.to_string();
        for ((_, column), idx) in std::mem::take(&mut self.index) {
            new_index.insert((new_source.clone(), column), idx);
        }
        self.index = new_index;
    }

    /// Build an index map from column descriptors, preserving deterministic order.
    pub fn build_index(columns: &[(String, String)]) -> HashMap<(String, String), usize> {
        let mut index = HashMap::with_capacity(columns.len());
        for (idx, pair) in columns.iter().enumerate() {
            index.insert(pair.clone(), idx);
        }
        index
    }
}
