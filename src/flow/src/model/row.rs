use datatypes::Value;

/// Row represents a row of data as a vector of values.
///
/// This is a wrapper around a vector of values, providing convenient methods
/// for row manipulation and access.
#[derive(Clone, Debug, PartialEq, Default)]
pub struct Row {
    /// The inner vector of values
    pub inner: Vec<Value>,
}

impl Row {
    /// Create an empty row
    pub fn empty() -> Self {
        Self { inner: vec![] }
    }

    /// Returns true if the Row contains no elements.
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Create a row from a vector of values
    pub fn new(row: Vec<Value>) -> Self {
        Self { inner: row }
    }

    /// Get the value at the given index
    pub fn get(&self, idx: usize) -> Option<&Value> {
        self.inner.get(idx)
    }

    /// Get a mutable reference to the value at the given index
    pub fn get_mut(&mut self, idx: usize) -> Option<&mut Value> {
        self.inner.get_mut(idx)
    }

    /// Clear the row
    pub fn clear(&mut self) {
        self.inner.clear();
    }

    /// Clear and return a mutable reference to the inner vector
    ///
    /// Useful if you want to reuse the vector as a buffer
    pub fn packer(&mut self) -> &mut Vec<Value> {
        self.inner.clear();
        &mut self.inner
    }

    /// Pack an iterator of values into a row
    pub fn pack<I>(iter: I) -> Row
    where
        I: IntoIterator<Item = Value>,
    {
        Self {
            inner: iter.into_iter().collect(),
        }
    }

    /// Unpack a row into a vector of values
    pub fn unpack(self) -> Vec<Value> {
        self.inner
    }

    /// Extend the row with values from an iterator
    pub fn extend<I>(&mut self, iter: I)
    where
        I: IntoIterator<Item = Value>,
    {
        self.inner.extend(iter);
    }

    /// Creates a consuming iterator, that is, one that moves each value out of the `Row`
    pub fn into_owned_iter(self) -> impl Iterator<Item = Value> {
        self.inner.into_iter()
    }

    /// Returns an iterator over the slice.
    pub fn iter(&self) -> impl Iterator<Item = &Value> {
        self.inner.iter()
    }

    /// Returns the number of elements in the row, also known as its 'length'.
    pub fn len(&self) -> usize {
        self.inner.len()
    }
}

impl From<Vec<Value>> for Row {
    fn from(row: Vec<Value>) -> Self {
        Row::new(row)
    }
}
