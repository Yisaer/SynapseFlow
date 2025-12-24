use crate::model::RecordBatch;

impl RecordBatch {
    pub fn debug_print(&self) {
        tracing::debug!(rows = self.num_rows(), "record batch");
        for (row_idx, row) in self.rows().iter().enumerate() {
            tracing::debug!(row_idx = row_idx, "record batch row");
            for ((source, column), value) in row.entries() {
                tracing::debug!(
                    source = source,
                    column = column,
                    value = ?value,
                    "record batch value"
                );
            }
        }
    }
}
