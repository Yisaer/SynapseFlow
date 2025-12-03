use crate::planner::physical::BasePhysicalPlan;
use crate::planner::sink::CommonSinkProps;
use std::fmt;
use std::sync::Arc;

use super::PhysicalPlan;

/// Physical node that performs batching before encoding.
#[derive(Clone)]
pub struct PhysicalBatch {
    pub base: BasePhysicalPlan,
    pub sink_id: String,
    pub common: CommonSinkProps,
}

impl PhysicalBatch {
    pub fn new(
        children: Vec<Arc<PhysicalPlan>>,
        index: i64,
        sink_id: String,
        common: CommonSinkProps,
    ) -> Self {
        Self {
            base: BasePhysicalPlan::new(children, index),
            sink_id,
            common,
        }
    }
}

impl fmt::Debug for PhysicalBatch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PhysicalBatch")
            .field("index", &self.base.index())
            .field("sink_id", &self.sink_id)
            .field("batch_count", &self.common.batch_count)
            .field(
                "batch_duration_ms",
                &self.common.batch_duration.map(|dur| dur.as_millis() as u64),
            )
            .finish()
    }
}
