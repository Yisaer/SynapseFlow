//! Physical result collect plan - collects outputs from multiple sinks

use crate::planner::physical::base_physical::BasePhysicalPlan;
use std::sync::Arc;

/// Physical plan node for collecting results from multiple sinks
#[derive(Debug, Clone)]
pub struct PhysicalResultCollect {
    pub base: BasePhysicalPlan,
}

impl PhysicalResultCollect {
    /// Create a new PhysicalResultCollect
    pub fn new(children: Vec<Arc<crate::planner::physical::PhysicalPlan>>, index: i64) -> Self {
        Self {
            base: BasePhysicalPlan::new(children, index),
        }
    }
}
