use crate::planner::physical::{BasePhysicalPlan, PhysicalPlan, WatermarkConfig};
use std::sync::Arc;

/// Event-time watermark physical node (data-driven).
///
/// Note: this type is introduced as part of the PhysicalPlan watermark split (step 6).
#[derive(Debug, Clone)]
pub struct PhysicalEventtimeWatermark {
    pub base: BasePhysicalPlan,
    pub config: WatermarkConfig,
}

impl PhysicalEventtimeWatermark {
    pub fn new(config: WatermarkConfig, children: Vec<Arc<PhysicalPlan>>, index: i64) -> Self {
        let base = BasePhysicalPlan::new(children, index);
        Self { base, config }
    }
}
