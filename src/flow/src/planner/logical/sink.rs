use std::sync::Arc;
use crate::planner::logical::{LogicalPlan, BaseLogicalPlan};

#[derive(Debug, Clone)]
pub struct Sink {
    pub base: BaseLogicalPlan,
    pub sink_name: String,
}

impl Sink {
    pub fn new(sink_name: String, children: Vec<Arc<dyn LogicalPlan>>,index: i64) -> Self {
        let base = BaseLogicalPlan::new(children,index);
        Self { base, sink_name }
    }
}

impl LogicalPlan for Sink {
    fn children(&self) -> &[Arc<dyn LogicalPlan>] {
        &self.base.children
    }

    fn get_plan_type(&self) -> &str {
        "Sink"
    }

    fn get_plan_index(&self) -> &i64 {
        &self.base.index
    }
}