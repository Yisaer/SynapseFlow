use crate::planner::logical::{BaseLogicalPlan, LogicalPlan};
use std::any::Any;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct DataSource {
    pub base: BaseLogicalPlan,
    pub source_name: String,
    pub alias: Option<String>,
}

impl DataSource {
    pub fn new(source_name: String, alias: Option<String>, index: i64) -> Self {
        let base = BaseLogicalPlan::new(vec![], index);
        Self {
            base,
            source_name,
            alias,
        }
    }
}

impl LogicalPlan for DataSource {
    fn children(&self) -> &[Arc<dyn LogicalPlan>] {
        &self.base.children
    }

    fn get_plan_type(&self) -> &str {
        "DataSource"
    }

    fn get_plan_index(&self) -> &i64 {
        &self.base.index
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
