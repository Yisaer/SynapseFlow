use crate::planner::physical::{BasePhysicalPlan, PhysicalPlan};
use datatypes::Schema;
use std::any::Any;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct PhysicalSharedStream {
    base: BasePhysicalPlan,
    stream_name: String,
    alias: Option<String>,
    schema: Arc<Schema>,
}

impl PhysicalSharedStream {
    pub fn new(
        stream_name: String,
        alias: Option<String>,
        schema: Arc<Schema>,
        index: i64,
    ) -> Self {
        let base = BasePhysicalPlan::new_leaf(index);
        Self {
            base,
            stream_name,
            alias,
            schema,
        }
    }

    pub fn stream_name(&self) -> &str {
        &self.stream_name
    }

    pub fn alias(&self) -> Option<&str> {
        self.alias.as_deref()
    }

    pub fn schema(&self) -> Arc<Schema> {
        Arc::clone(&self.schema)
    }
}

impl PhysicalPlan for PhysicalSharedStream {
    fn children(&self) -> &[Arc<dyn PhysicalPlan>] {
        &self.base.children
    }

    fn get_plan_type(&self) -> &str {
        "PhysicalSharedStream"
    }

    fn get_plan_index(&self) -> &i64 {
        &self.base.index
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
