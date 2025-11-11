use std::any::Any;
use std::sync::Arc;
use crate::planner::physical::{PhysicalPlan, BasePhysicalPlan};
use crate::expr::ScalarExpr;

/// Field definition for physical projection
#[derive(Debug, Clone)]
pub struct PhysicalProjectField {
    /// Output field name
    pub field_name: String,
    /// Expression to compute the field value
    pub expr: ScalarExpr,
}

/// Physical operator for projection operations
/// 
/// This operator represents the physical execution of projection operations,
/// evaluating expressions and producing output with projected fields.
#[derive(Debug, Clone)]
pub struct PhysicalProject {
    pub base: BasePhysicalPlan,
    pub fields: Vec<PhysicalProjectField>,
}

impl PhysicalProject {
    /// Create a new PhysicalProject
    pub fn new(fields: Vec<PhysicalProjectField>, children: Vec<Arc<dyn PhysicalPlan>>, index: i64) -> Self {
        let base = BasePhysicalPlan::new(children, index);
        Self { base, fields }
    }
    
    /// Create a new PhysicalProject with a single child
    pub fn with_single_child(fields: Vec<PhysicalProjectField>, child: Arc<dyn PhysicalPlan>, index: i64) -> Self {
        let base = BasePhysicalPlan::new(vec![child], index);
        Self { base, fields }
    }
}

impl PhysicalPlan for PhysicalProject {
    fn children(&self) -> &[Arc<dyn PhysicalPlan>] {
        &self.base.children
    }
    
    fn get_plan_type(&self) -> &str {
        "PhysicalProject"
    }
    
    fn get_plan_index(&self) -> &i64 {
        &self.base.index
    }
    
    fn as_any(&self) -> &dyn Any {
        self
    }
}