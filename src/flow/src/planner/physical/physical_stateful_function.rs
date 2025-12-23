use crate::expr::ScalarExpr;
use crate::planner::physical::BasePhysicalPlan;
use std::sync::Arc;

use super::PhysicalPlan;

#[derive(Debug, Clone)]
pub struct StatefulCall {
    pub output_column: String,
    pub func_name: String,
    pub arg_scalars: Vec<ScalarExpr>,
}

#[derive(Debug, Clone)]
pub struct PhysicalStatefulFunction {
    pub base: BasePhysicalPlan,
    pub calls: Vec<StatefulCall>,
}

impl PhysicalStatefulFunction {
    pub fn new(calls: Vec<StatefulCall>, children: Vec<Arc<PhysicalPlan>>, index: i64) -> Self {
        Self {
            base: BasePhysicalPlan::new(children, index),
            calls,
        }
    }
}

