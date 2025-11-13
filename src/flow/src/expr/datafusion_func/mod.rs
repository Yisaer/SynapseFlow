//! DataFusion integration module for flow expressions
//!
//! This module provides functionality to integrate DataFusion expressions
//! and functions with the flow expression system.

pub mod adapter;
pub mod evaluator;

// Re-export main types for convenience
pub use adapter::{
    concrete_datatype_to_arrow_type, create_df_function_call, flow_schema_to_arrow_schema,
    scalar_value_to_value, value_to_scalar_value, AdapterError,
};

pub use datafusion_common::{DataFusionError, Result as DataFusionResult};
pub use evaluator::DataFusionEvaluator;
