//! DataFusion integration module for flow expressions.
//!
//! The heavy dependencies are guarded behind the optional `datafusion` feature so
//! default builds can skip compiling DataFusion entirely.

pub mod adapter;
pub mod evaluator;

pub use adapter::DATAFUSION_FUNCTIONS;
pub use evaluator::DataFusionEvaluator;

#[cfg(feature = "datafusion")]
pub use adapter::{
    concrete_datatype_to_arrow_type, create_df_function_call, flow_schema_to_arrow_schema,
    scalar_value_to_value, value_to_scalar_value, AdapterError,
};

#[cfg(feature = "datafusion")]
pub use datafusion_common::{DataFusionError, Result as DataFusionResult};

#[cfg(not(feature = "datafusion"))]
pub use evaluator::{DataFusionError, DataFusionResult};
