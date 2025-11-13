pub mod context;
pub mod custom_func;
pub mod datafusion_func;
pub mod func;
pub mod scalar;
pub mod sql_conversion;

pub use context::EvalContext;
pub use custom_func::ConcatFunc;
pub use datafusion_func::DataFusionEvaluator;
#[cfg(feature = "datafusion")]
pub use datafusion_func::{
    concrete_datatype_to_arrow_type, create_df_function_call, flow_schema_to_arrow_schema,
    scalar_value_to_value, value_to_scalar_value, AdapterError, DataFusionError, DataFusionResult,
};
pub use func::{BinaryFunc, UnaryFunc};
pub use scalar::ScalarExpr;
pub use sql_conversion::{
    convert_expr_to_scalar, convert_select_stmt_to_scalar, extract_select_expressions,
    ConversionError, StreamSqlConverter,
};
