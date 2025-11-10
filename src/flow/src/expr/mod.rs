pub mod context;
pub mod custom_func;
pub mod datafusion_adapter;
pub mod evaluator;
pub mod func;
pub mod scalar;
pub mod sql_conversion;

pub use context::EvalContext;
pub use custom_func::ConcatFunc;
pub use datafusion_adapter::*;
pub use evaluator::DataFusionEvaluator;
pub use func::{BinaryFunc, UnaryFunc};
pub use scalar::{CustomFunc, ScalarExpr};
pub use sql_conversion::{
    convert_expr_to_scalar, convert_expr_to_scalar_with_schema, convert_select_stmt_to_scalar,
    extract_select_expressions, extract_select_expressions_with_aliases, parse_sql_to_scalar_expr,
    ConversionError, StreamSqlConverter,
};
