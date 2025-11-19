use crate::expr::custom_func::CustomFunc;
use crate::expr::func::EvalError;
use datatypes::Value;

/// Custom implementation of the concat function
/// This function concatenates exactly 2 String arguments
#[derive(Debug, Clone)]
pub struct ConcatFunc;

impl CustomFunc for ConcatFunc {
    fn validate_row(&self, args: &[Value]) -> Result<(), EvalError> {
        if args.len() != 2 {
            return Err(EvalError::TypeMismatch {
                expected: "2 arguments".to_string(),
                actual: format!("{} arguments", args.len()),
            });
        }
        for (idx, arg) in args.iter().enumerate() {
            if !matches!(arg, Value::String(_)) {
                return Err(EvalError::TypeMismatch {
                    expected: "String".to_string(),
                    actual: format!("{:?} at argument {}", arg, idx),
                });
            }
        }
        Ok(())
    }

    fn eval_row(&self, args: &[Value]) -> Result<Value, EvalError> {
        self.validate_row(args)?;
        let first = match &args[0] {
            Value::String(s) => s,
            _ => unreachable!("validated as string"),
        };
        let second = match &args[1] {
            Value::String(s) => s,
            _ => unreachable!("validated as string"),
        };
        Ok(Value::String(format!("{}{}", first, second)))
    }

    fn name(&self) -> &str {
        "concat"
    }
}
