use crate::aggregation::{AggregateAccumulator, AggregateFunction};
use datatypes::{ConcreteDatatype, Int64Type, Value};
use std::collections::HashSet;

#[derive(Debug, Default)]
pub struct NdvFunction;

impl NdvFunction {
    pub fn new() -> Self {
        Self
    }
}

impl AggregateFunction for NdvFunction {
    fn name(&self) -> &str {
        "ndv"
    }

    fn return_type(&self, input_types: &[ConcreteDatatype]) -> Result<ConcreteDatatype, String> {
        if input_types.len() != 1 {
            return Err(format!(
                "NDV expects exactly one argument, got {}",
                input_types.len()
            ));
        }
        Ok(ConcreteDatatype::Int64(Int64Type))
    }

    fn create_accumulator(&self) -> Box<dyn AggregateAccumulator> {
        Box::new(NdvAccumulator::default())
    }
}

#[derive(Debug, Default, Clone)]
struct NdvAccumulator {
    distinct_values: HashSet<Value>,
}

impl AggregateAccumulator for NdvAccumulator {
    fn update(&mut self, args: &[Value]) -> Result<(), String> {
        let Some(value) = args.first() else {
            return Err("NDV expects one argument".to_string());
        };
        if value.is_null() {
            return Ok(());
        }
        self.distinct_values.insert(value.clone());
        Ok(())
    }

    fn finalize(&self) -> Value {
        Value::Int64(i64::try_from(self.distinct_values.len()).unwrap_or(i64::MAX))
    }
}
