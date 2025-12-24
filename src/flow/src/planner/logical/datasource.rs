use crate::catalog::EventtimeDefinition;
use crate::catalog::StreamDecoderConfig;
use crate::planner::logical::BaseLogicalPlan;
use datatypes::Schema;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct DataSource {
    pub base: BaseLogicalPlan,
    pub source_name: String,
    pub alias: Option<String>,
    pub decoder: StreamDecoderConfig,
    pub schema: Arc<Schema>,
    pub eventtime: Option<EventtimeDefinition>,
}

impl DataSource {
    pub fn new(
        source_name: String,
        alias: Option<String>,
        decoder: StreamDecoderConfig,
        index: i64,
        schema: Arc<Schema>,
        eventtime: Option<EventtimeDefinition>,
    ) -> Self {
        let base = BaseLogicalPlan::new(vec![], index);
        Self {
            base,
            source_name,
            alias,
            decoder,
            schema,
            eventtime,
        }
    }

    pub fn decoder(&self) -> &StreamDecoderConfig {
        &self.decoder
    }

    pub fn schema(&self) -> Arc<Schema> {
        Arc::clone(&self.schema)
    }

    pub fn eventtime(&self) -> Option<&EventtimeDefinition> {
        self.eventtime.as_ref()
    }
}
