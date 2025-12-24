use crate::catalog::EventtimeDefinition;
use crate::eventtime::EventtimeTypeRegistry;
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Clone, Default)]
pub struct EventtimePipelineContext {
    pub enabled: bool,
    pub registry: Arc<EventtimeTypeRegistry>,
    pub per_source: HashMap<String, EventtimeDefinition>,
}
