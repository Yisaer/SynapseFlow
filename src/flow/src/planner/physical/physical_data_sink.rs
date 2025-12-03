use crate::planner::physical::BasePhysicalPlan;
use crate::planner::sink::SinkConnectorConfig;
use std::fmt;
use std::sync::Arc;

use super::PhysicalPlan;

/// Physical plan node for sink stage.
#[derive(Clone)]
pub struct PhysicalDataSink {
    pub base: BasePhysicalPlan,
    pub connectors: Vec<PhysicalSinkConnector>,
}

impl PhysicalDataSink {
    pub fn new(
        children: Vec<Arc<PhysicalPlan>>,
        index: i64,
        connectors: Vec<PhysicalSinkConnector>,
    ) -> Self {
        Self {
            base: BasePhysicalPlan::new(children, index),
            connectors,
        }
    }
}

impl fmt::Debug for PhysicalDataSink {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PhysicalDataSink")
            .field("index", &self.base.index())
            .field("connector_count", &self.connectors.len())
            .finish()
    }
}

/// Declarative description of a sink connector bound to an encoder node.
#[derive(Clone)]
pub struct PhysicalSinkConnector {
    pub sink_id: String,
    pub forward_to_result: bool,
    pub connector_id: String,
    pub connector: SinkConnectorConfig,
    pub encoder_plan_index: i64,
}

impl PhysicalSinkConnector {
    pub fn new(
        sink_id: String,
        forward_to_result: bool,
        connector_id: String,
        connector: SinkConnectorConfig,
        encoder_plan_index: i64,
    ) -> Self {
        Self {
            sink_id,
            forward_to_result,
            connector_id,
            connector,
            encoder_plan_index,
        }
    }
}
