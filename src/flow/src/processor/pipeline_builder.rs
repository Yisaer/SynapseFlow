//! Stream processing pipeline builder
//! 
//! Builds a pipeline of StreamProcessors from a PhysicalPlan tree.
//! Each processor runs in its own tokio task and communicates via broadcast channels.

use std::sync::Arc;
use crate::planner::physical::{PhysicalPlan, PhysicalDataSource, PhysicalFilter, PhysicalProject};
use crate::processor::{DataSourceProcessor, FilterProcessor, ProjectProcessor, ProcessorView, StreamData, stream_processor::StreamProcessor};
use tokio::sync::broadcast;

/// Represents a processor node in the execution pipeline
pub struct ProcessorNode {
    /// The processor instance
    pub processor: Arc<dyn crate::processor::StreamProcessor>,
    /// View for controlling and monitoring the processor
    pub processor_view: ProcessorView,
}

impl std::fmt::Debug for ProcessorNode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProcessorNode")
            .field("processor", &"<dyn StreamProcessor>")
            .field("processor_view", &self.processor_view)
            .finish()
    }
}

/// Build a stream processing pipeline from a physical plan
/// 
/// This function walks through the physical plan tree and creates corresponding processor nodes.
/// Each processor runs in its own tokio task and communicates via broadcast channels.
pub fn build_processor_pipeline(
    physical_plan: Arc<dyn PhysicalPlan>,
) -> Result<ProcessorNode, String> {
    // Build the pipeline recursively
    build_processor_node(physical_plan)
}

/// Build a processor node and its subtree using type-safe downcasting
/// 
/// This function uses downcast_ref to match concrete physical plan types,
/// providing better type safety than string-based matching.
fn build_processor_node(
    physical_plan: Arc<dyn PhysicalPlan>,
) -> Result<ProcessorNode, String> {
    // First, recursively build all child processors and collect their output receivers
    let mut child_receivers = Vec::new();
    
    for child in physical_plan.children() {
        let child_node = build_processor_node(child.clone())?;
        let result_rx = child_node.processor_view.result_resubscribe();
        child_receivers.push(result_rx);
    }
    
    // Use downcast_ref for type-safe matching of concrete plan types
    if let Some(data_source) = physical_plan.as_any().downcast_ref::<PhysicalDataSource>() {
        build_data_source_processor(data_source, physical_plan.clone(), child_receivers)
    } else if let Some(filter) = physical_plan.as_any().downcast_ref::<PhysicalFilter>() {
        build_filter_processor(filter, physical_plan.clone(), child_receivers)
    } else if let Some(project) = physical_plan.as_any().downcast_ref::<PhysicalProject>() {
        build_project_processor(project, physical_plan.clone(), child_receivers)
    } else {
        Err(format!(
            "Unsupported physical plan type: {:?}", 
            std::any::type_name_of_val(physical_plan.as_ref())
        ))
    }
}

/// Build a DataSource processor
/// 
/// DataSource is typically a leaf node but we treat all plans uniformly,
/// allowing for future extensions where DataSource might have children.
fn build_data_source_processor(
    _data_source: &PhysicalDataSource,
    physical_plan: Arc<dyn PhysicalPlan>,
    upstream_receivers: Vec<broadcast::Receiver<Result<StreamData, String>>>,
) -> Result<ProcessorNode, String> {
    // For now, assume 1 downstream processor (can be improved later)
    let downstream_count = 1;
    
    let processor = Arc::new(DataSourceProcessor::new(
        physical_plan,
        upstream_receivers,
        downstream_count,
    ));
    
    let processor_view = processor.start();
    Ok(ProcessorNode {
        processor,
        processor_view,
    })
}

/// Build a Filter processor
/// 
/// Filter processes data from its children (upstream) and applies filtering logic.
fn build_filter_processor(
    _filter: &PhysicalFilter,
    physical_plan: Arc<dyn PhysicalPlan>,
    upstream_receivers: Vec<broadcast::Receiver<Result<StreamData, String>>>,
) -> Result<ProcessorNode, String> {
    let downstream_count = 1;
    
    let processor = Arc::new(FilterProcessor::new(
        physical_plan,
        upstream_receivers,
        downstream_count,
    ));
    
    let processor_view = processor.start();
    Ok(ProcessorNode {
        processor,
        processor_view,
    })
}

/// Build a Project processor
/// 
/// Project processes data from its children (upstream) and applies projection logic.
fn build_project_processor(
    _project: &PhysicalProject,
    physical_plan: Arc<dyn PhysicalPlan>,
    upstream_receivers: Vec<broadcast::Receiver<Result<StreamData, String>>>,
) -> Result<ProcessorNode, String> {
    let downstream_count = 1;
    
    let processor = Arc::new(ProjectProcessor::new(
        physical_plan,
        upstream_receivers,
        downstream_count,
    ));
    
    let processor_view = processor.start();
    Ok(ProcessorNode {
        processor,
        processor_view,
    })
}

/// Execute a complete processor pipeline
/// 
/// This is a convenience function that builds and starts the pipeline.
/// Returns the view of the root processor for result consumption and control.
pub fn execute_pipeline(
    physical_plan: Arc<dyn PhysicalPlan>,
) -> Result<ProcessorView, String> {
    let pipeline = build_processor_pipeline(physical_plan)?;
    Ok(pipeline.processor_view)
}