//! Stream processing pipeline builder
//! 
//! Builds a pipeline of StreamProcessors from a PhysicalPlan tree.
//! Each processor runs in its own tokio task and communicates via broadcast channels.
//! 
//! Now supports flexible pipeline construction with custom start/end nodes for:
//! - Manual control signal injection at pipeline start
//! - Data forwarding from the pipeline end to external consumers

use std::sync::Arc;
use crate::planner::physical::{PhysicalPlan, PhysicalDataSource, PhysicalFilter, PhysicalProject};
use crate::processor::{DataSourceProcessor, FilterProcessor, ProjectProcessor, ProcessorView, ProcessorHandle, StreamData, stream_processor::StreamProcessor};
use crate::processor::control_source_processor::ControlSourceProcessor;
use crate::processor::result_sink_processor::ResultSinkProcessor;
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

/// Pipeline endpoints for flexible pipeline construction
/// 
/// Allows specifying custom start and end nodes for the pipeline, enabling:
/// - Manual control signal injection at the pipeline start
/// - Data forwarding from the pipeline end to external consumers
pub struct PipelineEndpoints {
    /// The start node of the pipeline (can be used to inject control signals)
    pub start_node: ProcessorNode,
    /// The end node of the pipeline (receives final processed data)
    pub end_node: ProcessorNode,
}

impl PipelineEndpoints {
    /// Create new pipeline endpoints
    pub fn new(start_node: ProcessorNode, end_node: ProcessorNode) -> Self {
        Self {
            start_node,
            end_node,
        }
    }
    
    /// Get the start node's processor view for sending control signals
    pub fn start_view(&self) -> &ProcessorView {
        &self.start_node.processor_view
    }
    
    /// Get the end node's processor view for receiving processed data
    pub fn end_view(&self) -> &ProcessorView {
        &self.end_node.processor_view
    }
    
    /// Stop all processors in the pipeline
    pub fn stop_all(&self) -> Vec<Result<usize, broadcast::error::SendError<()>>> {
        vec![
            self.start_node.processor_view.stop(),
            self.end_node.processor_view.stop(),
        ]
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

/// Build a pipeline with explicit start and end control points
/// 
/// Creates a pipeline where:
/// - Start node allows manual control signal injection
/// - End node provides access to final processed data
/// - Middle nodes handle the actual data processing
/// 
/// Example usage:
/// ```rust
/// let endpoints = build_processor_pipeline_with_endpoints(physical_plan)?;
/// 
/// // Send control signal to start
/// endpoints.send_control(StreamData::stream_start())?;
/// 
/// // Receive processed data from end
/// let result = endpoints.end_view().result_receiver.recv().await?;
/// ```
pub fn build_processor_pipeline_with_endpoints(
    physical_plan: Arc<dyn PhysicalPlan>,
) -> Result<PipelineEndpoints, String> {
    // Build the main processing pipeline
    let main_pipeline = build_processor_pipeline(physical_plan)?;
    
    // For simple pipelines, start and end are the same node
    // This allows both control injection and result collection from the same point
    // In complex topologies, these could be different nodes
    // Note: We can't clone ProcessorNode directly, so we use the same node for both start and end
    let start_node = ProcessorNode {
        processor: main_pipeline.processor.clone(),
        processor_view: ProcessorView::new(
            main_pipeline.processor_view.result_resubscribe(),
            main_pipeline.processor_view.stop_sender.clone(),
            ProcessorHandle::new(tokio::spawn(async {})), // Dummy handle
        ),
    };
    
    Ok(PipelineEndpoints::new(
        start_node,
        main_pipeline,
    ))
}

/// Build a pipeline with external control and result collection capabilities
/// 
/// Creates a complete pipeline with:
/// 1. Control source node (start) - receives external control signals
/// 2. Main processing chain - processes data according to physical plan
/// 3. Result sink node (end) - forwards results to external consumers
/// 
/// Returns a complete pipeline setup with external interaction channels.
pub fn build_pipeline_with_external_io(
    physical_plan: Arc<dyn PhysicalPlan>,
) -> Result<ExternalPipeline, String> {
    // Create external interaction channels
    let (control_tx, control_rx) = broadcast::channel(1024);
    let (result_tx, result_rx) = broadcast::channel(1024);
    
    // Build the main processing chain
    let processing_pipeline = build_processor_pipeline(physical_plan)?;
    
    // Create control source node (pipeline entry point)
    let control_source = Arc::new(ControlSourceProcessor::with_name(
        "PipelineEntry".to_string(),
        control_rx,
        1, // Send to processing chain
    ));
    
    let control_source_view = control_source.start();
    let control_source_node = ProcessorNode {
        processor: control_source,
        processor_view: control_source_view,
    };
    
    // Create result sink node (pipeline exit point)
    let result_sink = Arc::new(ResultSinkProcessor::with_name(
        "PipelineExit".to_string(),
        vec![processing_pipeline.processor_view.result_resubscribe()],
        result_tx,
        0, // No downstream for sink
    ));
    
    let result_sink_view = result_sink.start();
    let result_sink_node = ProcessorNode {
        processor: result_sink,
        processor_view: result_sink_view,
    };
    
    Ok(ExternalPipeline {
        control_sender: control_tx,
        result_receiver: result_rx,
        control_source: control_source_node,
        processing_chain: processing_pipeline,
        result_sink: result_sink_node,
    })
}

/// Complete pipeline with external I/O capabilities
pub struct ExternalPipeline {
    /// Sender for external control signal injection
    pub control_sender: broadcast::Sender<StreamData>,
    /// Receiver for external result collection
    pub result_receiver: broadcast::Receiver<StreamData>,
    /// Control source node (pipeline entry)
    pub control_source: ProcessorNode,
    /// Main processing chain
    pub processing_chain: ProcessorNode,
    /// Result sink node (pipeline exit)
    pub result_sink: ProcessorNode,
}

impl ExternalPipeline {
    /// Send a control signal to the pipeline start
    pub fn send_control(&self, signal: StreamData) -> Result<usize, broadcast::error::SendError<StreamData>> {
        self.control_sender.send(signal)
    }
    
    /// Receive processed data from pipeline end
    pub async fn receive_result(&mut self) -> Result<StreamData, broadcast::error::RecvError> {
        self.result_receiver.recv().await
    }
    
    /// Get the main processing chain view
    pub fn processing_view(&self) -> &ProcessorView {
        &self.processing_chain.processor_view
    }
    
    /// Stop the entire pipeline
    pub fn stop_all(&self) -> Vec<Result<usize, broadcast::error::SendError<()>>> {
        vec![
            self.control_source.processor_view.stop(),
            self.processing_chain.processor_view.stop(),
            self.result_sink.processor_view.stop(),
        ]
    }
    
    /// Get pipeline statistics
    pub fn get_stats(&self) -> PipelineStats {
        PipelineStats {
            control_signals_sent: self.control_sender.len(),
            results_available: self.result_receiver.len(),
            is_processing_active: !self.processing_chain.processor_view.result_receiver.is_empty(),
        }
    }
}

/// Pipeline statistics for monitoring
#[derive(Debug, Clone)]
pub struct PipelineStats {
    pub control_signals_sent: usize,
    pub results_available: usize,
    pub is_processing_active: bool,
}

/// Build a pipeline with external control capabilities
/// 
/// Creates a pipeline with:
/// 1. A control injection point that can receive external signals
/// 2. The main processing chain based on the physical plan
/// 3. A result collection point for external consumption
/// 
/// Returns channels for external interaction:
/// - `control_tx`: Send control signals to pipeline start
/// - `result_rx`: Receive processed data from pipeline end
pub fn build_pipeline_with_external_control(
    physical_plan: Arc<dyn PhysicalPlan>,
) -> Result<(broadcast::Sender<StreamData>, broadcast::Receiver<StreamData>, ProcessorNode), String> {
    let external_pipeline = build_pipeline_with_external_io(physical_plan)?;
    
    Ok((
        external_pipeline.control_sender,
        external_pipeline.result_receiver,
        external_pipeline.processing_chain,
    ))
}

/// Build a processor node and its subtree using type-safe downcasting
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
fn build_data_source_processor(
    _data_source: &PhysicalDataSource,
    physical_plan: Arc<dyn PhysicalPlan>,
    upstream_receivers: Vec<broadcast::Receiver<StreamData>>,
) -> Result<ProcessorNode, String> {
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
fn build_filter_processor(
    _filter: &PhysicalFilter,
    physical_plan: Arc<dyn PhysicalPlan>,
    upstream_receivers: Vec<broadcast::Receiver<StreamData>>,
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
fn build_project_processor(
    _project: &PhysicalProject,
    physical_plan: Arc<dyn PhysicalPlan>,
    upstream_receivers: Vec<broadcast::Receiver<StreamData>>,
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
pub fn execute_pipeline(
    physical_plan: Arc<dyn PhysicalPlan>,
) -> Result<ProcessorView, String> {
    let pipeline = build_processor_pipeline(physical_plan)?;
    Ok(pipeline.processor_view)
}