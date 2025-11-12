//! ProjectProcessor - processes projection operations
//!
//! This processor evaluates projection expressions and produces output with projected fields.

use tokio::sync::mpsc;
use std::sync::Arc;
use crate::processor::{Processor, ProcessorError, StreamData};
use crate::planner::physical::{PhysicalProject, PhysicalProjectField};
use crate::model::Collection;

/// ProjectProcessor - evaluates projection expressions
///
/// This processor:
/// - Takes input data (Collection) and projection expressions
/// - Evaluates the expressions to create projected fields
/// - Sends the projected data downstream as StreamData::Collection
pub struct ProjectProcessor {
    /// Processor identifier
    id: String,
    /// Physical projection configuration
    physical_project: Arc<PhysicalProject>,
    /// Input channels for receiving data
    inputs: Vec<mpsc::Receiver<StreamData>>,
    /// Output channels for sending data downstream
    outputs: Vec<mpsc::Sender<StreamData>>,
}

impl ProjectProcessor {
    /// Create a new ProjectProcessor from PhysicalProject
    pub fn new(
        id: impl Into<String>,
        physical_project: Arc<PhysicalProject>,
    ) -> Self {
        Self {
            id: id.into(),
            physical_project,
            inputs: Vec::new(),
            outputs: Vec::new(),
        }
    }
    
    /// Create a ProjectProcessor from a PhysicalPlan
    /// Returns None if the plan is not a PhysicalProject
    pub fn from_physical_plan(
        id: impl Into<String>,
        plan: Arc<dyn crate::planner::physical::PhysicalPlan>,
    ) -> Option<Self> {
        plan.as_any().downcast_ref::<PhysicalProject>().map(|proj| Self::new(id, Arc::new(proj.clone())))
    }
}

/// Apply projection to a collection
fn apply_projection(input_collection: &dyn Collection, fields: &[PhysicalProjectField]) -> Result<Box<dyn Collection>, ProcessorError> {
    // Use the collection's apply_projection method
    input_collection.apply_projection(fields)
        .map_err(|e| ProcessorError::ProcessingError(format!("Failed to apply projection: {}", e)))
}

impl Processor for ProjectProcessor {
    fn id(&self) -> &str {
        &self.id
    }
    
    fn start(&mut self) -> tokio::task::JoinHandle<Result<(), ProcessorError>> {
        let id = self.id.clone();
        let mut inputs = std::mem::take(&mut self.inputs);
        let outputs = self.outputs.clone();
        let fields = self.physical_project.fields.clone();
        
        tokio::spawn(async move {
            loop {
                let mut all_closed = true;
                let mut received_data = false;
                
                // Check all input channels
                for input in &mut inputs {
                    match input.try_recv() {
                        Ok(data) => {
                            all_closed = false;
                            received_data = true;
                            
                            // Handle control signals
                            if let Some(control) = data.as_control() {
                                match control {
                                    crate::processor::ControlSignal::StreamStart => {
                                        // Forward StreamStart to outputs
                                        for output in &outputs {
                                            if output.send(data.clone()).await.is_err() {
                                                return Err(ProcessorError::ChannelClosed);
                                            }
                                        }
                                    }
                                    crate::processor::ControlSignal::StreamEnd => {
                                        // Forward StreamEnd to outputs
                                        for output in &outputs {
                                            let _ = output.send(data.clone()).await;
                                        }
                                        return Ok(());
                                    }
                                    _ => {
                                        // Forward other control signals
                                        for output in &outputs {
                                            if output.send(data.clone()).await.is_err() {
                                                return Err(ProcessorError::ChannelClosed);
                                            }
                                        }
                                    }
                                }
                            } else if let Some(collection) = data.as_collection() {
                                // Apply projection to the collection
                                match apply_projection(collection, &fields) {
                                    Ok(projected_collection) => {
                                        // Send projected data to all outputs
                                        let projected_data = StreamData::collection(projected_collection);
                                        for output in &outputs {
                                            if output.send(projected_data.clone()).await.is_err() {
                                                return Err(ProcessorError::ChannelClosed);
                                            }
                                        }
                                    }
                                    Err(e) => {
                                        // Send error downstream
                                        let error_data = StreamData::error(
                                            crate::processor::StreamError::new(e.to_string())
                                                .with_source(id.clone()),
                                        );
                                        for output in &outputs {
                                            if output.send(error_data.clone()).await.is_err() {
                                                return Err(ProcessorError::ChannelClosed);
                                            }
                                        }
                                    }
                                }
                            } else {
                                // Forward non-collection data (errors, etc.)
                                for output in &outputs {
                                    if output.send(data.clone()).await.is_err() {
                                        return Err(ProcessorError::ChannelClosed);
                                    }
                                }
                            }
                        }
                        Err(mpsc::error::TryRecvError::Empty) => {
                            all_closed = false;
                        }
                        Err(mpsc::error::TryRecvError::Disconnected) => {
                            // Channel disconnected
                        }
                    }
                }
                
                // If all channels are closed and no data received, exit
                if all_closed && !received_data {
                    return Ok(());
                }
                
                // Yield to allow other tasks to run
                tokio::task::yield_now().await;
            }
        })
    }
    
    fn output_senders(&self) -> Vec<mpsc::Sender<StreamData>> {
        self.outputs.clone()
    }
    
    fn add_input(&mut self, receiver: mpsc::Receiver<StreamData>) {
        self.inputs.push(receiver);
    }
    
    fn add_output(&mut self, sender: mpsc::Sender<StreamData>) {
        self.outputs.push(sender);
    }
}