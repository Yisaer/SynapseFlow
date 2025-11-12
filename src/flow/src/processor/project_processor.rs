//! Project processor - corresponds to PhysicalProject
//! 
//! This processor projects (selects/renames) fields from incoming data.
//! Currently just passes data through for pipeline establishment.
//! 
//! Redesigned with single input channel architecture:
//! - Single input channel for receiving data from upstream
//! - Multiple output channels for broadcasting projected results

use tokio::sync::broadcast;
use std::sync::Arc;
use crate::planner::physical::PhysicalPlan;
use crate::processor::{StreamProcessor, ProcessorView, ProcessorHandle, utils, StreamData, StreamError};

/// Project processor that corresponds to PhysicalProject
/// 
/// This processor projects fields from incoming data based on projection expressions.
/// Now designed with single input, multiple output architecture.
pub struct ProjectProcessor {
    /// The physical plan this processor corresponds to
    physical_plan: Arc<dyn PhysicalPlan>,
    /// Input channel - receives data from upstream
    input_channel: (broadcast::Sender<StreamData>, broadcast::Receiver<StreamData>),
    /// Output channels - broadcast projected results to downstream
    output_channels: Vec<(broadcast::Sender<StreamData>, broadcast::Receiver<StreamData>)>,
    /// Number of downstream processors this will broadcast to
    downstream_count: usize,
}

impl ProjectProcessor {
    /// Create a new ProjectProcessor
    pub fn new(
        physical_plan: Arc<dyn PhysicalPlan>,
        downstream_count: usize,
    ) -> Self {
        // Create input channel
        let input_channel = utils::create_channel(1);
        
        // Create output channels for broadcasting to downstream
        let output_channels = utils::create_output_channels(downstream_count);
        
        Self {
            physical_plan,
            input_channel,
            output_channels,
            downstream_count,
        }
    }
    
    /// Apply projection to data (currently just passes through)
    fn project_data(&self, data: Box<dyn crate::model::Collection>) -> Result<Box<dyn crate::model::Collection>, String> {
        // TODO: Implement actual projection logic based on projection expressions
        // For now, return data as-is to establish pipeline
        Ok(data)
    }
}

impl StreamProcessor for ProjectProcessor {
    fn start(&self, input_receiver: broadcast::Receiver<StreamData>) -> ProcessorView {
        // Create output channels for broadcasting to downstream
        let output_senders: Vec<broadcast::Sender<StreamData>> = utils::create_output_senders(self.downstream_count);
        
        // Clone data that will be used in the async task
        let processor_name = "ProjectProcessor".to_string();
        let downstream_count = self.downstream_count;
        let output_senders_clone = output_senders.clone();
        
        // Clone for the routine (Receiver supports resubscribe)
        let mut routine_input_receiver = input_receiver.resubscribe();
        
        // Spawn the project routine
        let routine = async move {
            println!("{}: Starting project routine for {} downstream processors", 
                     processor_name, downstream_count);
            
            // Forward all control signals and project data
            loop {
                match routine_input_receiver.recv().await {
                    Ok(stream_data) => {
                        println!("{}: Processing input: {:?}", processor_name, stream_data.description());
                        
                        // Check for stop signal
                        if utils::is_stop_signal(&stream_data) {
                            println!("{}: Received stop signal, shutting down gracefully", processor_name);
                            // Forward stop signal to all outputs
                            for sender in &output_senders_clone {
                                let _ = sender.send(stream_data.clone());
                            }
                            break;
                        }
                        
                        // Forward control signals to all outputs
                        if stream_data.is_control() {
                            for sender in &output_senders_clone {
                                if sender.send(stream_data.clone()).is_err() {
                                    println!("{}: Failed to forward control signal to some outputs", processor_name);
                                }
                            }
                            continue;
                        }
                        
                        // Project data items - for now just forward everything
                        if stream_data.is_data() {
                            // For now, forward all data without actual projection
                            for sender in &output_senders_clone {
                                if sender.send(stream_data.clone()).is_err() {
                                    println!("{}: Failed to forward data to some outputs", processor_name);
                                }
                            }
                        }
                    }
                    Err(e) => {
                        println!("{}: Input channel error: {}", processor_name, e);
                        // Send error to all outputs
                        let error_data = utils::handle_receive_error(e);
                        for sender in &output_senders_clone {
                            let _ = sender.send(error_data.clone());
                        }
                        break;
                    }
                }
            }
            
            println!("{}: Project routine completed", processor_name);
        };
        
        let join_handle = tokio::spawn(routine);
        
        // For ProjectProcessor, we create input sender for control signals
        let (input_sender, _) = utils::create_input_channel();
        
        ProcessorView::new(
            Some(input_sender),
            input_receiver,
            output_senders,
            ProcessorHandle::new(join_handle),
        )
    }
    
    fn downstream_count(&self) -> usize {
        self.downstream_count
    }
    
    fn create_input_channel(&self) -> (broadcast::Sender<StreamData>, broadcast::Receiver<StreamData>) {
        utils::create_input_channel()
    }
}

// Private helper methods
impl ProjectProcessor {
    /// Create project routine that runs in tokio task
    /// Now with single input, multiple output architecture
    pub fn create_project_routine(
        self: Arc<Self>,
        mut input_receiver: broadcast::Receiver<StreamData>,
        output_senders: Vec<broadcast::Sender<StreamData>>,
    ) -> impl std::future::Future<Output = ()> + Send + 'static {
        let processor_name = "ProjectProcessor".to_string();
        let downstream_count = self.downstream_count;
        
        // Clone projection function
        let project_func = self.clone();
        
        async move {
            println!("{}: Starting project routine for {} downstream processors", 
                     processor_name, downstream_count);
            
            // Forward all control signals and project data
            loop {
                match input_receiver.recv().await {
                    Ok(stream_data) => {
                        println!("{}: Processing input: {:?}", processor_name, stream_data.description());
                        
                        // Check for stop signal
                        if utils::is_stop_signal(&stream_data) {
                            println!("{}: Received stop signal, shutting down gracefully", processor_name);
                            // Forward stop signal to all outputs
                            for sender in &output_senders {
                                let _ = sender.send(stream_data.clone());
                            }
                            break;
                        }
                        
                        // Forward control signals to all outputs
                        if stream_data.is_control() {
                            for sender in &output_senders {
                                if sender.send(stream_data.clone()).is_err() {
                                    println!("{}: Failed to forward control signal to some outputs", processor_name);
                                }
                            }
                            continue;
                        }
                        
                        // Project data items
                        if stream_data.is_data() {
                            match stream_data {
                                StreamData::Collection(collection) => {
                                    // Apply projection
                                    match project_func.project_data(collection) {
                                        Ok(projected_data) => {
                                            // Broadcast projected data to all outputs
                                            let projected_signal = StreamData::Collection(projected_data);
                                            for sender in &output_senders {
                                                if sender.send(projected_signal.clone()).is_err() {
                                                    println!("{}: Failed to send projected data to some outputs", processor_name);
                                                }
                                            }
                                        }
                                        Err(e) => {
                                            // Projection error - send error to all outputs
                                            println!("{}: Projection error: {}", processor_name, e);
                                            let error_data = StreamData::error(StreamError::new(e));
                                            for sender in &output_senders {
                                                let _ = sender.send(error_data.clone());
                                            }
                                        }
                                    }
                                }
                                _ => {
                                    // Non-collection data - forward as-is
                                    for sender in &output_senders {
                                        if sender.send(stream_data.clone()).is_err() {
                                            println!("{}: Failed to forward non-collection data to some outputs", processor_name);
                                        }
                                    }
                                }
                            }
                        }
                    }
                    Err(e) => {
                        println!("{}: Input channel error: {}", processor_name, e);
                        // Send error to all outputs
                        let error_data = utils::handle_receive_error(e);
                        for sender in &output_senders {
                            let _ = sender.send(error_data.clone());
                        }
                        break;
                    }
                }
            }
            
            println!("{}: Project routine completed", processor_name);
        }
    }
}