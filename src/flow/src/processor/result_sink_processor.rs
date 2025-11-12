//! Result Sink processor - pipeline exit point for collecting processed data
//! 
//! This processor acts as a pipeline exit point that collects final processed data
//! and forwards it to external consumers via a result channel.
//! 
//! Redesigned with single input channel architecture:
//! - Single input channel for receiving data from upstream
//! - Multiple output channels (typically 0 for sink, but supports broadcasting)

use tokio::sync::broadcast;
use std::sync::Arc;
use crate::processor::{StreamProcessor, ProcessorView, utils, StreamData, ProcessorHandle};

/// ResultSink processor that serves as a pipeline exit point
/// 
/// This processor:
/// 1. Receives processed data from upstream processors via single input channel
/// 2. Forwards results to external consumers via a result channel
/// 3. Handles pipeline completion and cleanup
/// Now designed with single input, multiple output architecture.
pub struct ResultSinkProcessor {
    /// Input receiver - receives data from upstream
    input_receiver: broadcast::Receiver<StreamData>,
    /// Result channel for forwarding processed data to external consumers
    result_sender: broadcast::Sender<StreamData>,
    /// Number of downstream processors (typically 0 for sink)
    downstream_count: usize,
    /// Processor name for debugging
    processor_name: String,
}

impl ResultSinkProcessor {
    /// Create a new ResultSinkProcessor
    pub fn new(
        upstream_receiver: broadcast::Receiver<StreamData>,
        result_sender: broadcast::Sender<StreamData>,
        downstream_count: usize,
    ) -> Self {
        Self {
            input_receiver: upstream_receiver,
            result_sender,
            downstream_count,
            processor_name: "ResultSinkProcessor".to_string(),
        }
    }
    
    /// Create a ResultSinkProcessor with single upstream input (common case)
    pub fn with_single_upstream(
        upstream_receiver: broadcast::Receiver<StreamData>,
        result_sender: broadcast::Sender<StreamData>,
    ) -> Self {
        Self::new(upstream_receiver, result_sender, 0)
    }
    
    /// Create a ResultSinkProcessor with a name for debugging
    pub fn with_name(
        name: String,
        upstream_receiver: broadcast::Receiver<StreamData>,
        result_sender: broadcast::Sender<StreamData>,
        downstream_count: usize,
    ) -> Self {
        Self {
            input_receiver: upstream_receiver,
            result_sender,
            downstream_count,
            processor_name: name,
        }
    }
}

impl StreamProcessor for ResultSinkProcessor {
    fn start(&self, input_receiver: broadcast::Receiver<StreamData>) -> ProcessorView {
        // Create output channels (even though typically empty for sink)
        let output_senders = utils::create_output_senders(self.downstream_count);
        
        // Clone data that will be used in the async task
        let processor_name = self.processor_name.clone();
        let output_senders_clone = output_senders.clone();
        
        // Clone the result sender for external forwarding
        let result_sender_clone = self.result_sender.clone();
        let result_sender = self.result_sender.clone();
        
        // Clone for the routine (Receiver supports resubscribe)
        let mut routine_input_receiver = input_receiver.resubscribe();
        
        // Spawn the result sink routine
        let routine = async move {
            println!("{}: Starting result sink routine", processor_name);
            
            // Main collection loop
            loop {
                // Receive processed data from upstream
                match routine_input_receiver.recv().await {
                    Ok(stream_data) => {
                        println!("{}: Received processed data: {:?}", processor_name, stream_data.description());
                        
                        // Check if this is a terminal signal
                        let is_terminal = stream_data.is_terminal();
                        
                        // Forward to external consumers (clone to avoid move)
                        if result_sender.send(stream_data.clone()).is_err() {
                            println!("{}: No external consumers remaining, continuing to collect", processor_name);
                            // Continue collecting even if no external consumers
                            // This prevents data loss in the pipeline
                        }
                        
                        // Also forward to output channels if any (for chaining)
                        for sender in &output_senders_clone {
                            if sender.send(stream_data.clone()).is_err() {
                                println!("{}: Failed to forward to output channel", processor_name);
                            }
                        }
                        
                        if is_terminal {
                            println!("{}: Received terminal signal, completing collection", processor_name);
                            break;
                        }
                    }
                    Err(e) => {
                        // Handle broadcast errors from upstream
                        let error_data = utils::handle_receive_error(e);
                        println!("{}: Upstream error: {:?}", processor_name, error_data.description());
                        
                        // Forward error to external consumers
                        let _ = result_sender.send(error_data.clone());
                        
                        // Also forward to output channels
                        for sender in &output_senders_clone {
                            let _ = sender.send(error_data.clone());
                        }
                        break;
                    }
                }
            }
            
            println!("{}: Result sink routine completed", processor_name);
        };
        
        let join_handle = tokio::spawn(routine);
        
        // For ResultSinkProcessor, we expose the result sender for external forwarding
        ProcessorView::new(
            Some(result_sender_clone),
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
impl ResultSinkProcessor {
    /// Create result sink routine that runs in tokio task
    /// Now with single input, multiple output architecture
    pub fn create_result_sink_routine(
        self: Arc<Self>,
        mut input_receiver: broadcast::Receiver<StreamData>,
        output_senders: Vec<broadcast::Sender<StreamData>>,
    ) -> impl std::future::Future<Output = ()> + Send + 'static {
        let result_sender = self.result_sender.clone();
        let processor_name = self.processor_name.clone();
        
        async move {
            println!("{}: Starting result sink routine", processor_name);
            
            // Main collection loop
            loop {
                // Receive processed data from upstream
                match input_receiver.recv().await {
                    Ok(stream_data) => {
                        println!("{}: Received processed data: {:?}", processor_name, stream_data.description());
                        
                        // Check if this is a terminal signal
                        let is_terminal = stream_data.is_terminal();
                        
                        // Forward to external consumers (clone to avoid move)
                        if result_sender.send(stream_data.clone()).is_err() {
                            println!("{}: No external consumers remaining, continuing to collect", processor_name);
                            // Continue collecting even if no external consumers
                            // This prevents data loss in the pipeline
                        }
                        
                        // Also forward to output channels if any (for chaining)
                        for sender in &output_senders {
                            if sender.send(stream_data.clone()).is_err() {
                                println!("{}: Failed to forward to output channel", processor_name);
                            }
                        }
                        
                        if is_terminal {
                            println!("{}: Received terminal signal, completing collection", processor_name);
                            break;
                        }
                    }
                    Err(e) => {
                        // Handle broadcast errors from upstream
                        let error_data = utils::handle_receive_error(e);
                        println!("{}: Upstream error: {:?}", processor_name, error_data.description());
                        
                        // Forward error to external consumers
                        let _ = result_sender.send(error_data.clone());
                        
                        // Also forward to output channels
                        for sender in &output_senders {
                            let _ = sender.send(error_data.clone());
                        }
                        break;
                    }
                }
            }
            
            println!("{}: Result sink routine completed", processor_name);
        }
    }
}