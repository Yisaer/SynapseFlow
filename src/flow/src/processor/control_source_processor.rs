//! Control Source processor - pipeline entry point for manual control signal injection
//! 
//! This processor acts as a pipeline entry point that can receive external control signals
//! and inject them into the processing pipeline. It serves as the starting point for
//! pipelines that need external control capabilities.
//! 
//! Redesigned with single input channel architecture:
//! - Single input channel for receiving external control signals
//! - Multiple output channels for broadcasting control signals to downstream

use tokio::sync::broadcast;
use std::sync::Arc;
use crate::processor::{StreamProcessor, ProcessorView, utils, StreamData, ProcessorHandle};

/// ControlSource processor that serves as a pipeline entry point
/// 
/// This processor:
/// 1. Receives external control signals via a control channel
/// 2. Injects them into the processing pipeline
/// 3. Serves as the starting point for controlled pipelines
/// Now designed with single input, multiple output architecture.
pub struct ControlSourceProcessor {
    /// External control channel for receiving control signals
    control_receiver: broadcast::Receiver<StreamData>,
    /// Number of downstream processors this will broadcast to
    downstream_count: usize,
    /// Processor name for debugging
    processor_name: String,
}

impl ControlSourceProcessor {
    /// Create a new ControlSourceProcessor with control channel
    pub fn new(
        control_receiver: broadcast::Receiver<StreamData>,
        downstream_count: usize,
    ) -> Self {
        Self {
            control_receiver,
            downstream_count,
            processor_name: "ControlSourceProcessor".to_string(),
        }
    }
    
    /// Create a ControlSourceProcessor with a custom name
    pub fn with_name(
        name: String,
        control_receiver: broadcast::Receiver<StreamData>,
        downstream_count: usize,
    ) -> Self {
        Self {
            control_receiver,
            downstream_count,
            processor_name: name,
        }
    }
}

impl StreamProcessor for ControlSourceProcessor {
    fn start(&self, input_receiver: broadcast::Receiver<StreamData>) -> ProcessorView {
        // Create output channels for broadcasting to downstream
        let output_senders = utils::create_output_senders(self.downstream_count);
        
        // Clone data that will be used in the async task
        let processor_name = self.processor_name.clone();
        let downstream_count = self.downstream_count;
        let output_senders_clone = output_senders.clone();
        
        // Clone for the routine (Receiver supports resubscribe)
        let mut routine_input_receiver = input_receiver.resubscribe();
        
        // Simplified control source - just forward input signals
        let routine = async move {
            println!("{}: Starting control source routine for {} downstream processors", 
                     processor_name, downstream_count);
            
            // Send initial stream start signal to all outputs
            let start_signal = StreamData::stream_start();
            for sender in &output_senders_clone {
                if sender.send(start_signal.clone()).is_err() {
                    println!("{}: Failed to send start signal to some outputs", processor_name);
                }
            }
            
            // Main processing loop - forward input signals
            loop {
                match routine_input_receiver.recv().await {
                    Ok(input_signal) => {
                        println!("{}: Received input signal: {:?}", 
                                 processor_name, input_signal.description());
                        
                        // Check if this is a stop signal
                        if utils::is_stop_signal(&input_signal) {
                            println!("{}: Received stop signal, shutting down gracefully", processor_name);
                            // Forward to all outputs and exit
                            for sender in &output_senders_clone {
                                let _ = sender.send(input_signal.clone());
                            }
                            break;
                        }
                        
                        // Forward to all downstream
                        for sender in &output_senders_clone {
                            if sender.send(input_signal.clone()).is_err() {
                                println!("{}: Failed to forward input signal to some outputs", processor_name);
                            }
                        }
                    }
                    Err(e) => {
                        println!("{}: Input channel error: {}", processor_name, e);
                        // Send error signal to all outputs
                        let error_data = utils::handle_receive_error(e);
                        for sender in &output_senders_clone {
                            let _ = sender.send(error_data.clone());
                        }
                        break;
                    }
                }
            }
            
            // Send final stream end signal to all outputs
            let end_signal = StreamData::stream_end();
            for sender in &output_senders_clone {
                let _ = sender.send(end_signal.clone());
            }
            
            println!("{}: Control source routine completed", processor_name);
        };
        
        let join_handle = tokio::spawn(routine);
        
        // For ControlSourceProcessor, we create input sender for external control
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
impl ControlSourceProcessor {
    /// Create control source routine that runs in tokio task
    /// Now with single input, multiple output architecture
    pub fn create_control_source_routine(
        self: Arc<Self>,
        mut input_receiver: broadcast::Receiver<StreamData>,
        output_senders: Vec<broadcast::Sender<StreamData>>,
    ) -> impl std::future::Future<Output = ()> + Send + 'static {
        let downstream_count = self.downstream_count;
        let mut control_rx = self.control_receiver.resubscribe();
        let processor_name = self.processor_name.clone();
        
        async move {
            println!("{}: Starting control source routine for {} downstream processors", 
                     processor_name, downstream_count);
            
            // Send initial stream start signal to all outputs
            let start_signal = StreamData::stream_start();
            for sender in &output_senders {
                if sender.send(start_signal.clone()).is_err() {
                    println!("{}: Failed to send start signal to some outputs", processor_name);
                }
            }
            
            // Main processing loop - receive and forward external control signals
            loop {
                // Use select! to also monitor for stop signals from downstream
                tokio::select! {
                    // Receive external control signals
                    control_result = control_rx.recv() => {
                        match control_result {
                            Ok(control_signal) => {
                                println!("{}: Received external control signal: {:?}", 
                                         processor_name, control_signal.description());
                                
                                // Check if this is a stop signal
                                if utils::is_stop_signal(&control_signal) {
                                    println!("{}: Received stop signal, shutting down gracefully", processor_name);
                                    // Forward the stop signal to all outputs and exit
                                    for sender in &output_senders {
                                        let _ = sender.send(control_signal.clone());
                                    }
                                    break;
                                }
                                
                                // Forward control signal to all downstream
                                for sender in &output_senders {
                                    if sender.send(control_signal.clone()).is_err() {
                                        println!("{}: Failed to forward control signal to some outputs", processor_name);
                                    }
                                }
                            }
                            Err(e) => {
                                println!("{}: Control channel error: {}", processor_name, e);
                                // Send channel closed signal to all outputs
                                let end_signal = StreamData::stream_end();
                                for sender in &output_senders {
                                    let _ = sender.send(end_signal.clone());
                                }
                                break;
                            }
                        }
                    }
                    
                    // Monitor for input signals on the input channel
                    input_result = input_receiver.recv() => {
                        match input_result {
                            Ok(input_signal) => {
                                println!("{}: Received input signal: {:?}", 
                                         processor_name, input_signal.description());
                                
                                // Check if this is a stop signal
                                if utils::is_stop_signal(&input_signal) {
                                    println!("{}: Received stop signal via input, shutting down", processor_name);
                                    // Forward to all outputs and exit
                                    for sender in &output_senders {
                                        let _ = sender.send(input_signal.clone());
                                    }
                                    break;
                                }
                                
                                // Forward to all downstream
                                for sender in &output_senders {
                                    if sender.send(input_signal.clone()).is_err() {
                                        println!("{}: Failed to forward input signal to some outputs", processor_name);
                                    }
                                }
                            }
                            Err(e) => {
                                println!("{}: Input channel error: {}", processor_name, e);
                                // Send error signal to all outputs
                                let error_data = utils::handle_receive_error(e);
                                for sender in &output_senders {
                                    let _ = sender.send(error_data.clone());
                                }
                                break;
                            }
                        }
                    }
                    
                    // Monitor output channels for stop signals from downstream
                    // This allows graceful shutdown when downstream closes
                    output_check = async {
                        // Check one output channel (they all should be consistent)
                        if let Some(sender) = output_senders.first() {
                            let mut temp_rx = sender.subscribe();
                            // Use timeout to avoid blocking indefinitely
                            tokio::time::timeout(tokio::time::Duration::from_millis(100), temp_rx.recv()).await
                        } else {
                            // No outputs, just wait
                            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                            Ok(Err(broadcast::error::RecvError::Closed))
                        }
                    } => {
                        match output_check {
                            Ok(Ok(stop_signal)) => {
                                if utils::is_stop_signal(&stop_signal) {
                                    println!("{}: Detected stop signal in output channel, shutting down", processor_name);
                                    break;
                                }
                            }
                            Ok(Err(_)) | Err(_) => {
                                // No stop signal detected or timeout, continue normal operation
                            }
                        }
                    }
                }
            }
            
            // Send final stream end signal to all outputs
            let end_signal = StreamData::stream_end();
            for sender in &output_senders {
                let _ = sender.send(end_signal.clone());
            }
            
            println!("{}: Control source routine completed", processor_name);
        }
    }
}