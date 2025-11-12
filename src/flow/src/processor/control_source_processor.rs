//! Control Source processor - pipeline entry point for manual control signal injection
//! 
//! This processor acts as a pipeline entry point that can receive external control signals
//! and inject them into the processing pipeline. It serves as the starting point for
//! pipelines that need external control capabilities.

use tokio::sync::broadcast;
use crate::processor::{StreamProcessor, ProcessorView, ProcessorHandle, utils, StreamData};

/// ControlSource processor that serves as a pipeline entry point
/// 
/// This processor:
/// 1. Receives external control signals via a control channel
/// 2. Injects them into the processing pipeline
/// 3. Serves as the starting point for controlled pipelines
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
    fn start(&self) -> ProcessorView {
        // Create channels using utils
        let (result_tx, result_rx) = utils::create_result_channel(self.downstream_count);
        let (stop_tx, stop_rx) = utils::create_stop_channel();
        
        // Spawn the control source routine
        let routine = self.create_control_source_routine(result_tx, stop_rx);
        
        let join_handle = tokio::spawn(routine);
        
        ProcessorView::new(
            result_rx,
            stop_tx,
            ProcessorHandle::new(join_handle),
        )
    }
    
    fn downstream_count(&self) -> usize {
        self.downstream_count
    }
    
    fn input_receivers(&self) -> Vec<broadcast::Receiver<StreamData>> {
        // ControlSourceProcessor has no upstream inputs - it's a pipeline entry point
        Vec::new()
    }
}

// Private helper methods
impl ControlSourceProcessor {
    /// Create control source routine that runs in tokio task
    fn create_control_source_routine(
        &self,
        result_tx: broadcast::Sender<StreamData>,
        mut stop_rx: broadcast::Receiver<()>,
    ) -> impl std::future::Future<Output = ()> + Send + 'static {
        let downstream_count = self.downstream_count;
        let mut control_rx = self.control_receiver.resubscribe();
        let processor_name = self.processor_name.clone();
        
        async move {
            println!("{}: Starting control source routine for {} downstream processors", 
                     processor_name, downstream_count);
            
            // Send initial stream start signal
            if result_tx.send(StreamData::stream_start()).is_err() {
                println!("{}: Failed to send start signal", processor_name);
                return;
            }
            
            // Main processing loop - receive and forward external control signals
            loop {
                tokio::select! {
                    // Check stop signal
                    _ = stop_rx.recv() => {
                        println!("{}: Received stop signal, shutting down", processor_name);
                        break;
                    }
                    
                    // Receive external control signals
                    control_result = control_rx.recv() => {
                        match control_result {
                            Ok(control_signal) => {
                                println!("{}: Received external control signal: {:?}", 
                                         processor_name, control_signal.description());
                                
                                // Forward control signal to downstream
                                if result_tx.send(control_signal).is_err() {
                                    println!("{}: All downstream receivers dropped, stopping", processor_name);
                                    break;
                                }
                            }
                            Err(e) => {
                                println!("{}: Control channel error: {}", processor_name, e);
                                // Send channel closed signal
                                if result_tx.send(StreamData::control(crate::processor::stream_data::ControlSignal::StreamEnd)).is_err() {
                                    break;
                                }
                            }
                        }
                    }
                }
            }
            
            // Send stream end signal
            if result_tx.send(StreamData::stream_end()).is_err() {
                println!("{}: Failed to send end signal", processor_name);
            }
            
            println!("{}: Control source routine completed", processor_name);
        }
    }
}