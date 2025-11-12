//! Result Sink processor - pipeline exit point for collecting processed data
//! 
//! This processor acts as a pipeline exit point that collects final processed data
//! and forwards it to external consumers via a result channel.

use tokio::sync::broadcast;
use crate::processor::{StreamProcessor, ProcessorView, ProcessorHandle, utils, StreamData};

/// ResultSink processor that serves as a pipeline exit point
/// 
/// This processor:
/// 1. Receives processed data from upstream processors
/// 2. Forwards results to external consumers via a result channel
/// 3. Handles pipeline completion and cleanup
pub struct ResultSinkProcessor {
    /// Input channels from upstream processors
    input_receivers: Vec<broadcast::Receiver<StreamData>>,
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
        input_receivers: Vec<broadcast::Receiver<StreamData>>,
        result_sender: broadcast::Sender<StreamData>,
        downstream_count: usize,
    ) -> Self {
        Self {
            input_receivers,
            result_sender,
            downstream_count,
            processor_name: "ResultSinkProcessor".to_string(),
        }
    }
    
    /// Create a ResultSinkProcessor with single upstream input
    pub fn with_single_upstream(
        upstream_receiver: broadcast::Receiver<StreamData>,
        result_sender: broadcast::Sender<StreamData>,
    ) -> Self {
        Self::new(vec![upstream_receiver], result_sender, 0)
    }
    
    /// Create a ResultSinkProcessor with a name for debugging
    pub fn with_name(
        name: String,
        input_receivers: Vec<broadcast::Receiver<StreamData>>,
        result_sender: broadcast::Sender<StreamData>,
        downstream_count: usize,
    ) -> Self {
        Self {
            input_receivers,
            result_sender,
            downstream_count,
            processor_name: name,
        }
    }
}

impl StreamProcessor for ResultSinkProcessor {
    fn start(&self) -> ProcessorView {
        // Create channels using utils
        let (result_tx, result_rx) = utils::create_result_channel(self.downstream_count);
        let (stop_tx, stop_rx) = utils::create_stop_channel();
        
        // Spawn the result sink routine
        let routine = self.create_result_sink_routine(result_tx, stop_rx);
        
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
        self.input_receivers.iter()
            .map(|rx| rx.resubscribe())
            .collect()
    }
}

// Private helper methods
impl ResultSinkProcessor {
    /// Create result sink routine that runs in tokio task
    fn create_result_sink_routine(
        &self,
        _result_tx: broadcast::Sender<StreamData>, // Internal result channel (not used for external forwarding)
        mut stop_rx: broadcast::Receiver<()>,
    ) -> impl std::future::Future<Output = ()> + Send + 'static {
        let mut input_receivers = self.input_receivers();
        let result_sender = self.result_sender.clone();
        let processor_name = self.processor_name.clone();
        
        async move {
            println!("{}: Starting result sink routine with {} upstream inputs", processor_name, input_receivers.len());
            
            // Main collection loop
            loop {
                tokio::select! {
                    // Check stop signal
                    _ = stop_rx.recv() => {
                        println!("{}: Received stop signal, shutting down", processor_name);
                        
                        // Send final end signal to external consumers
                        let _ = result_sender.send(StreamData::stream_end());
                        break;
                    }
                    
                    // Receive processed data from upstream
                    upstream_result = async {
                        if let Some(receiver) = input_receivers.get_mut(0) {
                            receiver.recv().await
                        } else {
                            // No upstream input, wait indefinitely
                            futures::future::pending::<Result<StreamData, broadcast::error::RecvError>>().await
                        }
                    } => {
                        match upstream_result {
                            Ok(stream_data) => {
                                println!("{}: Received processed data: {:?}", processor_name, stream_data.description());
                                
                                // Check if this is a terminal signal before forwarding
                                let is_terminal = stream_data.is_terminal();
                                
                                // Forward to external consumers (clone to avoid move)
                                if result_sender.send(stream_data.clone()).is_err() {
                                    println!("{}: No external consumers remaining, continuing to collect", processor_name);
                                    // Continue collecting even if no external consumers
                                    // This prevents data loss in the pipeline
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
                                let _ = result_sender.send(error_data);
                            }
                        }
                    }
                }
            }
            
            println!("{}: Result sink routine completed", processor_name);
        }
    }
}