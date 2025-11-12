//! DataSource processor - corresponds to PhysicalDataSource
//! 
//! This processor acts as a source of data in the stream processing pipeline.
//! Currently generates empty data to establish the data flow pipeline.
//! 
//! Redesigned with single input channel architecture:
//! - Single input channel (can receive control signals)
//! - Multiple output channels for broadcasting to downstream

use tokio::sync::broadcast;
use std::sync::Arc;
use crate::planner::physical::PhysicalPlan;
use crate::processor::{StreamProcessor, ProcessorView, ProcessorHandle, utils, StreamData};

/// DataSource processor that corresponds to PhysicalDataSource
/// 
/// This is typically the starting point of a stream processing pipeline.
/// Now designed with single input, multiple output architecture.
pub struct DataSourceProcessor {
    /// The physical plan this processor corresponds to
    physical_plan: Arc<dyn PhysicalPlan>,
    /// Input channel - can receive control signals from upstream or external sources
    input_channel: (broadcast::Sender<StreamData>, broadcast::Receiver<StreamData>),
    /// Output channels - broadcast processed data to downstream processors
    output_channels: Vec<(broadcast::Sender<StreamData>, broadcast::Receiver<StreamData>)>,
    /// Number of downstream processors this will broadcast to
    downstream_count: usize,
}

impl DataSourceProcessor {
    /// Create a new DataSourceProcessor with specified downstream count
    pub fn new(
        physical_plan: Arc<dyn PhysicalPlan>,
        downstream_count: usize,
    ) -> Self {
        // Create input channel (mainly for control signals)
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
    
    /// Generate initial data (currently empty for pipeline establishment)
    fn generate_data(&self) -> Vec<Box<dyn crate::model::Collection>> {
        // For now, return empty data to establish pipeline
        // In a real implementation, this would:
        // 1. Connect to the actual data source (Kafka, file, etc.)
        // 2. Read and parse data
        // 3. Return data items
        vec![]
    }
    
    /// Create data source routine that runs in tokio task
    /// Now with single input, multiple output architecture
    pub fn create_datasource_routine(
        self: Arc<Self>,
        mut input_receiver: broadcast::Receiver<StreamData>,
        output_senders: Vec<broadcast::Sender<StreamData>>,
    ) -> impl std::future::Future<Output = ()> + Send + 'static {
        let processor_name = "DataSourceProcessor".to_string();
        let downstream_count = self.downstream_count;
        
        // Clone data generation method
        let data_generator = self.clone();
        
        async move {
            println!("{}: Starting data source routine for {} downstream processors", 
                     processor_name, downstream_count);
            
            // Send initial stream start signal to all outputs
            let start_signal = StreamData::stream_start();
            for sender in &output_senders {
                if sender.send(start_signal.clone()).is_err() {
                    println!("{}: Failed to send start signal to some outputs", processor_name);
                }
            }
            
            // Main processing loop
            loop {
                // Listen for control signals on input channel
                match input_receiver.recv().await {
                    Ok(stream_data) => {
                        println!("{}: Received input: {:?}", processor_name, stream_data.description());
                        
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
                        
                        // For data signals, generate actual data (currently empty)
                        if stream_data.is_data() {
                            // In a real implementation, this would generate data based on the signal
                            let data_items = data_generator.generate_data();
                            
                            for data in data_items {
                                let data_signal = StreamData::collection(data);
                                // Broadcast to all outputs
                                for sender in &output_senders {
                                    if sender.send(data_signal.clone()).is_err() {
                                        println!("{}: Failed to send data to some outputs", processor_name);
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
            
            // Send final stream end signal to all outputs
            let end_signal = StreamData::stream_end();
            for sender in &output_senders {
                let _ = sender.send(end_signal.clone());
            }
            
            println!("{}: Data source routine completed", processor_name);
        }
    }
}

impl StreamProcessor for DataSourceProcessor {
    fn start(&self, input_receiver: broadcast::Receiver<StreamData>) -> ProcessorView {
        // Create output channels for broadcasting to downstream
        let output_senders: Vec<broadcast::Sender<StreamData>> = utils::create_output_senders(self.downstream_count);
        
        // Clone data that will be used in the async task
        let processor_name = "DataSourceProcessor".to_string();
        let downstream_count = self.downstream_count;
        let output_senders_clone = output_senders.clone();
        
        // Clone for the routine (Receiver supports resubscribe)
        let mut routine_input_receiver = input_receiver.resubscribe();
        
        // Spawn the data source routine
        let routine = async move {
            println!("{}: Starting data source routine for {} downstream processors", 
                     processor_name, downstream_count);
            
            // Send initial stream start signal to all outputs
            let start_signal = StreamData::stream_start();
            for sender in &output_senders_clone {
                if sender.send(start_signal.clone()).is_err() {
                    println!("{}: Failed to send start signal to some outputs", processor_name);
                }
            }
            
            // Main processing loop
            loop {
                // Listen for control signals on input channel
                match routine_input_receiver.recv().await {
                    Ok(stream_data) => {
                        println!("{}: Received input: {:?}", processor_name, stream_data.description());
                        
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
                        
                        // For data signals, generate actual data (currently empty)
                        if stream_data.is_data() {
                            // In a real implementation, this would generate data based on the signal
                            // For now, just forward the data signal as-is since we don't have real data generation
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
            
            // Send final stream end signal to all outputs
            let end_signal = StreamData::stream_end();
            for sender in &output_senders_clone {
                let _ = sender.send(end_signal.clone());
            }
            
            println!("{}: Data source routine completed", processor_name);
        };
        
        let join_handle = tokio::spawn(routine);
        
        // For DataSourceProcessor, we create input sender for control signals
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
