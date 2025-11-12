//! DataSource processor - corresponds to PhysicalDataSource
//! 
//! This processor acts as a source of data in the stream processing pipeline.
//! Currently generates empty data to establish the data flow pipeline.
//! 
//! Design follows rstream's Executor pattern with tokio::spawn and dedicated data generation routine.

use tokio::sync::broadcast;
use std::sync::Arc;
use crate::planner::physical::PhysicalPlan;
use crate::processor::{StreamProcessor, ProcessorView, ProcessorHandle, utils, StreamData, StreamError};

/// DataSource processor that corresponds to PhysicalDataSource
/// 
/// This is typically the starting point of a stream processing pipeline.
/// Currently just generates empty data to establish data flow.
pub struct DataSourceProcessor {
    /// The physical plan this processor corresponds to
    physical_plan: Arc<dyn PhysicalPlan>,
    /// Input channels from children (upstream processors)
    /// For DataSource, this is typically empty, but we support it for flexibility
    input_receivers: Vec<broadcast::Receiver<Result<StreamData, String>>>,
    /// Number of downstream processors this will broadcast to
    downstream_count: usize,
}

impl DataSourceProcessor {
    /// Create a new DataSourceProcessor with specified downstream count and upstream receivers
    pub fn new(
        physical_plan: Arc<dyn PhysicalPlan>,
        input_receivers: Vec<broadcast::Receiver<Result<StreamData, String>>>,
        downstream_count: usize,
    ) -> Self {
        Self {
            physical_plan,
            input_receivers,
            downstream_count,
        }
    }
    
    /// Generate initial data (currently empty for pipeline establishment)
    async fn generate_initial_data(&self) -> Result<Vec<Box<dyn crate::model::Collection>>, String> {
        // For now, return empty data to establish pipeline
        // In a real implementation, this would:
        // 1. Connect to the actual data source (Kafka, file, etc.)
        // 2. Read and parse data
        // 3. Return StreamData items
        Ok(vec![])
    }
}

impl StreamProcessor for DataSourceProcessor {
    fn start(&self) -> ProcessorView {
        // Create channels using utils
        let (result_tx, result_rx) = utils::create_result_channel(self.downstream_count);
        let (stop_tx, stop_rx) = utils::create_stop_channel();
        
        // Spawn the data source routine - currently just generates empty data
        let routine = self.create_datasource_routine(result_tx, stop_rx);
        
        let join_handle = tokio::spawn(routine);
        
        ProcessorView::new(
            result_rx,
            stop_tx,
            ProcessorHandle::new(join_handle),
        )
    }
    
    fn get_physical_plan(&self) -> &Arc<dyn PhysicalPlan> {
        &self.physical_plan
    }
    
    fn downstream_count(&self) -> usize {
        self.downstream_count
    }
    
    fn input_receivers(&self) -> Vec<broadcast::Receiver<Result<StreamData, String>>> {
        self.input_receivers.iter()
            .map(|rx| rx.resubscribe())
            .collect()
    }
}

// Private helper methods
impl DataSourceProcessor {
    /// Create data source routine that runs in tokio task
    /// Now handles both data generation and upstream input processing
    fn create_datasource_routine(
        &self,
        result_tx: broadcast::Sender<Result<StreamData, String>>,
        mut stop_rx: broadcast::Receiver<()>,
    ) -> impl std::future::Future<Output = ()> + Send + 'static {
        let downstream_count = self.downstream_count;
        let processor_name = "DataSourceProcessor".to_string();
        let mut input_receivers = self.input_receivers();
        
        async move {
            println!("DataSourceProcessor: Starting data source routine for {} downstream processors with {} upstream inputs", 
                     downstream_count, input_receivers.len());
            
            // Send stream start signal
            if result_tx.send(Ok(StreamData::stream_start())).is_err() {
                println!("DataSourceProcessor: Failed to send start signal");
                return;
            }
            
            // If we have upstream inputs, process them (for future extensions)
            // For now, DataSource typically doesn't have upstream, but we support it
            if !input_receivers.is_empty() {
                println!("DataSourceProcessor: Processing upstream inputs (unexpected for data source)");
                // Process upstream data - this is unusual for a data source but supports flexibility
                for receiver in &mut input_receivers {
                    loop {
                        tokio::select! {
                            _ = stop_rx.recv() => {
                                println!("DataSourceProcessor: Received stop signal while processing upstream");
                                break;
                            }
                            result = receiver.recv() => {
                                match result {
                                    Ok(Ok(stream_data)) => {
                                        // Pass through upstream data (unusual for data source)
                                        if result_tx.send(Ok(stream_data)).is_err() {
                                            println!("DataSourceProcessor: All downstream receivers dropped");
                                            return;
                                        }
                                    }
                                    Ok(Err(e)) => {
                                        // Forward upstream error
                                        let stream_error = StreamError::new(e)
                                            .with_source("upstream")
                                            .with_timestamp(std::time::SystemTime::now());
                                        if result_tx.send(Ok(StreamData::error(stream_error))).is_err() {
                                            return;
                                        }
                                    }
                                    Err(e) => {
                                        // Handle broadcast errors
                                        let error_data = utils::handle_receive_error(e);
                                        if result_tx.send(error_data).is_err() {
                                            return;
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            } else {
                // Traditional data source behavior - generate data
                match Self::static_generate_initial_data().await {
                    Ok(data_items) => {
                        for data in data_items {
                            // Check stop signal before sending each item
                            if stop_rx.try_recv().is_ok() {
                                println!("DataSourceProcessor: Received stop signal, shutting down");
                                break;
                            }
                            
                            if result_tx.send(Ok(StreamData::collection(data))).is_err() {
                                // All downstream receivers dropped, stop processing
                                println!("DataSourceProcessor: All downstream receivers dropped, stopping");
                                break;
                            }
                        }
                    }
                    Err(e) => {
                        println!("DataSourceProcessor: Error generating data: {}", e);
                        // Send error as StreamData::Error instead of stopping the flow
                        let stream_error = StreamError::new(e)
                            .with_source(&processor_name)
                            .with_timestamp(std::time::SystemTime::now());
                        
                        if result_tx.send(Ok(StreamData::error(stream_error))).is_err() {
                            println!("DataSourceProcessor: Failed to send error to downstream");
                        }
                    }
                }
            }
            
            // Send stream end signal
            if result_tx.send(Ok(StreamData::stream_end())).is_err() {
                println!("DataSourceProcessor: Failed to send end signal");
            }
            
            println!("DataSourceProcessor: Data source routine completed");
        }
    }
    
    /// Static version of generate_initial_data for use in async routine
    async fn static_generate_initial_data() -> Result<Vec<Box<dyn crate::model::Collection>>, String> {
        // For now, return empty data to establish pipeline
        // In a real implementation, this would generate actual data
        Ok(vec![])
    }
}