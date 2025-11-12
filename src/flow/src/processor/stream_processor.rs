//! Stream processor trait and common utilities
//! 
//! Defines the core StreamProcessor interface and shared utilities for all processors.

use tokio::sync::broadcast;
use crate::processor::stream_data::{StreamData, ControlSignal, StreamError};
use crate::processor::ProcessorView;

/// Core trait for all stream processors
/// 
/// Processors run in their own tokio task and handle data flow through broadcast channels.
/// They can receive data from multiple upstream sources and broadcast to multiple downstream sinks.
/// 
/// Design pattern:
/// 1. Constructor takes configuration and input receivers
/// 2. `start()` method spawns tokio task and returns ProcessorView
/// 3. Each processor implements data processing routine with tokio::select!
/// 4. Supports graceful shutdown via broadcast channels
/// 5. Handles multiple input sources and output broadcasting
/// 
/// Note: This trait is designed to work with both PhysicalPlan-based processors
/// and special-purpose processors like ControlSourceProcessor and ResultSinkProcessor
pub trait StreamProcessor: Send + Sync {
    /// Start the processor and return a view for control and output
    /// 
    /// This method should:
    /// 1. Create necessary channels (stop, result)
    /// 2. Spawn a tokio task for the processor routine
    /// 3. Return a ProcessorView for external control and data consumption
    fn start(&self) -> ProcessorView;
    
    /// Get the number of downstream processors this will broadcast to
    /// This is determined at construction time and used for channel capacity planning
    fn downstream_count(&self) -> usize;
    
    /// Get input channels from upstream processors
    fn input_receivers(&self) -> Vec<broadcast::Receiver<StreamData>>;
}

/// Common utilities for stream processors
pub mod utils {
    use super::*;
    
    /// Create a broadcast channel with appropriate capacity based on downstream count
    pub fn create_result_channel(downstream_count: usize) -> (broadcast::Sender<StreamData>, broadcast::Receiver<StreamData>) {
        // Base capacity + additional capacity per downstream
        let base_capacity = 1024;
        let additional_capacity = downstream_count * 256;
        let total_capacity = base_capacity + additional_capacity;
        
        broadcast::channel(total_capacity)
    }
    
    /// Create a stop channel for graceful shutdown
    pub fn create_stop_channel() -> (broadcast::Sender<()>, broadcast::Receiver<()>) {
        broadcast::channel(1)
    }
    
    /// Handle broadcast receive errors by converting them to appropriate StreamData
    pub fn handle_receive_error(error: broadcast::error::RecvError) -> StreamData {
        match error {
            broadcast::error::RecvError::Lagged(_) => {
                StreamData::control(ControlSignal::Backpressure)
            }
            broadcast::error::RecvError::Closed => {
                StreamData::control(ControlSignal::StreamEnd)
            }
        }
    }
    
    /// Create a stream error with processor source information
    pub fn create_stream_error(message: impl Into<String>, processor_name: &str) -> StreamData {
        let error = StreamError::new(message)
            .with_source(processor_name)
            .with_timestamp(std::time::SystemTime::now());
        StreamData::error(error)
    }
}