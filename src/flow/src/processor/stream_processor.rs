//! Stream processor trait and common utilities
//! 
//! Defines the core StreamProcessor interface and shared utilities for all processors.
//! 
//! Redesigned with single input channel architecture:
//! - Each processor has exactly ONE input channel
//! - Each processor can have MULTIPLE output channels
//! - Simplifies processor logic and improves performance

use tokio::sync::broadcast;
use crate::processor::stream_data::{StreamData, ControlSignal, StreamError};
use crate::processor::ProcessorView;

/// Core trait for all stream processors
/// 
/// New architecture:
/// 1. Each processor has exactly ONE input channel
/// 2. Processors broadcast results to multiple output channels
/// 3. Control signals flow through the same channel as data
/// 4. Simplified processor logic without complex select!
pub trait StreamProcessor: Send + Sync {
    /// Start the processor and return a view for control and output
    /// 
    /// The processor will:
    /// 1. Receive data from its single input channel
    /// 2. Process the data according to its logic
    /// 3. Broadcast results to multiple output channels
    /// 4. Handle control signals for graceful shutdown
    /// 
    /// # Arguments
    /// * `input_receiver` - The input channel receiver that this processor will listen to
    fn start(&self, input_receiver: broadcast::Receiver<StreamData>) -> ProcessorView;
    
    /// Get the number of downstream processors this will broadcast to
    /// Used for channel capacity planning
    fn downstream_count(&self) -> usize;
    
    /// Create an input channel for this processor (for control signals)
    fn create_input_channel(&self) -> (broadcast::Sender<StreamData>, broadcast::Receiver<StreamData>);
}

/// Common utilities for stream processors
pub mod utils {
    use super::*;
    
    /// Create a broadcast channel with appropriate capacity based on downstream count
    pub fn create_channel(downstream_count: usize) -> (broadcast::Sender<StreamData>, broadcast::Receiver<StreamData>) {
        // Base capacity + additional capacity per downstream
        let base_capacity = 1024;
        let additional_capacity = downstream_count * 256;
        let total_capacity = base_capacity + additional_capacity;
        
        broadcast::channel(total_capacity)
    }
    
    /// Create an input channel for a processor
    /// 
    /// This is the channel that the processor will listen to for incoming data
    pub fn create_input_channel() -> (broadcast::Sender<StreamData>, broadcast::Receiver<StreamData>) {
        // Use a reasonable base capacity for input channels
        broadcast::channel(1024)
    }
    
    /// Create output channels for broadcasting to downstream processors
    /// 
    /// Returns only the senders (receivers are created but not returned)
    pub fn create_output_senders(count: usize) -> Vec<broadcast::Sender<StreamData>> {
        (0..count).map(|i| {
            let (sender, _) = create_channel(count);
            sender
        }).collect()
    }
    
    /// Create output channels for a processor (both senders and receivers)
    /// 
    /// Returns tuples of (sender, receiver) for each output channel
    pub fn create_output_channels(count: usize) -> Vec<(broadcast::Sender<StreamData>, broadcast::Receiver<StreamData>)> {
        (0..count).map(|_| create_channel(count)).collect()
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
    
    /// Check if a StreamData item is a stop/termination signal
    pub fn is_stop_signal(stream_data: &StreamData) -> bool {
        matches!(stream_data, StreamData::Control(ControlSignal::StreamEnd))
    }
}