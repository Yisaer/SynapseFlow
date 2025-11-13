//! Decoder abstraction for converting bytes to RecordBatch
//!
//! This module defines the Decoder trait for converting raw bytes received
//! from subscription sources into RecordBatch format.

use async_trait::async_trait;
use crate::model::RecordBatch;
use crate::processor::ProcessorError;

/// Trait for decoders that convert bytes to RecordBatch
///
/// Decoders are responsible for parsing raw bytes received from subscription
/// sources and converting them into RecordBatch format that can be processed
/// by the stream processing pipeline.
#[async_trait]
pub trait Decoder: Send + Sync {
    /// Decode bytes into a RecordBatch
    ///
    /// This method should parse the provided bytes and convert them into
    /// a RecordBatch. The bytes may represent a single message or a batch
    /// of messages depending on the source format.
    ///
    /// # Arguments
    /// * `bytes` - The raw bytes to decode
    ///
    /// # Returns
    /// A Result containing the decoded RecordBatch, or a ProcessorError if decoding fails
    async fn decode(&self, bytes: &[u8]) -> Result<RecordBatch, ProcessorError>;
    
    /// Get a human-readable name for this decoder
    fn name(&self) -> &str;
}
