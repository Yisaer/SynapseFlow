//! Subscription source abstraction for data ingestion
//!
//! This module defines the SubscriptionSource trait for abstracting different
//! data sources that can subscribe to and receive data as bytes.

use async_trait::async_trait;
use tokio::sync::mpsc;
use crate::processor::ProcessorError;

/// Trait for subscription sources that can receive data as bytes
///
/// Subscription sources are responsible for subscribing to external data sources
/// (e.g., MQTT, Kafka, etc.) and providing received data as bytes through a channel.
#[async_trait]
pub trait SubscriptionSource: Send + Sync {
    /// Start subscribing and return a receiver channel for incoming data
    ///
    /// This method should start the subscription process and return a channel
    /// receiver that will receive bytes as they arrive from the source.
    ///
    /// # Returns
    /// A Result containing an mpsc::Receiver<Vec<u8>> that will receive data,
    /// or a ProcessorError if subscription fails
    async fn subscribe(&mut self) -> Result<mpsc::Receiver<Vec<u8>>, ProcessorError>;
    
    /// Get a human-readable name for this subscription source
    fn name(&self) -> &str;
    
    /// Stop the subscription
    ///
    /// This method should gracefully stop the subscription and clean up resources.
    async fn stop(&mut self) -> Result<(), ProcessorError>;
}
