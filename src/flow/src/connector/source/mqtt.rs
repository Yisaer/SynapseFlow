//! MQTT connector mock implementation.
//!
//! The connector exposes an async stream built on top of an mpsc receiver so
//! the processing pipeline can treat MQTT payloads just like any other input.

use futures::StreamExt;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

use crate::connector::{ConnectorError, ConnectorEvent, ConnectorStream, SourceConnector};

/// Basic MQTT configuration.
#[derive(Debug, Clone)]
pub struct MqttSourceConfig {
    /// Logical identifier for the upstream MQTT source.
    pub source_name: String,
    /// Broker endpoint (e.g., `mqtt://localhost:1883`).
    pub broker_url: String,
    /// Topic to subscribe to.
    pub topic: String,
    /// Requested QoS level.
    pub qos: u8,
}

impl MqttSourceConfig {
    pub fn new(
        source_name: impl Into<String>,
        broker_url: impl Into<String>,
        topic: impl Into<String>,
        qos: u8,
    ) -> Self {
        Self {
            source_name: source_name.into(),
            broker_url: broker_url.into(),
            topic: topic.into(),
            qos,
        }
    }
}

/// Connector that consumes payloads from an mpsc receiver.
///
/// A production implementation would wire this to a real MQTT client, but this
/// structure keeps the same async-stream interface for the processor layer.
pub struct MqttSourceConnector {
    id: String,
    config: MqttSourceConfig,
    payload_rx: Option<mpsc::Receiver<Vec<u8>>>,
}

impl MqttSourceConnector {
    /// Create a connector backed by an existing mpsc receiver.
    pub fn from_channel(
        id: impl Into<String>,
        config: MqttSourceConfig,
        payload_rx: mpsc::Receiver<Vec<u8>>,
    ) -> Self {
        Self {
            id: id.into(),
            config,
            payload_rx: Some(payload_rx),
        }
    }

    /// Borrow the configuration.
    pub fn config(&self) -> &MqttSourceConfig {
        &self.config
    }
}

impl SourceConnector for MqttSourceConnector {
    fn id(&self) -> &str {
        &self.id
    }

    fn subscribe(&mut self) -> Result<ConnectorStream, ConnectorError> {
        let receiver = self
            .payload_rx
            .take()
            .ok_or_else(|| ConnectorError::AlreadySubscribed(self.id.clone()))?;

        let stream = ReceiverStream::new(receiver).map(|payload| {
            if payload.is_empty() {
                Ok(ConnectorEvent::EndOfStream)
            } else {
                Ok(ConnectorEvent::Payload(payload))
            }
        });

        Ok(Box::pin(stream))
    }
}
