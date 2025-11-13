//! Mock connector that lets tests or scripts push bytes into the pipeline manually.

use futures::{stream, StreamExt};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

use crate::connector::{ConnectorError, ConnectorEvent, ConnectorStream, SourceConnector};

/// Connector exposing a handle for manually injecting payloads.
pub struct MockSourceConnector {
    id: String,
    payload_rx: Option<mpsc::Receiver<Vec<u8>>>,
}

/// Handle used to push payloads into a [`MockSourceConnector`].
pub struct MockSourceHandle {
    sender: mpsc::Sender<Vec<u8>>,
}

impl MockSourceConnector {
    /// Create a new mock connector along with the handle used for sending data.
    pub fn new(id: impl Into<String>) -> (Self, MockSourceHandle) {
        let (sender, receiver) = mpsc::channel(100);
        (
            Self {
                id: id.into(),
                payload_rx: Some(receiver),
            },
            MockSourceHandle { sender },
        )
    }
}

impl SourceConnector for MockSourceConnector {
    fn id(&self) -> &str {
        &self.id
    }

    fn subscribe(&mut self) -> Result<ConnectorStream, ConnectorError> {
        let receiver = self
            .payload_rx
            .take()
            .ok_or_else(|| ConnectorError::AlreadySubscribed(self.id.clone()))?;

        let payload_stream =
            ReceiverStream::new(receiver).map(|payload| Ok(ConnectorEvent::Payload(payload)));

        // Append a single EndOfStream event once the sender side closes.
        let stream = payload_stream.chain(stream::once(async { Ok(ConnectorEvent::EndOfStream) }));

        Ok(Box::pin(stream))
    }
}

impl MockSourceHandle {
    /// Send a payload to every subscriber of the mock connector.
    pub async fn send(&self, payload: impl Into<Vec<u8>>) -> Result<(), MockSourceError> {
        self.sender
            .send(payload.into())
            .await
            .map_err(|_| MockSourceError::Closed)
    }

    /// Borrow a clone of the underlying sender for advanced scenarios.
    pub fn sender(&self) -> mpsc::Sender<Vec<u8>> {
        self.sender.clone()
    }
}

/// Errors returned by [`MockSourceHandle`].
#[derive(thiserror::Error, Debug)]
pub enum MockSourceError {
    /// All subscribers dropped, no further payloads can be delivered.
    #[error("mock source closed")]
    Closed,
}
