//! DataSourceProcessor - processes data from PhysicalDatasource
//!
//! This processor reads data from a PhysicalDatasource and sends it downstream
//! as StreamData::Collection.

use crate::codec::RecordDecoder;
use crate::connector::{ConnectorEvent, SourceConnector};
use crate::processor::base::{broadcast_all, fan_in_streams};
use crate::processor::{Processor, ProcessorError, StreamData, StreamError};
use futures::stream::StreamExt;
use std::sync::Arc;
use tokio::sync::mpsc;

/// DataSourceProcessor - reads data from PhysicalDatasource
///
/// This processor:
/// - Takes a PhysicalDatasource as input
/// - Reads data from the source when triggered by control signals
/// - Sends data downstream as StreamData::Collection
pub struct DataSourceProcessor {
    /// Processor identifier
    source_name: String,
    /// Input channels for receiving control signals
    inputs: Vec<mpsc::Receiver<StreamData>>,
    /// Output channels for sending data downstream
    outputs: Vec<mpsc::Sender<StreamData>>,
    /// External source connectors that feed this processor
    connectors: Vec<ConnectorBinding>,
}

struct ConnectorBinding {
    connector: Box<dyn SourceConnector>,
    decoder: Arc<dyn RecordDecoder>,
}

impl DataSourceProcessor {
    /// Create a new DataSourceProcessor from PhysicalDatasource
    pub fn new(source_name: impl Into<String>) -> Self {
        Self {
            source_name: source_name.into(),
            inputs: Vec::new(),
            outputs: Vec::new(),
            connectors: Vec::new(),
        }
    }

    /// Register an external source connector and its decoder.
    pub fn add_connector(
        &mut self,
        connector: Box<dyn SourceConnector>,
        decoder: Arc<dyn RecordDecoder>,
    ) {
        self.connectors
            .push(ConnectorBinding { connector, decoder });
    }

    fn activate_connectors(&mut self) -> Vec<mpsc::Receiver<StreamData>> {
        let mut receivers = Vec::new();
        for binding in std::mem::take(&mut self.connectors) {
            let (sender, receiver) = mpsc::channel(100);
            Self::spawn_connector_task(
                binding.connector,
                binding.decoder,
                sender,
                self.source_name.clone(),
            );
            receivers.push(receiver);
        }
        receivers
    }

    fn spawn_connector_task(
        mut connector: Box<dyn SourceConnector>,
        decoder: Arc<dyn RecordDecoder>,
        sender: mpsc::Sender<StreamData>,
        processor_id: String,
    ) {
        tokio::spawn(async move {
            let mut stream = match connector.subscribe() {
                Ok(stream) => stream,
                Err(err) => {
                    let _ = sender
                        .send(StreamData::error(
                            StreamError::new(format!("connector subscribe error: {}", err))
                                .with_source(processor_id.clone()),
                        ))
                        .await;
                    return;
                }
            };

            while let Some(event) = stream.next().await {
                match event {
                    Ok(ConnectorEvent::Payload(bytes)) => match decoder.decode(&bytes) {
                        Ok(batch) => {
                            if sender
                                .send(StreamData::collection(Box::new(batch)))
                                .await
                                .is_err()
                            {
                                break;
                            }
                        }
                        Err(err) => {
                            let _ = sender
                                .send(StreamData::error(
                                    StreamError::new(format!("decode error: {}", err))
                                        .with_source(processor_id.clone()),
                                ))
                                .await;
                        }
                    },
                    Ok(ConnectorEvent::EndOfStream) => break,
                    Err(err) => {
                        let _ = sender
                            .send(StreamData::error(
                                StreamError::new(format!("connector error: {}", err))
                                    .with_source(processor_id.clone()),
                            ))
                            .await;
                    }
                }
            }
        });
    }
}

impl Processor for DataSourceProcessor {
    fn id(&self) -> &str {
        &self.source_name
    }

    fn start(&mut self) -> tokio::task::JoinHandle<Result<(), ProcessorError>> {
        let outputs = self.outputs.clone();
        let mut base_inputs = std::mem::take(&mut self.inputs);
        base_inputs.extend(self.activate_connectors());
        let mut input_streams = fan_in_streams(base_inputs);

        tokio::spawn(async move {
            while let Some(data) = input_streams.next().await {
                match data.as_control() {
                    Some(crate::processor::ControlSignal::StreamEnd) => {
                        broadcast_all(&outputs, data.clone()).await?;
                        return Ok(());
                    }
                    Some(_) => {
                        broadcast_all(&outputs, data.clone()).await?;
                    }
                    None => {
                        broadcast_all(&outputs, data.clone()).await?;
                    }
                }
            }

            Ok(())
        })
    }

    fn output_senders(&self) -> Vec<mpsc::Sender<StreamData>> {
        self.outputs.clone()
    }

    fn add_input(&mut self, receiver: mpsc::Receiver<StreamData>) {
        self.inputs.push(receiver);
    }

    fn add_output(&mut self, sender: mpsc::Sender<StreamData>) {
        self.outputs.push(sender);
    }
}
