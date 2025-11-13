//! DataSourceProcessor - processes data from PhysicalDatasource
//!
//! This processor reads data from a PhysicalDatasource and sends it downstream
//! as StreamData::Collection. It supports both traditional control-signal-based
//! data sources and subscription-based sources (e.g., MQTT) with decoders.

use tokio::sync::mpsc;
use std::sync::Arc;
use crate::processor::{Processor, ProcessorError, StreamData};
use crate::connector::SubscriptionSource;
use crate::codec::Decoder;

/// DataSourceProcessor - reads data from PhysicalDatasource
///
/// This processor:
/// - Takes a PhysicalDatasource as input
/// - Reads data from the source when triggered by control signals
/// - Or subscribes to a subscription source (e.g., MQTT) and decodes bytes to RecordBatch
/// - Sends data downstream as StreamData::Collection
pub struct DataSourceProcessor {
    /// Processor identifier
    source_name: String,
    /// Input channels for receiving control signals
    inputs: Vec<mpsc::Receiver<StreamData>>,
    /// Output channels for sending data downstream
    outputs: Vec<mpsc::Sender<StreamData>>,
    /// Optional subscription source for subscribing to external data sources
    subscription_source: Option<Box<dyn SubscriptionSource>>,
    /// Optional decoder for converting bytes to RecordBatch
    decoder: Option<Arc<dyn Decoder>>,
}

impl DataSourceProcessor {
    /// Create a new DataSourceProcessor from PhysicalDatasource
    pub fn new(
        source_name: impl Into<String>,
    ) -> Self {
        Self {
            source_name: source_name.into(),
            inputs: Vec::new(),
            outputs: Vec::new(),
            subscription_source: None,
            decoder: None,
        }
    }
    
    /// Create a new DataSourceProcessor with subscription source and decoder
    ///
    /// This constructor is used when the processor should subscribe to an external
    /// data source (e.g., MQTT) and decode the received bytes into RecordBatch.
    ///
    /// # Arguments
    /// * `source_name` - Identifier for this processor
    /// * `subscription_source` - The subscription source to subscribe to
    /// * `decoder` - The decoder to convert bytes to RecordBatch
    pub fn with_subscription(
        source_name: impl Into<String>,
        subscription_source: Box<dyn SubscriptionSource>,
        decoder: Arc<dyn Decoder>,
    ) -> Self {
        Self {
            source_name: source_name.into(),
            inputs: Vec::new(),
            outputs: Vec::new(),
            subscription_source: Some(subscription_source),
            decoder: Some(decoder),
        }
    }
    
    /// Set the subscription source for this processor
    pub fn set_subscription_source(&mut self, source: Box<dyn SubscriptionSource>) {
        self.subscription_source = Some(source);
    }
    
    /// Set the decoder for this processor
    pub fn set_decoder(&mut self, decoder: Arc<dyn Decoder>) {
        self.decoder = Some(decoder);
    }
}

impl Processor for DataSourceProcessor {
    fn id(&self) -> &str {
        &self.source_name
    }
    
    fn start(&mut self) -> tokio::task::JoinHandle<Result<(), ProcessorError>> {
        let mut inputs = std::mem::take(&mut self.inputs);
        let outputs = self.outputs.clone();
        let subscription_source = self.subscription_source.take();
        let decoder = self.decoder.clone();
        let source_name = self.source_name.clone();
        
        tokio::spawn(async move {
            // Start subscription if available
            let mut subscription_receiver: Option<mpsc::Receiver<Vec<u8>>> = None;
            let mut subscription_source_handle: Option<Box<dyn SubscriptionSource>> = None;
            
            if let (Some(mut source), Some(_)) = (subscription_source, decoder.as_ref()) {
                match source.subscribe().await {
                    Ok(receiver) => {
                        subscription_receiver = Some(receiver);
                        subscription_source_handle = Some(source);
                    }
                    Err(e) => {
                        // Send error downstream if subscription fails
                        let error_data = StreamData::error(
                            crate::processor::StreamError::new(format!(
                                "Failed to start subscription: {}", e
                            )).with_source(source_name.clone())
                        );
                        for output in &outputs {
                            let _ = output.send(error_data.clone()).await;
                        }
                    }
                }
            }
            
            let decoder = decoder.clone();
            
            // Main loop: monitor both inputs and subscription simultaneously
            loop {
                let mut all_inputs_closed = true;
                
                // Check subscription receiver if available
                if let Some(ref mut sub_receiver) = subscription_receiver {
                    match sub_receiver.try_recv() {
                        Ok(bytes) => {
                            // Decode bytes to RecordBatch
                            if let Some(ref decoder) = decoder {
                                match decoder.decode(&bytes).await {
                                    Ok(record_batch) => {
                                        // Send RecordBatch downstream
                                        let stream_data = StreamData::collection(Box::new(record_batch));
                                        for output in &outputs {
                                            if output.send(stream_data.clone()).await.is_err() {
                                                return Err(ProcessorError::ChannelClosed);
                                            }
                                        }
                                    }
                                    Err(e) => {
                                        // Send error downstream
                                        let error_data = StreamData::error(
                                            crate::processor::StreamError::new(format!(
                                                "Decoder '{}' failed: {}", decoder.name(), e
                                            )).with_source(source_name.clone())
                                        );
                                        for output in &outputs {
                                            if output.send(error_data.clone()).await.is_err() {
                                                return Err(ProcessorError::ChannelClosed);
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        Err(mpsc::error::TryRecvError::Empty) => {
                            // No data available, continue
                        }
                        Err(mpsc::error::TryRecvError::Disconnected) => {
                            // Subscription channel closed
                            if let Some(mut source) = subscription_source_handle.take() {
                                let _ = source.stop().await;
                            }
                            subscription_receiver = None;
                        }
                    }
                }
                
                // Check all input channels
                for input in &mut inputs {
                    match input.try_recv() {
                        Ok(data) => {
                            all_inputs_closed = false;
                            
                            // Handle control signals
                            if let Some(control) = data.as_control() {
                                match control {
                                    crate::processor::ControlSignal::StreamEnd => {
                                        // Forward StreamEnd to outputs and stop subscription
                                        if let Some(mut source) = subscription_source_handle.take() {
                                            let _ = source.stop().await;
                                        }
                                        for output in &outputs {
                                            let _ = output.send(data.clone()).await;
                                        }
                                        return Ok(());
                                    }
                                    _ => {
                                        // Forward other control signals
                                        for output in &outputs {
                                            if output.send(data.clone()).await.is_err() {
                                                return Err(ProcessorError::ChannelClosed);
                                            }
                                        }
                                    }
                                }
                            } else {
                                // Forward non-control data (Collection or Error)
                                for output in &outputs {
                                    if output.send(data.clone()).await.is_err() {
                                        return Err(ProcessorError::ChannelClosed);
                                    }
                                }
                            }
                        }
                        Err(mpsc::error::TryRecvError::Empty) => {
                            all_inputs_closed = false;
                        }
                        Err(mpsc::error::TryRecvError::Disconnected) => {
                            // Channel disconnected
                        }
                    }
                }
                
                // If all input channels are closed and no subscription, exit
                if all_inputs_closed && subscription_receiver.is_none() {
                    return Ok(());
                }
                
                // Yield to allow other tasks to run
                tokio::task::yield_now().await;
            }
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

