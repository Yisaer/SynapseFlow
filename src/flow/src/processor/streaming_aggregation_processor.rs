//! StreamingAggregationProcessor - incremental aggregation with windowing.

use crate::aggregation::{AggregateAccumulator, AggregateFunctionRegistry};
use crate::model::{Collection, RecordBatch};
use crate::planner::physical::{
    AggregateCall, PhysicalPlan, PhysicalStreamingAggregation, StreamingWindowSpec,
};
use crate::processor::base::{
    fan_in_control_streams, fan_in_streams, log_received_data, send_control_with_backpressure,
    send_with_backpressure, DEFAULT_CHANNEL_CAPACITY,
};
use crate::processor::{ControlSignal, Processor, ProcessorError, StreamData};
use datatypes::Value;
use futures::stream::StreamExt;
use sqlparser::ast::Expr;
use std::collections::hash_map::Entry;
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::broadcast;
use tokio_stream::wrappers::errors::BroadcastStreamRecvError;

/// Tracks aggregation state for a single group key.
struct GroupState {
    accumulators: Vec<Box<dyn AggregateAccumulator>>,
    last_tuple: crate::model::Tuple,
    key_values: Vec<Value>,
}

/// Shared aggregation logic reused by count and tumbling windows.
struct AggregationWorker {
    physical: Arc<PhysicalStreamingAggregation>,
    aggregate_registry: Arc<AggregateFunctionRegistry>,
    group_by_simple_flags: Vec<bool>,
    groups: HashMap<String, GroupState>,
}

impl AggregationWorker {
    fn new(
        physical: Arc<PhysicalStreamingAggregation>,
        aggregate_registry: Arc<AggregateFunctionRegistry>,
        group_by_simple_flags: Vec<bool>,
    ) -> Self {
        Self {
            physical,
            aggregate_registry,
            group_by_simple_flags,
            groups: HashMap::new(),
        }
    }

    fn update_groups(&mut self, tuple: &crate::model::Tuple) -> Result<(), String> {
        let key_values = self.evaluate_group_by(tuple)?;
        let key_repr = format!("{:?}", key_values);

        let entry = match self.groups.entry(key_repr) {
            Entry::Occupied(o) => o.into_mut(),
            Entry::Vacant(v) => {
                let accumulators = create_accumulators_static(
                    &self.physical.aggregate_calls,
                    self.aggregate_registry.as_ref(),
                )?;
                v.insert(GroupState {
                    accumulators,
                    last_tuple: tuple.clone(),
                    key_values: key_values.clone(),
                })
            }
        };

        entry.last_tuple = tuple.clone();
        entry.key_values = key_values;

        for (idx, call) in self.physical.aggregate_calls.iter().enumerate() {
            let mut args = Vec::new();
            for arg_expr in &call.args {
                args.push(
                    arg_expr
                        .eval_with_tuple(tuple)
                        .map_err(|e| format!("Failed to evaluate aggregate argument: {}", e))?,
                );
            }
            entry
                .accumulators
                .get_mut(idx)
                .ok_or_else(|| "accumulator missing".to_string())?
                .update(&args)?;
        }

        Ok(())
    }

    fn evaluate_group_by(&self, tuple: &crate::model::Tuple) -> Result<Vec<Value>, String> {
        let mut values = Vec::with_capacity(self.physical.group_by_scalars.len());
        for scalar in &self.physical.group_by_scalars {
            values.push(
                scalar
                    .eval_with_tuple(tuple)
                    .map_err(|e| format!("Failed to evaluate group-by expression: {}", e))?,
            );
        }
        Ok(values)
    }

    fn finalize_current_window(&mut self) -> Result<Option<Box<dyn Collection>>, String> {
        if self.groups.is_empty() {
            self.groups.clear();
            return Ok(None);
        }

        let mut output_tuples = Vec::with_capacity(self.groups.len());
        for (_key, mut state) in self.groups.drain() {
            let tuple = finalize_group(
                &self.physical.aggregate_calls,
                &self.physical.group_by_exprs,
                &self.group_by_simple_flags,
                &mut state.accumulators,
                &state.last_tuple,
                &state.key_values,
            )?;
            output_tuples.push(tuple);
        }

        let collection = RecordBatch::new(output_tuples)
            .map_err(|e| format!("Failed to build RecordBatch: {e}"))?;
        Ok(Some(Box::new(collection)))
    }
}

/// Tracks progress for a count-based window.
struct CountWindowState {
    target: u64,
    seen: u64,
}

impl CountWindowState {
    fn new(target: u64) -> Self {
        Self { target, seen: 0 }
    }

    fn register_row_and_check_finalize(&mut self) -> bool {
        self.seen += 1;
        self.seen >= self.target
    }

    fn reset(&mut self) {
        self.seen = 0;
    }
}

/// Per-window aggregation state.
struct WindowAggState {
    start_secs: u64,
    end_secs: u64,
    worker: AggregationWorker,
}

impl WindowAggState {
    fn new(
        start_secs: u64,
        len_secs: u64,
        physical: Arc<PhysicalStreamingAggregation>,
        aggregate_registry: Arc<AggregateFunctionRegistry>,
        group_by_simple_flags: Vec<bool>,
    ) -> Self {
        Self {
            start_secs,
            end_secs: start_secs.saturating_add(len_secs),
            worker: AggregationWorker::new(
                Arc::clone(&physical),
                Arc::clone(&aggregate_registry),
                group_by_simple_flags,
            ),
        }
    }
}

/// Window manager supporting event-time and processing-time modes.
enum WindowState {
    EventTime(EventWindowState),
    ProcessingTime(ProcessingWindowState),
}

impl WindowState {
    fn add_row(&mut self, row: &crate::model::Tuple) -> Result<(), String> {
        match self {
            WindowState::EventTime(state) => state.add_row(row),
            WindowState::ProcessingTime(state) => state.add_row(row),
        }
    }

    async fn flush_until(
        &mut self,
        watermark: SystemTime,
        output: &broadcast::Sender<StreamData>,
    ) -> Result<(), ProcessorError> {
        match self {
            WindowState::EventTime(state) => state.flush_until(watermark, output).await,
            WindowState::ProcessingTime(state) => state.flush_until(watermark, output).await,
        }
    }

    async fn flush_all(
        &mut self,
        output: &broadcast::Sender<StreamData>,
    ) -> Result<(), ProcessorError> {
        match self {
            WindowState::EventTime(state) => state.flush_all(output).await,
            WindowState::ProcessingTime(state) => state.flush_all(output).await,
        }
    }
}

struct EventWindowState {
    windows: BTreeMap<u64, WindowAggState>,
    len_secs: u64,
    physical: Arc<PhysicalStreamingAggregation>,
    aggregate_registry: Arc<AggregateFunctionRegistry>,
    group_by_simple_flags: Vec<bool>,
}

impl EventWindowState {
    fn new(
        len_secs: u64,
        physical: Arc<PhysicalStreamingAggregation>,
        aggregate_registry: Arc<AggregateFunctionRegistry>,
        group_by_simple_flags: Vec<bool>,
    ) -> Self {
        Self {
            windows: BTreeMap::new(),
            len_secs,
            physical,
            aggregate_registry,
            group_by_simple_flags,
        }
    }

    fn add_row(&mut self, row: &crate::model::Tuple) -> Result<(), String> {
        let start_secs = window_start_secs_str(row.timestamp, self.len_secs)?;
        let entry = self.windows.entry(start_secs).or_insert_with(|| {
            WindowAggState::new(
                start_secs,
                self.len_secs,
                Arc::clone(&self.physical),
                Arc::clone(&self.aggregate_registry),
                self.group_by_simple_flags.clone(),
            )
        });
        entry.worker.update_groups(row)
    }

    async fn flush_until(
        &mut self,
        watermark: SystemTime,
        output: &broadcast::Sender<StreamData>,
    ) -> Result<(), ProcessorError> {
        let watermark_secs = to_secs(watermark, "watermark")?;
        let mut ready = Vec::new();
        for (&start, state) in self.windows.iter() {
            if state.end_secs <= watermark_secs {
                ready.push(start);
            }
        }

        for key in ready {
            if let Some(mut state) = self.windows.remove(&key) {
                if let Some(batch) = state
                    .worker
                    .finalize_current_window()
                    .map_err(|e| ProcessorError::ProcessingError(e))?
                {
                    send_with_backpressure(output, StreamData::Collection(batch)).await?;
                }
            }
        }
        Ok(())
    }

    async fn flush_all(
        &mut self,
        output: &broadcast::Sender<StreamData>,
    ) -> Result<(), ProcessorError> {
        let keys: Vec<u64> = self.windows.keys().copied().collect();
        for key in keys {
            if let Some(mut state) = self.windows.remove(&key) {
                if let Some(batch) = state
                    .worker
                    .finalize_current_window()
                    .map_err(|e| ProcessorError::ProcessingError(e))?
                {
                    send_with_backpressure(output, StreamData::Collection(batch)).await?;
                }
            }
        }
        Ok(())
    }
}

struct ProcessingWindowState {
    windows: VecDeque<WindowAggState>,
    len_secs: u64,
    physical: Arc<PhysicalStreamingAggregation>,
    aggregate_registry: Arc<AggregateFunctionRegistry>,
    group_by_simple_flags: Vec<bool>,
}

impl ProcessingWindowState {
    fn new(
        len_secs: u64,
        physical: Arc<PhysicalStreamingAggregation>,
        aggregate_registry: Arc<AggregateFunctionRegistry>,
        group_by_simple_flags: Vec<bool>,
    ) -> Self {
        Self {
            windows: VecDeque::new(),
            len_secs,
            physical,
            aggregate_registry,
            group_by_simple_flags,
        }
    }

    fn add_row(&mut self, row: &crate::model::Tuple) -> Result<(), String> {
        let start_secs = window_start_secs_str(row.timestamp, self.len_secs)?;
        if let Some(back) = self.windows.back_mut() {
            if back.start_secs == start_secs {
                return back.worker.update_groups(row);
            }
        }
        let new_state = WindowAggState::new(
            start_secs,
            self.len_secs,
            Arc::clone(&self.physical),
            Arc::clone(&self.aggregate_registry),
            self.group_by_simple_flags.clone(),
        );
        self.windows.push_back(new_state);
        self.windows
            .back_mut()
            .expect("just pushed")
            .worker
            .update_groups(row)
    }

    async fn flush_until(
        &mut self,
        watermark: SystemTime,
        output: &broadcast::Sender<StreamData>,
    ) -> Result<(), ProcessorError> {
        let watermark_secs = to_secs(watermark, "watermark")?;
        while let Some(front) = self.windows.front() {
            if front.end_secs > watermark_secs {
                break;
            }
            let mut state = self.windows.pop_front().expect("front exists");
            if let Some(batch) = state
                .worker
                .finalize_current_window()
                .map_err(|e| ProcessorError::ProcessingError(e))?
            {
                send_with_backpressure(output, StreamData::Collection(batch)).await?;
            }
        }
        Ok(())
    }

    async fn flush_all(
        &mut self,
        output: &broadcast::Sender<StreamData>,
    ) -> Result<(), ProcessorError> {
        while let Some(mut state) = self.windows.pop_front() {
            if let Some(batch) = state
                .worker
                .finalize_current_window()
                .map_err(|e| ProcessorError::ProcessingError(e))?
            {
                send_with_backpressure(output, StreamData::Collection(batch)).await?;
            }
        }
        Ok(())
    }
}

/// StreamingAggregationProcessor - performs incremental aggregation with windowing.
pub enum StreamingAggregationProcessor {
    Count(StreamingCountAggregationProcessor),
    Tumbling(StreamingTumblingAggregationProcessor),
}

/// Data-driven count window implementation.
pub struct StreamingCountAggregationProcessor {
    id: String,
    physical: Arc<PhysicalStreamingAggregation>,
    aggregate_registry: Arc<AggregateFunctionRegistry>,
    inputs: Vec<broadcast::Receiver<StreamData>>,
    control_inputs: Vec<broadcast::Receiver<ControlSignal>>,
    output: broadcast::Sender<StreamData>,
    control_output: broadcast::Sender<ControlSignal>,
    group_by_simple_flags: Vec<bool>,
    target: u64,
}

/// Time-driven tumbling window implementation.
pub struct StreamingTumblingAggregationProcessor {
    id: String,
    physical: Arc<PhysicalStreamingAggregation>,
    aggregate_registry: Arc<AggregateFunctionRegistry>,
    inputs: Vec<broadcast::Receiver<StreamData>>,
    control_inputs: Vec<broadcast::Receiver<ControlSignal>>,
    output: broadcast::Sender<StreamData>,
    control_output: broadcast::Sender<ControlSignal>,
    group_by_simple_flags: Vec<bool>,
    event_time: bool,
}

impl StreamingAggregationProcessor {
    pub fn new(
        id: impl Into<String>,
        physical: Arc<PhysicalStreamingAggregation>,
        aggregate_registry: Arc<AggregateFunctionRegistry>,
    ) -> Self {
        let window = physical.window.clone();
        match window {
            StreamingWindowSpec::Count { count } => {
                StreamingAggregationProcessor::Count(StreamingCountAggregationProcessor::new(
                    id,
                    Arc::clone(&physical),
                    Arc::clone(&aggregate_registry),
                    count,
                ))
            }
            StreamingWindowSpec::Tumbling {
                time_unit: _,
                length: _,
            } => {
                // Currently only seconds are supported at the logical level.
                StreamingAggregationProcessor::Tumbling(StreamingTumblingAggregationProcessor::new(
                    id,
                    Arc::clone(&physical),
                    aggregate_registry,
                ))
            }
        }
    }

    pub fn from_physical_plan(
        id: impl Into<String>,
        plan: Arc<PhysicalPlan>,
        aggregate_registry: Arc<AggregateFunctionRegistry>,
    ) -> Option<Self> {
        match plan.as_ref() {
            PhysicalPlan::StreamingAggregation(aggregation) => Some(Self::new(
                id,
                Arc::new(aggregation.clone()),
                aggregate_registry,
            )),
            _ => None,
        }
    }
}

impl Processor for StreamingAggregationProcessor {
    fn id(&self) -> &str {
        match self {
            StreamingAggregationProcessor::Count(p) => p.id(),
            StreamingAggregationProcessor::Tumbling(p) => p.id(),
        }
    }

    fn start(&mut self) -> tokio::task::JoinHandle<Result<(), ProcessorError>> {
        match self {
            StreamingAggregationProcessor::Count(p) => p.start(),
            StreamingAggregationProcessor::Tumbling(p) => p.start(),
        }
    }

    fn subscribe_output(&self) -> Option<broadcast::Receiver<StreamData>> {
        match self {
            StreamingAggregationProcessor::Count(p) => p.subscribe_output(),
            StreamingAggregationProcessor::Tumbling(p) => p.subscribe_output(),
        }
    }

    fn subscribe_control_output(&self) -> Option<broadcast::Receiver<ControlSignal>> {
        match self {
            StreamingAggregationProcessor::Count(p) => p.subscribe_control_output(),
            StreamingAggregationProcessor::Tumbling(p) => p.subscribe_control_output(),
        }
    }

    fn add_input(&mut self, receiver: broadcast::Receiver<StreamData>) {
        match self {
            StreamingAggregationProcessor::Count(p) => p.add_input(receiver),
            StreamingAggregationProcessor::Tumbling(p) => p.add_input(receiver),
        }
    }

    fn add_control_input(&mut self, receiver: broadcast::Receiver<ControlSignal>) {
        match self {
            StreamingAggregationProcessor::Count(p) => p.add_control_input(receiver),
            StreamingAggregationProcessor::Tumbling(p) => p.add_control_input(receiver),
        }
    }
}

impl StreamingCountAggregationProcessor {
    fn new(
        id: impl Into<String>,
        physical: Arc<PhysicalStreamingAggregation>,
        aggregate_registry: Arc<AggregateFunctionRegistry>,
        target: u64,
    ) -> Self {
        let group_by_simple_flags = group_by_flags(&physical.group_by_exprs);
        let (output, _) = broadcast::channel(DEFAULT_CHANNEL_CAPACITY);
        let (control_output, _) = broadcast::channel(DEFAULT_CHANNEL_CAPACITY);
        Self {
            id: id.into(),
            physical,
            aggregate_registry,
            inputs: Vec::new(),
            control_inputs: Vec::new(),
            output,
            control_output,
            group_by_simple_flags,
            target,
        }
    }

    fn process_collection(
        worker: &mut AggregationWorker,
        window_state: &mut CountWindowState,
        collection: &dyn Collection,
    ) -> Result<Vec<Box<dyn Collection>>, String> {
        let mut outputs = Vec::new();
        for row in collection.rows() {
            worker.update_groups(row)?;

            if window_state.register_row_and_check_finalize() {
                if let Some(batch) = worker.finalize_current_window()? {
                    outputs.push(batch);
                }
                window_state.reset();
            }
        }
        Ok(outputs)
    }

    fn id(&self) -> &str {
        &self.id
    }
}

impl Processor for StreamingCountAggregationProcessor {
    fn id(&self) -> &str {
        self.id()
    }

    fn start(&mut self) -> tokio::task::JoinHandle<Result<(), ProcessorError>> {
        let id = self.id.clone();
        let mut input_streams = fan_in_streams(std::mem::take(&mut self.inputs));
        let control_receivers = std::mem::take(&mut self.control_inputs);
        let control_active = !control_receivers.is_empty();
        let mut control_streams = fan_in_control_streams(control_receivers);
        let output = self.output.clone();
        let control_output = self.control_output.clone();
        let aggregate_registry = Arc::clone(&self.aggregate_registry);
        let physical = Arc::clone(&self.physical);
        let group_by_simple_flags = self.group_by_simple_flags.clone();
        let target = self.target;

        tokio::spawn(async move {
            let mut worker =
                AggregationWorker::new(physical, aggregate_registry, group_by_simple_flags);
            let mut window_state = CountWindowState::new(target);
            let mut stream_ended = false;

            loop {
                tokio::select! {
                    // Handle control signals first if present
                    biased;
                    Some(ctrl) = control_streams.next(), if control_active => {
                        if let Ok(control_signal) = ctrl {
                            let is_terminal = control_signal.is_terminal();
                            send_control_with_backpressure(&control_output, control_signal).await?;
                            if is_terminal {
                                stream_ended = true;
                                break;
                            }
                        }
                    }
                    data_item = input_streams.next() => {
                        match data_item {
                            Some(Ok(StreamData::Collection(collection))) => {
                                log_received_data(&id, &StreamData::Collection(collection.clone()));
                                match StreamingCountAggregationProcessor::process_collection(&mut worker, &mut window_state, collection.as_ref()) {
                                    Ok(outputs) => {
                                        for out in outputs {
                                            let data = StreamData::Collection(out);
                                            send_with_backpressure(&output, data).await?
                                        }
                                    }
                                    Err(e) => {
                                        return Err(ProcessorError::ProcessingError(format!("Failed to process streaming count aggregation: {e}")));
                                    }
                                }
                            }
                            Some(Ok(StreamData::Control(control_signal))) => {
                                let is_terminal = control_signal.is_terminal();
                                send_control_with_backpressure(&control_output, control_signal).await?;
                                if is_terminal {
                                    stream_ended = true;
                                    break;
                                }
                            }
                            Some(Ok(other)) => {
                                log_received_data(&id, &other);
                                send_with_backpressure(&output, other).await?;
                            }
                            Some(Err(BroadcastStreamRecvError::Lagged(n))) => {
                                println!("[StreamingCountAggregationProcessor:{id}] lagged by {n} messages");
                            }
                            None => {
                                println!("[StreamingCountAggregationProcessor:{id}] all input streams ended");
                                break;
                            }
                        }
                    }
                }
            }

            if stream_ended {
                send_control_with_backpressure(&control_output, ControlSignal::StreamGracefulEnd)
                    .await?;
            }
            Ok(())
        })
    }

    fn subscribe_output(&self) -> Option<broadcast::Receiver<StreamData>> {
        Some(self.output.subscribe())
    }

    fn subscribe_control_output(&self) -> Option<broadcast::Receiver<ControlSignal>> {
        Some(self.control_output.subscribe())
    }

    fn add_input(&mut self, receiver: broadcast::Receiver<StreamData>) {
        self.inputs.push(receiver);
    }

    fn add_control_input(&mut self, receiver: broadcast::Receiver<ControlSignal>) {
        self.control_inputs.push(receiver);
    }
}

impl StreamingTumblingAggregationProcessor {
    fn new(
        id: impl Into<String>,
        physical: Arc<PhysicalStreamingAggregation>,
        aggregate_registry: Arc<AggregateFunctionRegistry>,
    ) -> Self {
        let group_by_simple_flags = group_by_flags(&physical.group_by_exprs);
        let (output, _) = broadcast::channel(DEFAULT_CHANNEL_CAPACITY);
        let (control_output, _) = broadcast::channel(DEFAULT_CHANNEL_CAPACITY);
        Self {
            id: id.into(),
            physical,
            aggregate_registry,
            inputs: Vec::new(),
            control_inputs: Vec::new(),
            output,
            control_output,
            group_by_simple_flags,
            event_time: false,
        }
    }

    fn id(&self) -> &str {
        &self.id
    }
}

impl Processor for StreamingTumblingAggregationProcessor {
    fn id(&self) -> &str {
        self.id()
    }

    fn start(&mut self) -> tokio::task::JoinHandle<Result<(), ProcessorError>> {
        let id = self.id.clone();
        let mut input_streams = fan_in_streams(std::mem::take(&mut self.inputs));
        let control_receivers = std::mem::take(&mut self.control_inputs);
        let control_active = !control_receivers.is_empty();
        let mut control_streams = fan_in_control_streams(control_receivers);
        let output = self.output.clone();
        let control_output = self.control_output.clone();
        let aggregate_registry = Arc::clone(&self.aggregate_registry);
        let physical = Arc::clone(&self.physical);
        let group_by_simple_flags = self.group_by_simple_flags.clone();
        let len_secs = match physical.window {
            StreamingWindowSpec::Tumbling {
                time_unit: _,
                length,
            } => length,
            _ => unreachable!("tumbling processor requires tumbling window spec"),
        };
        let event_time = self.event_time;

        tokio::spawn(async move {
            let window_state = if event_time {
                WindowState::EventTime(EventWindowState::new(
                    len_secs,
                    Arc::clone(&physical),
                    Arc::clone(&aggregate_registry),
                    group_by_simple_flags.clone(),
                ))
            } else {
                WindowState::ProcessingTime(ProcessingWindowState::new(
                    len_secs,
                    Arc::clone(&physical),
                    Arc::clone(&aggregate_registry),
                    group_by_simple_flags.clone(),
                ))
            };
            let mut window_state = window_state;
            let mut stream_ended = false;

            loop {
                tokio::select! {
                    biased;
                    Some(ctrl) = control_streams.next(), if control_active => {
                        if let Ok(control_signal) = ctrl {
                            let is_terminal = control_signal.is_terminal();
                            send_control_with_backpressure(&control_output, control_signal).await?;
                            if is_terminal {
                                stream_ended = true;
                                break;
                            }
                        }
                    }
                    data_item = input_streams.next() => {
                        match data_item {
                            Some(Ok(StreamData::Collection(collection))) => {
                                log_received_data(&id, &StreamData::Collection(collection.clone()));
                                for row in collection.rows() {
                                    window_state.add_row(row).map_err(|e| ProcessorError::ProcessingError(format!("Failed to update window state: {e}")))?;
                                }
                            }
                            Some(Ok(StreamData::Watermark(ts))) => {
                                window_state.flush_until(ts, &output).await?;
                            }
                            Some(Ok(StreamData::Control(control_signal))) => {
                                let is_terminal = control_signal.is_terminal();
                                let is_graceful = matches!(control_signal, ControlSignal::StreamGracefulEnd);
                                send_control_with_backpressure(&control_output, control_signal).await?;
                                if is_terminal {
                                    if is_graceful {
                                        window_state.flush_all(&output).await?;
                                    }
                                    stream_ended = true;
                                    break;
                                }
                            }
                            Some(Ok(other)) => {
                                log_received_data(&id, &other);
                                send_with_backpressure(&output, other).await?;
                            }
                            Some(Err(BroadcastStreamRecvError::Lagged(n))) => {
                                println!("[StreamingTumblingAggregationProcessor:{id}] lagged by {n} messages");
                            }
                            None => {
                                println!("[StreamingTumblingAggregationProcessor:{id}] all input streams ended");
                                break;
                            }
                        }
                    }
                }
            }

            if stream_ended {
                send_control_with_backpressure(&control_output, ControlSignal::StreamGracefulEnd)
                    .await?;
            }
            Ok(())
        })
    }

    fn subscribe_output(&self) -> Option<broadcast::Receiver<StreamData>> {
        Some(self.output.subscribe())
    }

    fn subscribe_control_output(&self) -> Option<broadcast::Receiver<ControlSignal>> {
        Some(self.control_output.subscribe())
    }

    fn add_input(&mut self, receiver: broadcast::Receiver<StreamData>) {
        self.inputs.push(receiver);
    }

    fn add_control_input(&mut self, receiver: broadcast::Receiver<ControlSignal>) {
        self.control_inputs.push(receiver);
    }
}

fn finalize_group(
    aggregate_calls: &[AggregateCall],
    group_by_exprs: &[Expr],
    group_by_simple_flags: &[bool],
    accumulators: &mut [Box<dyn AggregateAccumulator>],
    last_tuple: &crate::model::Tuple,
    key_values: &[Value],
) -> Result<crate::model::Tuple, String> {
    use crate::model::AffiliateRow;
    use std::sync::Arc;

    let mut affiliate_entries = Vec::new();
    for (call, accumulator) in aggregate_calls.iter().zip(accumulators.iter_mut()) {
        affiliate_entries.push((Arc::new(call.output_column.clone()), accumulator.finalize()));
    }

    for (idx, value) in key_values.iter().enumerate() {
        if !group_by_simple_flags.get(idx).copied().unwrap_or(false) {
            let name = group_by_exprs[idx].to_string();
            affiliate_entries.push((Arc::new(name), value.clone()));
        }
    }

    let mut tuple = last_tuple.clone();
    let mut affiliate = tuple
        .affiliate
        .take()
        .unwrap_or_else(|| AffiliateRow::new(Vec::new()));
    for (k, v) in affiliate_entries {
        affiliate.insert(k, v);
    }
    tuple.affiliate = Some(affiliate);
    Ok(tuple)
}

fn create_accumulators_static(
    aggregate_calls: &[AggregateCall],
    registry: &AggregateFunctionRegistry,
) -> Result<Vec<Box<dyn AggregateAccumulator>>, String> {
    let mut accumulators = Vec::new();
    for call in aggregate_calls {
        if call.distinct {
            return Err("DISTINCT aggregates are not supported yet".to_string());
        }
        let function = registry
            .get(&call.func_name)
            .ok_or_else(|| format!("Aggregate function '{}' not found", call.func_name))?;
        accumulators.push(function.create_accumulator());
    }
    Ok(accumulators)
}

fn is_simple_column_expr(expr: &sqlparser::ast::Expr) -> bool {
    matches!(expr, sqlparser::ast::Expr::Identifier(_))
}

fn group_by_flags(exprs: &[Expr]) -> Vec<bool> {
    exprs.iter().map(is_simple_column_expr).collect()
}

fn window_start_secs_str(ts: SystemTime, len_secs: u64) -> Result<u64, String> {
    let secs = ts
        .duration_since(UNIX_EPOCH)
        .map_err(|e| format!("invalid timestamp: {e}"))?
        .as_secs();
    let len = len_secs.max(1);
    Ok(secs / len * len)
}

fn to_secs(ts: SystemTime, label: &str) -> Result<u64, ProcessorError> {
    ts.duration_since(UNIX_EPOCH)
        .map_err(|e| ProcessorError::ProcessingError(format!("invalid {label}: {e}")))
        .map(|d| d.as_secs())
}
