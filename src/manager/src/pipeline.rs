use crate::{DEFAULT_BROKER_URL, MQTT_QOS, SINK_TOPIC};
use axum::{
    Json,
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
};
use flow::pipeline::{
    MqttSinkProps, PipelineDefinition, PipelineError, PipelineManager, PipelineStatus,
    SinkDefinition, SinkProps, SinkType,
};
use serde::{Deserialize, Serialize};
use serde_json::{Map as JsonMap, Value as JsonValue};
use std::sync::Arc;

#[derive(Clone)]
pub struct AppState {
    pub pipeline_manager: Arc<PipelineManager>,
}

impl AppState {
    pub fn new() -> Self {
        Self {
            pipeline_manager: Arc::new(PipelineManager::new()),
        }
    }
}

#[derive(Deserialize)]
pub struct CreatePipelineRequest {
    pub id: String,
    pub sql: String,
    #[serde(default)]
    pub sinks: Vec<CreatePipelineSinkRequest>,
}

#[derive(Serialize)]
pub struct CreatePipelineResponse {
    pub id: String,
    pub status: String,
}

#[derive(Serialize)]
pub struct ListPipelineItem {
    pub id: String,
    pub status: String,
}

#[derive(Deserialize, Clone)]
pub struct CreatePipelineSinkRequest {
    pub id: Option<String>,
    #[serde(rename = "type")]
    pub sink_type: String,
    #[serde(default)]
    pub props: SinkPropsRequest,
}

#[derive(Deserialize, Default, Clone)]
#[serde(default)]
pub struct SinkPropsRequest {
    #[serde(flatten)]
    fields: JsonMap<String, JsonValue>,
}

impl SinkPropsRequest {
    fn to_value(&self) -> JsonValue {
        JsonValue::Object(self.fields.clone())
    }
}

#[derive(Deserialize, Default, Clone)]
#[serde(default)]
pub struct MqttSinkPropsRequest {
    pub broker_url: Option<String>,
    pub topic: Option<String>,
    pub qos: Option<u8>,
    pub retain: Option<bool>,
    pub client_id: Option<String>,
    pub connector_key: Option<String>,
}

pub async fn create_pipeline_handler(
    State(state): State<AppState>,
    Json(req): Json<CreatePipelineRequest>,
) -> impl IntoResponse {
    if let Err(err) = validate_create_request(&req) {
        return (StatusCode::BAD_REQUEST, err).into_response();
    }
    let definition = match build_pipeline_definition(&req) {
        Ok(def) => def,
        Err(err) => return (StatusCode::BAD_REQUEST, err).into_response(),
    };
    match state.pipeline_manager.create_pipeline(definition) {
        Ok(snapshot) => {
            println!("[manager] pipeline {} created", snapshot.definition.id());
            (
                StatusCode::CREATED,
                Json(CreatePipelineResponse {
                    id: snapshot.definition.id().to_string(),
                    status: status_label(snapshot.status),
                }),
            )
                .into_response()
        }
        Err(PipelineError::AlreadyExists(_)) => (
            StatusCode::CONFLICT,
            format!("pipeline {} already exists", req.id),
        )
            .into_response(),
        Err(err) => (
            StatusCode::BAD_REQUEST,
            format!("failed to create pipeline {}: {err}", req.id),
        )
            .into_response(),
    }
}

pub async fn start_pipeline_handler(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    match state.pipeline_manager.start_pipeline(&id) {
        Ok(_) => {
            println!("[manager] pipeline {} started", id);
            (StatusCode::OK, format!("pipeline {id} started")).into_response()
        }
        Err(PipelineError::NotFound(_)) => {
            (StatusCode::NOT_FOUND, format!("pipeline {id} not found")).into_response()
        }
        Err(err) => (
            StatusCode::BAD_REQUEST,
            format!("failed to start pipeline {id}: {err}"),
        )
            .into_response(),
    }
}

pub async fn delete_pipeline_handler(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    match state.pipeline_manager.delete_pipeline(&id).await {
        Ok(_) => (StatusCode::OK, format!("pipeline {id} deleted")).into_response(),
        Err(PipelineError::NotFound(_)) => {
            (StatusCode::NOT_FOUND, format!("pipeline {id} not found")).into_response()
        }
        Err(err) => (
            StatusCode::BAD_REQUEST,
            format!("failed to delete pipeline {id}: {err}"),
        )
            .into_response(),
    }
}

pub async fn list_pipelines(State(state): State<AppState>) -> impl IntoResponse {
    let list = state
        .pipeline_manager
        .list()
        .into_iter()
        .map(|snapshot| ListPipelineItem {
            id: snapshot.definition.id().to_string(),
            status: status_label(snapshot.status),
        })
        .collect::<Vec<_>>();
    Json(list)
}

fn validate_create_request(req: &CreatePipelineRequest) -> Result<(), String> {
    if req.id.trim().is_empty() {
        return Err("pipeline id must not be empty".to_string());
    }
    if req.sql.trim().is_empty() {
        return Err("pipeline sql must not be empty".to_string());
    }
    if req.sinks.is_empty() {
        return Err("pipeline must define at least one sink".to_string());
    }
    Ok(())
}

fn build_pipeline_definition(req: &CreatePipelineRequest) -> Result<PipelineDefinition, String> {
    let mut sinks = Vec::with_capacity(req.sinks.len());
    for (index, sink_req) in req.sinks.iter().enumerate() {
        let sink_id = sink_req
            .id
            .clone()
            .unwrap_or_else(|| format!("{}_sink_{index}", req.id));
        let sink_type = sink_req.sink_type.to_ascii_lowercase();
        let sink_definition = match sink_type.as_str() {
            "mqtt" => {
                let mqtt_props: MqttSinkPropsRequest =
                    serde_json::from_value(sink_req.props.to_value())
                        .map_err(|err| format!("invalid mqtt sink props: {err}"))?;
                let broker = mqtt_props
                    .broker_url
                    .unwrap_or_else(|| DEFAULT_BROKER_URL.to_string());
                let topic = mqtt_props.topic.unwrap_or_else(|| SINK_TOPIC.to_string());
                let qos = mqtt_props.qos.unwrap_or(MQTT_QOS);
                let retain = mqtt_props.retain.unwrap_or(false);

                let mut props = MqttSinkProps::new(broker, topic, qos).with_retain(retain);
                if let Some(client_id) = mqtt_props.client_id {
                    props = props.with_client_id(client_id);
                }
                if let Some(connector_key) = mqtt_props.connector_key {
                    props = props.with_connector_key(connector_key);
                }
                SinkDefinition::new(sink_id, SinkType::Mqtt, SinkProps::Mqtt(props))
            }
            other => return Err(format!("unsupported sink type: {other}")),
        };
        sinks.push(sink_definition);
    }
    Ok(PipelineDefinition::new(
        req.id.clone(),
        req.sql.clone(),
        sinks,
    ))
}

fn status_label(status: PipelineStatus) -> String {
    match status {
        PipelineStatus::Created => "created".to_string(),
        PipelineStatus::Running => "running".to_string(),
    }
}
