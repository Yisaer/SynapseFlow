#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

mod metrics;

use std::env;
use std::ffi::CString;
use std::fs;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::process;
use std::sync::Arc;
use std::thread;
use std::time::Duration as StdDuration;

use crate::metrics::{CPU_SECONDS_TOTAL_COUNTER, CPU_USAGE_GAUGE, MEMORY_USAGE_GAUGE};
use flow::codec::JsonDecoder;
use flow::connector::{MqttSinkConfig, MqttSinkConnector, MqttSourceConfig, MqttSourceConnector};
use flow::processor::processor_builder::PlanProcessor;
use flow::processor::{ProcessorPipeline, SinkProcessor};
use flow::JsonEncoder;
use flow::Processor;
use pprof::protos::Message;
use pprof::ProfilerGuard;
use sysinfo::{Pid, System};
use tikv_jemalloc_ctl::raw;
use tokio::time::{sleep, Duration};

const DEFAULT_BROKER_URL: &str = "tcp://127.0.0.1:1883";
const SOURCE_TOPIC: &str = "/yisa/data";
const SINK_TOPIC: &str = "/yisa/data2";
const MQTT_QOS: u8 = 0;
const DEFAULT_METRICS_ADDR: &str = "0.0.0.0:9898";
const DEFAULT_METRICS_INTERVAL_SECS: u64 = 5;
const DEFAULT_PROFILE_ADDR: &str = "0.0.0.0:6060";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let profiling_enabled = profile_server_enabled();
    if profiling_enabled {
        ensure_jemalloc_profiling();
    }

    init_metrics_exporter().await?;
    if profiling_enabled {
        start_profile_server();
    }

    let sql = env::args().nth(1).unwrap_or_else(|| {
        eprintln!("Usage: synapse-flow \"<SQL query>\"");
        process::exit(1);
    });

    let mut sink = SinkProcessor::new("mqtt_sink");
    sink.disable_result_forwarding();
    let sink_config = MqttSinkConfig::new("mqtt_sink", DEFAULT_BROKER_URL, SINK_TOPIC, MQTT_QOS);
    let sink_connector = MqttSinkConnector::new("mqtt_sink_connector", sink_config);
    sink.add_connector(
        Box::new(sink_connector),
        Arc::new(JsonEncoder::new("mqtt_sink_encoder")),
    );

    let mut pipeline = flow::create_pipeline(&sql, vec![sink])?;
    attach_mqtt_sources(&mut pipeline, DEFAULT_BROKER_URL, SOURCE_TOPIC, MQTT_QOS)?;

    if let Some(mut output_rx) = pipeline.take_output() {
        tokio::spawn(async move {
            while let Some(message) = output_rx.recv().await {
                println!("[PipelineOutput] {}", message.description());
            }
            println!("[PipelineOutput] channel closed");
        });
    } else {
        println!("[PipelineOutput] output channel unavailable; nothing will be drained");
    }

    pipeline.start();
    println!("Pipeline running between MQTT topics {SOURCE_TOPIC} -> {SINK_TOPIC} WITH SQL {sql}.");
    println!("Press Ctrl+C to terminate.");

    tokio::signal::ctrl_c().await?;
    println!("Stopping pipeline...");
    pipeline.quick_close().await?;
    Ok(())
}

async fn init_metrics_exporter() -> Result<(), Box<dyn std::error::Error>> {
    let addr: SocketAddr = env::var("METRICS_ADDR")
        .unwrap_or_else(|_| DEFAULT_METRICS_ADDR.to_string())
        .parse()?;
    let exporter = prometheus_exporter::start(addr)?;
    // Leak exporter handle so the HTTP endpoint stays alive for the duration of the process.
    Box::leak(Box::new(exporter));

    let poll_interval = env::var("METRICS_POLL_INTERVAL_SECS")
        .ok()
        .and_then(|raw| raw.parse::<u64>().ok())
        .filter(|secs| *secs > 0)
        .unwrap_or(DEFAULT_METRICS_INTERVAL_SECS);

    tokio::spawn(async move {
        let mut system = System::new();
        let pid = Pid::from_u32(process::id());
        loop {
            system.refresh_process(pid);
            if let Some(proc_info) = system.process(pid) {
                let cpu_usage_percent = proc_info.cpu_usage() as f64;
                CPU_USAGE_GAUGE.set(cpu_usage_percent as i64);
                let delta_secs = (cpu_usage_percent / 100.0) * poll_interval as f64;
                if delta_secs.is_finite() && delta_secs >= 0.0 {
                    CPU_SECONDS_TOTAL_COUNTER.inc_by(delta_secs);
                }
                MEMORY_USAGE_GAUGE.set(proc_info.memory() as i64);
            } else {
                CPU_USAGE_GAUGE.set(0);
                MEMORY_USAGE_GAUGE.set(0);
            }

            sleep(Duration::from_secs(poll_interval)).await;
        }
    });

    Ok(())
}

fn start_profile_server() {
    let addr_str = env::var("PROFILE_ADDR").unwrap_or_else(|_| DEFAULT_PROFILE_ADDR.to_string());
    let addr: SocketAddr = match addr_str.parse() {
        Ok(a) => a,
        Err(err) => {
            eprintln!("[ProfileServer] invalid PROFILE_ADDR {addr_str}: {err}");
            return;
        }
    };

    thread::spawn(move || {
        if let Err(err) = run_profile_server(addr) {
            eprintln!("[ProfileServer] server error: {err}");
        }
    });
}

fn run_profile_server(addr: SocketAddr) -> std::io::Result<()> {
    let listener = TcpListener::bind(addr)?;
    println!(
        "[ProfileServer] CPU/heap endpoints at http://{addr}/debug/pprof/{{profile,flamegraph,heap}}"
    );
    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                thread::spawn(|| {
                    if let Err(err) = handle_profile_connection(stream) {
                        eprintln!("[ProfileServer] connection failed: {err}");
                    }
                });
            }
            Err(err) => eprintln!("[ProfileServer] accept error: {err}"),
        }
    }
    Ok(())
}

fn handle_profile_connection(mut stream: TcpStream) -> Result<(), Box<dyn std::error::Error>> {
    let mut buf = [0u8; 2048];
    let len = stream.read(&mut buf)?;
    if len == 0 {
        return Ok(());
    }
    let request = String::from_utf8_lossy(&buf[..len]);
    let mut lines = request.lines();
    let request_line = lines.next().unwrap_or("");
    let mut parts = request_line.split_whitespace();
    let method = parts.next().unwrap_or("");
    let target = parts.next().unwrap_or("/");
    if method != "GET" {
        write_response(&mut stream, 405, "text/plain", b"method not allowed")?;
        return Ok(());
    }
    let (path, query) = split_target(target);
    match path {
        "/debug/pprof/profile" => {
            let duration = parse_seconds(query).unwrap_or(30);
            match generate_profile(duration) {
                Ok(body) => write_response(&mut stream, 200, "application/octet-stream", &body)?,
                Err(err) => write_response(&mut stream, 500, "text/plain", err.as_bytes())?,
            }
        }
        "/debug/pprof/flamegraph" => {
            let duration = parse_seconds(query).unwrap_or(30);
            match generate_flamegraph(duration) {
                Ok(body) => write_response(&mut stream, 200, "image/svg+xml", &body)?,
                Err(err) => write_response(&mut stream, 500, "text/plain", err.as_bytes())?,
            }
        }
        "/debug/pprof/heap" => match capture_heap_profile() {
            Ok(body) => write_response(&mut stream, 200, "application/octet-stream", &body)?,
            Err(err) => write_response(&mut stream, 500, "text/plain", err.as_bytes())?,
        },
        _ => {
            write_response(&mut stream, 404, "text/plain", b"not found")?;
        }
    }
    Ok(())
}

fn generate_profile(duration: u64) -> Result<Vec<u8>, String> {
    let report = run_profiler(duration)?;
    let profile = report.pprof().map_err(|err| err.to_string())?;
    let mut body = Vec::new();
    profile.encode(&mut body).map_err(|err| err.to_string())?;
    Ok(body)
}

fn generate_flamegraph(duration: u64) -> Result<Vec<u8>, String> {
    let report = run_profiler(duration)?;
    let mut body = Vec::new();
    report
        .flamegraph(&mut body)
        .map_err(|err| err.to_string())?;
    Ok(body)
}

fn run_profiler(duration: u64) -> Result<pprof::Report, String> {
    let guard = ProfilerGuard::new(100).map_err(|err| err.to_string())?;
    thread::sleep(StdDuration::from_secs(duration));
    guard.report().build().map_err(|err| err.to_string())
}

fn capture_heap_profile() -> Result<Vec<u8>, String> {
    // Ensure profiling is active; if jemalloc lacks profiling support, return a clear error.
    if let Err(err) = unsafe { raw::write(b"prof.active\0", true) } {
        return Err(format!(
            "jemalloc heap profiling 未开启或不支持（需要带 profiling 的 tikv-jemallocator，且启动时设置 MALLOC_CONF=\"prof:true,prof_active:true\"）。错误: {}",
            err
        ));
    }

    let filename = format!("/tmp/synapse_flow.{}.heap", process::id());
    let c_path = CString::new(filename.clone()).map_err(|err| err.to_string())?;
    unsafe {
        raw::write(b"prof.dump\0", c_path.as_ptr()).map_err(|err| err.to_string())?;
    }
    let body = fs::read(&filename).map_err(|err| err.to_string())?;
    let _ = fs::remove_file(&filename);
    Ok(body)
}

fn write_response(
    stream: &mut TcpStream,
    status: u16,
    content_type: &str,
    body: &[u8],
) -> Result<(), Box<dyn std::error::Error>> {
    let status_text = match status {
        200 => "OK",
        404 => "Not Found",
        405 => "Method Not Allowed",
        500 => "Internal Server Error",
        _ => "OK",
    };
    let header = format!(
        "HTTP/1.1 {} {}\r\nContent-Length: {}\r\nContent-Type: {}\r\nConnection: close\r\n\r\n",
        status,
        status_text,
        body.len(),
        content_type
    );
    stream.write_all(header.as_bytes())?;
    stream.write_all(body)?;
    Ok(())
}

fn split_target(target: &str) -> (&str, Option<&str>) {
    if let Some((path, query)) = target.split_once('?') {
        (path, Some(query))
    } else {
        (target, None)
    }
}

fn parse_seconds(query: Option<&str>) -> Option<u64> {
    query
        .and_then(|q| {
            q.split('&').find_map(|pair| {
                let (key, value) = pair.split_once('=')?;
                if key == "seconds" {
                    Some(value)
                } else {
                    None
                }
            })
        })
        .and_then(|value| value.parse::<u64>().ok())
}

fn ensure_jemalloc_profiling() {
    // Best-effort: try to activate runtime profiling. If jemalloc was built
    // without profiling, mallctl will return an error and heap endpoint will
    // later surface a clearer message.
    let _ = unsafe { raw::write(b"prof.active\0", true) };
}

fn profile_server_enabled() -> bool {
    matches!(
        env::var("PROFILE_SERVER_ENABLE")
            .unwrap_or_default()
            .to_lowercase()
            .as_str(),
        "1" | "true" | "yes" | "on"
    )
}

fn attach_mqtt_sources(
    pipeline: &mut ProcessorPipeline,
    broker_url: &str,
    topic: &str,
    qos: u8,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut attached = false;
    for processor in pipeline.middle_processors.iter_mut() {
        if let PlanProcessor::DataSource(ds) = processor {
            let source_id = ds.id().to_string();
            let config = MqttSourceConfig::new(
                source_id.clone(),
                broker_url.to_string(),
                topic.to_string(),
                qos,
            );
            let connector =
                MqttSourceConnector::new(format!("{source_id}_source_connector"), config);
            let decoder = Arc::new(JsonDecoder::new(source_id));
            ds.add_connector(Box::new(connector), decoder);
            attached = true;
        }
    }

    if attached {
        Ok(())
    } else {
        Err("no datasource processors available to attach MQTT source".into())
    }
}
