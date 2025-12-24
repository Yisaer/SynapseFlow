#[cfg(all(feature = "profiling", not(target_env = "msvc")))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

use synapse_flow::server::{self};

#[derive(Debug, Clone)]
struct CliFlags {
    data_dir: Option<String>,
    config_path: Option<String>,
}

impl CliFlags {
    fn parse() -> Self {
        let mut data_dir = None;
        let mut config_path = None;
        let mut args = std::env::args().skip(1).peekable();
        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--data-dir" => {
                    if let Some(val) = args.next() {
                        data_dir = Some(val);
                    }
                }
                "--config" => {
                    if let Some(val) = args.next() {
                        config_path = Some(val);
                    }
                }
                _ => {}
            }
        }
        Self {
            data_dir,
            config_path,
        }
    }

    fn data_dir(&self) -> Option<&str> {
        self.data_dir.as_deref()
    }

    fn config_path(&self) -> Option<&str> {
        self.config_path.as_deref()
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let cli_flags = CliFlags::parse();
    let (config, loaded_config_path) = if let Some(path) = cli_flags.config_path() {
        let cfg = synapse_flow::config::AppConfig::load_required(path)?;
        (cfg, Some(path.to_string()))
    } else {
        (synapse_flow::config::AppConfig::default(), None)
    };

    let _logging_guard = synapse_flow::logging::init_logging(&config.logging)?;
    if let Some(path) = loaded_config_path.as_deref() {
        tracing::info!(config_path = path, "loaded config");
    }
    tracing::info!(
        git_sha = build_info::git_sha(),
        git_tag = build_info::git_tag(),
        "build info"
    );

    let mut options = config.to_server_options();
    if let Some(dir) = cli_flags.data_dir() {
        options.data_dir = Some(dir.to_string());
    }

    // Prepare the FlowInstance so callers can register custom codecs/connectors.
    let instance = server::prepare_registry();
    // Inject custom registrations on `instance` here before starting, if needed.

    let ctx = server::init(options, instance).await?;
    server::start(ctx).await
}
