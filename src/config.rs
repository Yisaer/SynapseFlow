use crate::server::ServerOptions;
use serde::Deserialize;
use std::fs;
use std::path::Path;

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct AppConfig {
    pub profiling: ProfilingConfig,
    pub metrics: MetricsConfig,
    pub server: ServerConfig,
}

impl Default for AppConfig {
    fn default() -> Self {
        Self {
            profiling: ProfilingConfig {
                enabled: None,
                addr: None,
            },
            metrics: MetricsConfig {
                addr: None,
                poll_interval_secs: None,
            },
            server: ServerConfig::default(),
        }
    }
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct ProfilingConfig {
    pub enabled: Option<bool>,
    pub addr: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct MetricsConfig {
    pub addr: Option<String>,
    pub poll_interval_secs: Option<u64>,
}

impl Default for MetricsConfig {
    fn default() -> Self {
        Self {
            addr: Some("0.0.0.0:9898".to_string()),
            poll_interval_secs: Some(15),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct ServerConfig {
    pub manager_addr: Option<String>,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            manager_addr: Some(crate::server::DEFAULT_MANAGER_ADDR.to_string()),
        }
    }
}

impl AppConfig {
    pub fn load_required(
        path: impl AsRef<Path>,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let path = path.as_ref();
        let raw = fs::read_to_string(path)
            .map_err(|err| format!("failed to read config file {}: {}", path.display(), err))?;
        let cfg: AppConfig = serde_yaml::from_str(&raw)
            .map_err(|err| format!("failed to parse yaml config {}: {}", path.display(), err))?;
        Ok(cfg)
    }

    pub fn load_optional(
        path: impl AsRef<Path>,
    ) -> Result<Option<Self>, Box<dyn std::error::Error + Send + Sync>> {
        let path = path.as_ref();
        if !path.exists() {
            return Ok(None);
        }
        Ok(Some(Self::load_required(path)?))
    }

    pub fn apply_to_server_options(&self, opts: &mut ServerOptions) {
        if let Some(enabled) = self.profiling.enabled {
            opts.profiling_enabled = Some(enabled);
        }
        if let Some(addr) = self.profiling.addr.as_ref() {
            opts.profile_addr = Some(addr.clone());
        }
        if let Some(addr) = self.metrics.addr.as_ref() {
            opts.metrics_addr = Some(addr.clone());
        }
        if let Some(secs) = self.metrics.poll_interval_secs {
            opts.metrics_poll_interval_secs = Some(secs);
        }
        if let Some(addr) = self.server.manager_addr.as_ref() {
            opts.manager_addr = Some(addr.clone());
        }
    }

    pub fn to_server_options(&self) -> ServerOptions {
        let mut opts = ServerOptions::default();
        self.apply_to_server_options(&mut opts);
        opts
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn unique_temp_path(name: &str) -> std::path::PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        std::env::temp_dir().join(format!("synapse_flow_test.{}.{}.yaml", name, nanos))
    }

    #[test]
    fn loads_optional_missing_file() {
        let path = unique_temp_path("missing");
        let loaded = AppConfig::load_optional(&path).unwrap();
        assert!(loaded.is_none());
    }

    #[test]
    fn applies_only_present_fields() {
        let yaml = r#"
profiling:
  enabled: true
server:
  manager_addr: "127.0.0.1:9999"
"#;
        let path = unique_temp_path("apply");
        std::fs::write(&path, yaml).unwrap();

        let cfg = AppConfig::load_required(&path).unwrap();
        let mut opts = ServerOptions::default();
        cfg.apply_to_server_options(&mut opts);

        assert_eq!(opts.profiling_enabled, Some(true));
        assert_eq!(opts.manager_addr.as_deref(), Some("127.0.0.1:9999"));
        assert!(opts.profile_addr.is_none());
        assert!(opts.metrics_addr.is_none());
        assert!(opts.metrics_poll_interval_secs.is_none());

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn loads_addresses_and_intervals() {
        let yaml = r#"
profiling:
  addr: "127.0.0.1:6060"
metrics:
  addr: "127.0.0.1:9898"
  poll_interval_secs: 2
"#;
        let path = unique_temp_path("addr");
        std::fs::write(&path, yaml).unwrap();

        let cfg = AppConfig::load_required(&path).unwrap();
        let opts = cfg.to_server_options();
        assert_eq!(opts.profile_addr.as_deref(), Some("127.0.0.1:6060"));
        assert_eq!(opts.metrics_addr.as_deref(), Some("127.0.0.1:9898"));
        assert_eq!(opts.metrics_poll_interval_secs, Some(2));

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn default_manager_addr_is_set() {
        let cfg = AppConfig::default();
        assert_eq!(
            cfg.server.manager_addr.as_deref(),
            Some(crate::server::DEFAULT_MANAGER_ADDR)
        );
    }
}
