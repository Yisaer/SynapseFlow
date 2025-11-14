use once_cell::sync::Lazy;
use prometheus::{register_int_gauge, IntGauge};

pub static CPU_USAGE_GAUGE: Lazy<IntGauge> = Lazy::new(|| {
    register_int_gauge!("cpu_usage", "CPU usage in percentage").expect("create cpu usage gauge")
});

pub static MEMORY_USAGE_GAUGE: Lazy<IntGauge> = Lazy::new(|| {
    register_int_gauge!("memory_usage_bytes", "Resident memory usage in bytes")
        .expect("create memory usage gauge")
});
