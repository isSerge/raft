use std::time::Duration;

pub const DEFAULT_HEARTBEAT_INTERVAL: Duration = Duration::from_millis(100);
pub const DEFAULT_ELECTION_TIMEOUT_MIN: Duration = Duration::from_millis(150);
pub const DEFAULT_ELECTION_TIMEOUT_MAX: Duration = Duration::from_millis(300);

#[derive(Debug, Clone)]
pub struct Config {
    pub heartbeat_interval: Duration,
    pub election_timeout_min: Duration,
    pub election_timeout_max: Duration,
    pub node_count: usize,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            node_count: 1,
            heartbeat_interval: DEFAULT_HEARTBEAT_INTERVAL,
            election_timeout_min: DEFAULT_ELECTION_TIMEOUT_MIN,
            election_timeout_max: DEFAULT_ELECTION_TIMEOUT_MAX,
        }
    }
}
