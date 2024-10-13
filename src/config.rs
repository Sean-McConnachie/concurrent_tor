use crate::{execution::scheduler::PlatformT, Result};
use serde::Deserialize;
use std::{collections::HashMap, hash::Hash};

#[derive(Deserialize, Clone, Debug)]
pub struct WorkerConfig {
    pub target_circulation: u32,
    pub http_workers: u16,
    pub headless_browser_workers: u16,
    pub headed_browser_workers: u16,
    pub driver_fp: String,
    pub socks_start_port: u16,
    pub driver_start_port: u16,
}

/// Configuration for every platform
#[derive(Deserialize, Clone, Debug)]
pub struct HttpPlatformConfig {
    /// Maximum number of requests per IP
    pub max_requests: u32,
    /// Rate limiting in seconds
    pub timeout_ms: u32,
}

/// Configuration for the headless browser platform
#[derive(Deserialize, Clone, Debug)]
pub struct HeadlessBrowserPlatformConfig {
    /// Maximum number of requests per IP
    pub max_requests: u32,
    /// Rate limiting in seconds
    pub timeout_ms: u32,
}

/// Configuration for the browser platform
#[derive(Deserialize, Clone, Debug)]
pub struct BrowserPlatformConfig {
    /// Maximum number of requests per IP
    pub max_requests: u32,
    /// Rate limiting in seconds
    pub timeout_ms: u32,
    /// Headless browser configuration
    pub headless: bool,
}

#[derive(Deserialize, Debug)]
pub struct ScraperConfig<P>
where
    P: 'static + Hash + Eq,
{
    pub workers: WorkerConfig,
    pub http_platforms: HashMap<P, HttpPlatformConfig>,
    pub browser_platforms: HashMap<P, BrowserPlatformConfig>,
}

impl<P> ScraperConfig<P>
where
    P: PlatformT + Eq + Hash,
{
    pub fn init<T: AsRef<std::path::Path>>(path: T) -> Result<Self> {
        let config = std::fs::read_to_string(&path).map_err(|e| {
            format!(
                "Could not read the configuration file at path: {:?}, error: {}",
                path.as_ref().display(),
                e
            )
        })?;
        let config: ScraperConfig<P> = toml::from_str(&config)?;
        Ok(config)
    }
}
