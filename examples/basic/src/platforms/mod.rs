pub mod ip_browser;
pub mod ip_cron;
pub mod ip_http;

use concurrent_tor::{
    execution::scheduler::{PlatformT, WorkerRequest},
    exports::StrumFromRepr,
    Result,
};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone, Copy, Hash, Eq, PartialEq, StrumFromRepr)]
pub enum Platform {
    IpHttp,
    IpBrowser,
}

impl PlatformT for Platform {
    fn request_from_json(&self, json: &str) -> Result<Box<dyn WorkerRequest>> {
        match self {
            Platform::IpHttp => ip_http::IpHttpRequest::from_json(json),
            Platform::IpBrowser => ip_browser::IpBrowserRequest::from_json(json),
        }
    }

    fn to_repr(&self) -> usize {
        *self as usize
    }

    fn from_repr(repr: usize) -> Self {
        Self::from_repr(repr).unwrap()
    }
}
