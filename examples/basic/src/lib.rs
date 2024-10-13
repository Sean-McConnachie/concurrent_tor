pub mod cron;
pub mod headed_browser;
pub mod headless_browser;
pub mod http;
pub mod monitor;

use concurrent_tor::{
    execution::scheduler::{PlatformT, WorkerRequest},
    Result,
};
use serde::{Deserialize, Serialize};
use strum::{EnumIter, FromRepr};

#[cfg(feature = "use_tor_backend")]
pub mod backend {
    use concurrent_tor::{
        execution::client::{CTorClient, MainCTorClient},
        exports::TorClientConfig,
        Result,
    };

    pub type ClientBackend = CTorClient;
    pub type MainClientBackend = MainCTorClient;

    pub async fn build_main_client() -> Result<MainClientBackend> {
        MainCTorClient::new(TorClientConfig::default()).await
    }
}

#[cfg(not(feature = "use_tor_backend"))]
pub mod backend {
    use concurrent_tor::{
        execution::client::{CStandardClient, MainCStandardClient},
        Result,
    };

    pub type ClientBackend = CStandardClient;
    pub type MainClientBackend = MainCStandardClient;

    pub async fn build_main_client() -> Result<MainClientBackend> {
        Ok(MainCStandardClient::new())
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, Copy, Hash, Eq, PartialEq, FromRepr, EnumIter)]
pub enum Platform {
    MyHttp,
    MyHeadlessBrowser,
    MyHeadedBrowser,
}

impl PlatformT for Platform {
    fn request_from_json(&self, json: &str) -> Result<Box<dyn WorkerRequest>> {
        match self {
            Platform::MyHttp => http::MyHttpRequest::from_json(json),
            Platform::MyHeadlessBrowser => {
                headless_browser::MyHeadlessBrowserRequest::from_json(json)
            }
            Platform::MyHeadedBrowser => headed_browser::MyHeadedBrowserRequest::from_json(json),
        }
    }

    fn to_repr(&self) -> usize {
        *self as usize
    }

    fn from_repr(repr: usize) -> Self {
        Self::from_repr(repr).unwrap()
    }
}
