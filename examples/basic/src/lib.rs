pub mod cron;
pub mod headed_browser;
pub mod headless_browser;
pub mod http;
pub mod monitor;

use concurrent_tor::{build_platform_enum, impl_platform_return_t, execution::scheduler::{PlatformT, PlatformReturnT, WorkerRequest}, Result};
use serde::{Deserialize, Serialize};
use strum::{EnumIter, FromRepr};
use concurrent_tor::exports::json_from_str;

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

build_platform_enum!(
    Platform,
    {
        MyHttp => http::MyHttpRequest,
        MyHeadlessBrowser => headless_browser::MyHeadlessBrowserRequest,
        MyHeadedBrowser => headed_browser::MyHeadedBrowserRequest
    },
    {
        http::MyHttpBuilder => [MyHttp],
        headless_browser::MyHeadlessBrowserBuilder => [MyHeadlessBrowser],
        headed_browser::MyHeadedBrowserBuilder => [MyHeadedBrowser]
    }
);
