use concurrent_tor::{
    config::ScraperConfig,
    execution::{
        runtime::run_scraper_runtime,
        scheduler::{PlatformT, SimpleScheduler, WorkerRequest},
    },
    exports::StrumFromRepr,
    *,
};
use local_client::*;
use log::info;
use serde::{Deserialize, Serialize};

mod local_client {
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

mod tor_client {
    use concurrent_tor::{
        execution::client::{CTorClient, MainCTorClient},
        exports::TorClientConfig,
        Result,
    };
    pub type Client = CTorClient;
    pub type MainClient = MainCTorClient;

    pub async fn build_main_client() -> Result<MainClient> {
        MainCTorClient::new(TorClientConfig::default()).await
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, Copy, Hash, Eq, PartialEq, StrumFromRepr)]
pub enum Platform {
    IpHttp,
    IpBrowser,
}

impl PlatformT for Platform {
    fn request_from_json(&self, json: &str) -> Result<Box<dyn WorkerRequest>> {
        match self {
            Platform::IpHttp => http::IpHttpRequest::from_json(json),
            Platform::IpBrowser => browser::IpBrowserRequest::from_json(json),
        }
    }

    fn to_repr(&self) -> usize {
        *self as usize
    }

    fn from_repr(repr: usize) -> Self {
        Self::from_repr(repr).unwrap()
    }
}

mod cron {
    use super::{browser::IpBrowserRequest, Platform};
    use concurrent_tor::{
        execution::{
            cron::CronPlatform,
            scheduler::{Job, QueueJob},
        },
        exports::{async_trait, CrossbeamSender},
        Result,
    };
    use std::time::Duration;
    use tokio::time::sleep;

    const IP_HTTP_URL: &str = "https://api.ipify.org?format=json";
    const IP_BROWSER_URL: &str = "https://whatismyipaddress.com/";

    pub struct IpCron {
        queue_job: Option<CrossbeamSender<QueueJob<Platform>>>,
    }

    impl IpCron {
        pub fn new() -> Self {
            Self { queue_job: None }
        }
    }

    #[async_trait]
    impl CronPlatform<Platform> for IpCron {
        fn set_queue_job(&mut self, queue_job: CrossbeamSender<QueueJob<Platform>>) {
            self.queue_job = Some(queue_job);
        }

        async fn start(self: Box<Self>) -> Result<()> {
            let queue_job = self.queue_job.unwrap();
            loop {
                let req = IpBrowserRequest {
                    url: IP_HTTP_URL.to_string(),
                };
                let job = Job::init(Platform::IpBrowser, Box::new(req));
                let job = QueueJob::New(job);
                queue_job.send(job)?;
                println!("IpCron job sent");
                sleep(Duration::from_millis(1000)).await;
            }
        }
    }
}

mod http {
    use super::{ClientBackend, Platform};
    use concurrent_tor::{
        execution::{
            http::{HttpPlatform, HttpPlatformBuilder},
            scheduler::{Job, NotRequested, QueueJob, WorkerRequest},
        },
        exports::*,
        Result,
    };
    use serde::{Deserialize, Serialize};
    use std::{any::Any, io::Cursor};

    #[derive(Debug, Serialize, Deserialize)]
    pub struct IpHttpRequest {
        pub url: String,
    }

    impl IpHttpRequest {
        pub fn from_json(json: &str) -> Result<Box<dyn WorkerRequest>> {
            let s: Result<Self> = json_from_str(json).map_err(|e| e.into());
            Ok(Box::new(s?))
        }
    }

    impl WorkerRequest for IpHttpRequest {
        fn as_any(&self) -> &dyn Any {
            self
        }

        fn hash(&self) -> Result<String> {
            Ok(murmur3_x64_128(&mut Cursor::new(&self.url), 0)?.to_string())
        }

        fn as_json(&self) -> String {
            json_to_string(self).unwrap()
        }
    }

    pub struct IpHttp {}

    impl IpHttp {
        pub fn new() -> Self {
            Self {}
        }
    }

    #[async_trait]
    impl HttpPlatform<Platform, ClientBackend> for IpHttp {
        async fn process_job(
            &self,
            job: Job<NotRequested, Platform>,
            client: &ClientBackend,
        ) -> Vec<QueueJob<Platform>> {
            let job: &IpHttpRequest = job.request.as_any().downcast_ref().unwrap();
            println!("IpHttp job response: {:?}", job);
            // client
            //     .make_request(HttpMethod::GET, &job.url, None)
            //     .await
            //     .unwrap();
            vec![]
        }
    }

    pub struct IpHttpBuilder {}

    impl IpHttpBuilder {
        pub fn new() -> Self {
            Self {}
        }
    }

    impl HttpPlatformBuilder<Platform, ClientBackend> for IpHttpBuilder {
        fn platform(&self) -> Platform {
            Platform::IpHttp
        }

        fn build(&self) -> Box<dyn HttpPlatform<Platform, ClientBackend>> {
            Box::new(IpHttp::new())
        }
    }
}

mod browser {
    use super::Platform;
    use concurrent_tor::{
        execution::{
            browser::{BrowserPlatform, BrowserPlatformBuilder},
            scheduler::{Job, NotRequested, QueueJob, WorkerRequest},
        },
        exports::*,
        Result,
    };
    use serde::{Deserialize, Serialize};
    use std::sync::Arc;
    use std::{any::Any, io::Cursor};

    #[derive(Serialize, Deserialize, Debug)]
    pub struct IpBrowserRequest {
        pub url: String,
    }

    impl IpBrowserRequest {
        pub fn from_json(json: &str) -> Result<Box<dyn WorkerRequest>> {
            let s: Result<Self> = json_from_str(json).map_err(|e| e.into());
            Ok(Box::new(s?))
        }
    }

    impl WorkerRequest for IpBrowserRequest {
        fn as_any(&self) -> &dyn Any {
            self
        }

        fn hash(&self) -> Result<String> {
            Ok(murmur3_x64_128(&mut Cursor::new(&self.url), 0)?.to_string())
        }

        fn as_json(&self) -> String {
            json_to_string(self).unwrap()
        }
    }

    pub struct IpBrowser {}

    impl IpBrowser {
        pub fn new() -> Self {
            Self {}
        }
    }

    #[async_trait]
    impl BrowserPlatform<Platform> for IpBrowser {
        async fn process_job(
            &self,
            job: Job<NotRequested, Platform>,
            tab: Arc<headless_chrome::Tab>,
        ) -> Vec<QueueJob<Platform>> {
            let job: &IpBrowserRequest = job.request.as_any().downcast_ref().unwrap();
            println!("IpBrowser job response: {:?}", job);
            let url = job.url.clone();
            let handle = tokio::task::spawn_blocking(move || {
                tab.navigate_to(&url).unwrap();
                tab.wait_until_navigated().unwrap();
                let r = tab.get_content().unwrap();
                println!("Page content: {:?}", r);
            });
            handle.await.unwrap();
            vec![]
        }
    }

    pub struct IpBrowserBuilder {}

    impl IpBrowserBuilder {
        pub fn new() -> Self {
            Self {}
        }
    }

    impl BrowserPlatformBuilder<Platform> for IpBrowserBuilder {
        fn platform(&self) -> Platform {
            Platform::IpBrowser
        }

        fn build(&self) -> Box<dyn BrowserPlatform<Platform>> {
            Box::new(IpBrowser::new())
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenv::dotenv().ok();
    tracing_subscriber::fmt::init();
    info!("Starting up");

    let config = ScraperConfig::init("Config.toml")?;
    run_scraper_runtime(
        config.workers,
        SimpleScheduler::new(),
        build_main_client().await?,
        vec![cron_box(cron::IpCron::new())],
        config.http_platforms,
        vec![http_box(http::IpHttpBuilder::new())],
        config.browser_platforms,
        vec![browser_box(browser::IpBrowserBuilder::new())],
    )
    .await
}
