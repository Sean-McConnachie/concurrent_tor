use crate::server::ServerEvent;
use concurrent_tor::{
    config::{BrowserPlatformConfig, HttpPlatformConfig, ScraperConfig, WorkerConfig},
    execution::{
        runtime::CTRuntime,
        scheduler::{PlatformT, SimpleScheduler, WorkerRequest},
    },
    exports::StrumFromRepr,
    *,
};
use local_client::*;
use log::info;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

const SERVER_ADDR: (&str, u16) = ("127.0.0.1", 8080);
const SERVER_WORKERS: usize = 4;

macro_rules! hashmap {
    () => {
        std::collections::HashMap::new()
    };

    ($($key:expr => $val:expr),* $(,)?) => {
        {
            let mut map = std::collections::HashMap::new();
            $(
                map.insert($key, $val);
            )*
            map
        }
    };
}

#[allow(dead_code)]
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

#[allow(dead_code)]
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
    use super::{Platform, SERVER_ADDR};
    use crate::browser::IpBrowserRequest;
    use crate::http::IpHttpRequest;
    use concurrent_tor::execution::scheduler::NotRequested;
    use concurrent_tor::exports::AsyncChannelSender;
    use concurrent_tor::{
        execution::{
            cron::{CronPlatform, CronPlatformBuilder},
            scheduler::{Job, QueueJob},
        },
        exports::async_trait,
        Result,
    };
    use log::info;
    use std::sync::atomic::AtomicBool;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::time::sleep;

    pub struct Cron {
        sleep_ms: u64,
        queue_job: AsyncChannelSender<QueueJob<Platform>>,
        stop_flag: Arc<AtomicBool>,
    }

    impl Cron {
        fn build_http_job(&self) -> Job<NotRequested, Platform> {
            let req = IpHttpRequest {
                url: format!("http://{}:{}/post", SERVER_ADDR.0, SERVER_ADDR.1),
            };
            Job::init_from_box(Platform::IpHttp, req, 3)
        }

        fn build_browser_job(&self) -> Job<NotRequested, Platform> {
            let req = IpBrowserRequest {
                url: format!("http://{}:{}/get/", SERVER_ADDR.0, SERVER_ADDR.1),
            };
            Job::init(Platform::IpBrowser, Box::new(req), 3)
        }
    }

    #[async_trait]
    impl CronPlatform<Platform> for Cron {
        async fn start(self: Box<Self>) -> Result<()> {
            loop {
                if self.stop_flag.load(std::sync::atomic::Ordering::Relaxed) {
                    break;
                }

                let job = QueueJob::New(self.build_http_job());
                // println!("HttpCron job: {:?}", job);
                self.queue_job.send(job).await.expect("Failed to send job");
                info!("IpCron job sent");

                sleep(Duration::from_millis(self.sleep_ms)).await;

                let job = QueueJob::New(self.build_browser_job());
                self.queue_job.send(job).await.expect("Failed to send job");
                info!("IpCron job sent");

                sleep(Duration::from_millis(self.sleep_ms)).await;
            }
            Ok(())
        }
    }

    pub struct MyCronBuilder {
        sleep_ms: u64,
        queue_job: Option<AsyncChannelSender<QueueJob<Platform>>>,
        stop_flag: Option<Arc<AtomicBool>>,
    }

    impl MyCronBuilder {
        pub fn new(sleep_ms: u64) -> Self {
            Self {
                sleep_ms,
                queue_job: None,
                stop_flag: None,
            }
        }
    }

    impl CronPlatformBuilder<Platform> for MyCronBuilder {
        fn set_queue_job(&mut self, queue_job: AsyncChannelSender<QueueJob<Platform>>) {
            self.queue_job = Some(queue_job);
        }

        fn set_stop_flag(&mut self, stop_flag: Arc<AtomicBool>) {
            self.stop_flag = Some(stop_flag);
        }

        fn build(&self) -> Box<dyn CronPlatform<Platform>> {
            Box::new(Cron {
                sleep_ms: self.sleep_ms,
                queue_job: self.queue_job.clone().unwrap(),
                stop_flag: self.stop_flag.clone().unwrap(),
            })
        }
    }
}

mod http {
    use super::{ClientBackend, Platform};
    use crate::server::JobPost;
    use concurrent_tor::execution::client::Client;
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

        fn hash(&self) -> Result<u128> {
            Ok(murmur3_x64_128(&mut Cursor::new(&self.url), 0)?)
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
            // println!("IpHttp job response: {:?}", job);
            let job_post = JobPost {
                hash: job.hash().expect("Unable to hash").to_string(),
            };
            let body = Some(json_to_string(&job_post).unwrap());
            client
                .make_request(
                    HttpMethod::POST,
                    &job.url,
                    Some(hashmap!(
                        "Content-Type".to_string() => "application/json".to_string()
                    )),
                    body,
                )
                .await
                .unwrap();
            vec![]
        }
    }

    pub struct MyHttpBuilder {}

    impl MyHttpBuilder {
        pub fn new() -> Self {
            Self {}
        }
    }

    impl HttpPlatformBuilder<Platform, ClientBackend> for MyHttpBuilder {
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

        fn hash(&self) -> Result<u128> {
            Ok(murmur3_x64_128(&mut Cursor::new(&self.url), 0)?)
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
            // println!("IpBrowser job response: {:?}", job);
            let url = job.url.clone();
            let url = format!("{}{}", url, job.hash().unwrap());
            let handle = tokio::task::spawn_blocking(move || {
                tab.navigate_to(&url).unwrap();
                tab.wait_until_navigated().unwrap();
                let _r = tab.get_content().unwrap();
                // println!("Page content: {:?}", r);
            });
            handle.await.unwrap();
            vec![]
        }
    }

    pub struct MyBrowserBuilder {}

    impl MyBrowserBuilder {
        pub fn new() -> Self {
            Self {}
        }
    }

    impl BrowserPlatformBuilder<Platform> for MyBrowserBuilder {
        fn platform(&self) -> Platform {
            Platform::IpBrowser
        }

        fn build(&self) -> Box<dyn BrowserPlatform<Platform>> {
            Box::new(IpBrowser::new())
        }
    }
}

mod monitor {
    use crate::server::ServerEvent;
    use crate::Platform;
    use async_channel::Receiver;
    use concurrent_tor::exports::AsyncChannelReceiver;
    use concurrent_tor::{
        execution::monitor::{Event, Monitor},
        exports::async_trait,
    };

    pub struct MyMonitor {
        server_event: Receiver<ServerEvent>,
    }

    impl MyMonitor {
        pub fn new(server_event: Receiver<ServerEvent>) -> Self {
            MyMonitor { server_event }
        }

        async fn start_server_recv(server_event: Receiver<ServerEvent>) {
            loop {
                let server_event = server_event
                    .recv()
                    .await
                    .expect("Failed to receive event from the server");
                if let ServerEvent::StopServer = server_event {
                    break;
                }
                println!("Server event: {:?}", server_event);
            }
        }

        async fn start_ct_recv(event_rx: AsyncChannelReceiver<Event<Platform>>) {
            loop {
                let ct_event = event_rx
                    .recv()
                    .await
                    .expect("Failed to receive event from the scheduler");
                if let Event::StopMonitor = ct_event {
                    break;
                }
                println!("Monitor event: {:?}", ct_event);
            }
        }
    }

    #[async_trait]
    impl Monitor<Platform> for MyMonitor {
        async fn start(
            self,
            event_rx: AsyncChannelReceiver<Event<Platform>>,
        ) -> concurrent_tor::Result<()> {
            let _server_handle =
                tokio::task::spawn(MyMonitor::start_server_recv(self.server_event.clone()));
            let ct_handle = tokio::task::spawn(MyMonitor::start_ct_recv(event_rx.clone()));
            // server_handle.await?;
            ct_handle.await?;
            Ok(())
        }
    }
}

mod server {
    use actix_web::dev::ServerHandle;
    use actix_web::http::StatusCode;
    use actix_web::{get, post, web, App, HttpServer, Responder};
    use async_channel::{Receiver, Sender};
    use log::info;
    use rand::prelude::SliceRandom;
    use rand::rngs::OsRng;
    use serde::{Deserialize, Serialize};
    use tokio::task::JoinHandle;

    #[allow(dead_code)]
    #[derive(Debug)]
    pub struct JobInfo {
        pub hash: u128,
        pub ts: quanta::Instant,
    }

    impl JobInfo {
        pub fn new(hash: u128) -> Self {
            Self {
                hash,
                ts: quanta::Instant::now(),
            }
        }
    }

    #[allow(dead_code)]
    #[derive(Debug)]
    pub enum ServerEvent {
        InfoSuccess(JobInfo),
        GetSuccess(JobInfo),
        InfoFailure(JobInfo),
        GetFailure(JobInfo),

        StopServer,
    }

    #[derive(Deserialize, Serialize, Debug)]
    pub struct JobPost {
        pub hash: String,
    }

    fn rand_success() -> bool {
        rand::random()
    }

    /// Only called by the the browser client
    #[get("/get/{job_hash}")]
    async fn get_job(
        job_hash: web::Path<String>,
        events: web::Data<Sender<ServerEvent>>,
    ) -> String {
        info!("Received GET request for job hash: {}", job_hash);
        let job_hash: u128 = job_hash.parse().unwrap();
        let job_info = JobInfo::new(job_hash.clone());
        if rand_success() {
            events
                .send(ServerEvent::GetSuccess(job_info))
                .await
                .unwrap();
            format!("Job hash: {}", job_hash)
        } else {
            events
                .send(ServerEvent::GetFailure(job_info))
                .await
                .unwrap();
            format!("Failed to get job hash: {}", job_hash)
        }
    }

    /// Only called by the the http client
    #[post("/post")]
    async fn post_job(
        job: web::Json<JobPost>,
        events: web::Data<Sender<ServerEvent>>,
    ) -> impl Responder {
        info!("Received POST request for job hash: {}", job.hash);
        let job_hash: u128 = job.hash.parse().unwrap();
        let job_info = JobInfo::new(job_hash.clone());
        if rand_success() {
            events
                .send(ServerEvent::InfoSuccess(job_info))
                .await
                .unwrap();
            let choices = vec![StatusCode::OK, StatusCode::CREATED, StatusCode::ACCEPTED];
            (
                "success".to_string(),
                choices.choose(&mut OsRng::default()).unwrap().clone(),
            )
        } else {
            events
                .send(ServerEvent::InfoFailure(job_info))
                .await
                .unwrap();
            let choices = vec![
                StatusCode::BAD_REQUEST,
                StatusCode::FORBIDDEN,
                StatusCode::NOT_FOUND,
                StatusCode::INTERNAL_SERVER_ERROR,
            ];
            (
                "fail".to_string(),
                choices.choose(&mut OsRng::default()).unwrap().clone(),
            )
        }
    }

    pub async fn start_server(
        addr: (&str, u16),
        workers: usize,
    ) -> std::io::Result<(
        ServerHandle,
        JoinHandle<std::io::Result<()>>,
        Receiver<ServerEvent>,
        Sender<ServerEvent>,
    )> {
        let (event_tx, event_rx) = async_channel::unbounded::<ServerEvent>();
        let event_tx_clone = event_tx.clone();
        let server = HttpServer::new(move || {
            App::new()
                .service(get_job)
                .service(post_job)
                .app_data(web::Data::new(event_tx_clone.clone()))
        })
        .bind(addr)?
        .workers(workers)
        .run();
        let handle = server.handle();
        let join = tokio::task::spawn(server);
        Ok((handle, join, event_rx, event_tx))
    }
}

async fn my_main() -> Result<()> {
    dotenv::dotenv().ok();
    tracing_subscriber::fmt::init();
    info!("Starting up");

    let db_path = PathBuf::from("concurrent_tor.sqlite3");
    if db_path.exists() {
        std::fs::remove_file(&db_path)?;
    }

    let (server_handle, server_join, server_events_rx, server_events_tx) =
        server::start_server(SERVER_ADDR, SERVER_WORKERS).await?;

    let send_req_timeout_ms = 500;
    let ct_config = ScraperConfig {
        workers: WorkerConfig {
            target_circulation: 10,
            http_workers: 1,
            browser_workers: 1,
        },
        http_platforms: hashmap!(
            Platform::IpHttp => HttpPlatformConfig {
                max_requests: 10,
                timeout_ms: 0,
            }
        ),
        browser_platforms: hashmap!(
            Platform::IpBrowser => BrowserPlatformConfig {
                max_requests: 10,
                timeout_ms: 0,
            }
        ),
    };
    let ct = CTRuntime::run(
        ct_config.workers,
        monitor::MyMonitor::new(server_events_rx),
        SimpleScheduler::new(),
        build_main_client().await?,
        vec![cron_box(cron::MyCronBuilder::new(send_req_timeout_ms))],
        ct_config.http_platforms,
        vec![http_box(http::MyHttpBuilder::new())],
        ct_config.browser_platforms,
        vec![browser_box(browser::MyBrowserBuilder::new())],
    )
    .await?;

    let stop = ct.graceful_stop();
    tokio::task::spawn(async move {
        tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;
        stop().await.expect("Failed to stop runtime");
    });

    ct.join().await?;
    server_events_tx.send(ServerEvent::StopServer).await?;
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    server_handle.stop(true).await;
    server_join.await??;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    my_main().await
}
