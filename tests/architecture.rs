use crate::{monitor::GlobalEvent, server::ServerEvent};
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
use std::{
    path::PathBuf,
    sync::{atomic::AtomicUsize, Arc},
};

const SERVER_ADDR: (&str, u16) = ("127.0.0.1", 8080);
const SERVER_WORKERS: usize = 4;
const SUCCESS_MESSAGE: &str = "success1231455 43gdfuhgiudf hgdifughdfui";
const FAILURE_MESSAGE: &str = "fail1231455 asdiuasyhdiuashduiasf hgdifughdfui";

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
    MyHttp,
    MyBrowser,
}

impl PlatformT for Platform {
    fn request_from_json(&self, json: &str) -> Result<Box<dyn WorkerRequest>> {
        match self {
            Platform::MyHttp => http::MyHttpRequest::from_json(json),
            Platform::MyBrowser => browser::MyBrowserRequest::from_json(json),
        }
    }

    fn to_repr(&self) -> usize {
        *self as usize
    }

    fn from_repr(repr: usize) -> Self {
        Self::from_repr(repr).unwrap()
    }
}

pub type EventInstant = quanta::Instant;
pub type JobHash = u128;
pub type Success = bool;

#[derive(Debug, Clone, Copy)]
pub enum ImplementationEvent {
    CronSendHttp((EventInstant, JobHash)),
    CronSendBrowser((EventInstant, JobHash)),

    MadeHttpRequest((EventInstant, JobHash, Success)),
    MadeBrowserRequest((EventInstant, JobHash, Success)),

    StopServer,
}

mod cron {
    use super::{ImplementationEvent, JobHash, Platform, SERVER_ADDR};
    use crate::{browser::MyBrowserRequest, http::MyHttpRequest};
    use async_channel::Sender;
    use concurrent_tor::{
        execution::{
            cron::{CronPlatform, CronPlatformBuilder},
            scheduler::{Job, NotRequested, QueueJob, WorkerRequest},
        },
        exports::{async_trait, AsyncChannelSender},
        Result,
    };
    use log::info;
    use std::{
        sync::{
            atomic::{AtomicBool, AtomicUsize},
            Arc,
        },
        time::Duration,
    };
    use tokio::time::sleep;

    pub struct Cron {
        job_count: Arc<AtomicUsize>,
        sleep_ms: u64,
        use_http: bool,
        use_browser: bool,
        events: Sender<ImplementationEvent>,
        queue_job: AsyncChannelSender<QueueJob<Platform>>,
        stop_flag: Arc<AtomicBool>,
    }

    impl Cron {
        fn build_http_job(&self) -> (JobHash, Job<NotRequested, Platform>) {
            let req =
                MyHttpRequest::new(format!("http://{}:{}/post", SERVER_ADDR.0, SERVER_ADDR.1));
            let hash = req.hash().expect("Unable to hash");
            (hash, Job::init_from_box(Platform::MyHttp, req, 3))
        }

        fn build_browser_job(&self) -> (JobHash, Job<NotRequested, Platform>) {
            let req =
                MyBrowserRequest::new(format!("http://{}:{}/get/", SERVER_ADDR.0, SERVER_ADDR.1));
            let hash = req.hash().expect("Unable to hash");
            (hash, Job::init(Platform::MyBrowser, Box::new(req), 3))
        }
    }

    #[async_trait]
    impl CronPlatform<Platform> for Cron {
        async fn start(self: Box<Self>) -> Result<()> {
            info!(
                "Starting cron. Http enabled: {}, Browser enabled: {}",
                self.use_http, self.use_browser
            );
            loop {
                if self.stop_flag.load(std::sync::atomic::Ordering::Relaxed) {
                    break;
                }

                if self.use_http {
                    let (hash, job) = self.build_http_job();
                    let job = QueueJob::New(job);
                    self.queue_job.send(job).await.expect("Failed to send job");
                    self.events
                        .send(ImplementationEvent::CronSendHttp((
                            quanta::Instant::now(),
                            hash,
                        )))
                        .await
                        .expect("Failed to send event");
                    info!("Sent http job");
                    self.job_count
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    sleep(Duration::from_millis(self.sleep_ms)).await;
                }

                if self.use_browser {
                    let (hash, job) = self.build_browser_job();
                    let job = QueueJob::New(job);
                    self.queue_job.send(job).await.expect("Failed to send job");
                    self.events
                        .send(ImplementationEvent::CronSendBrowser((
                            quanta::Instant::now(),
                            hash,
                        )))
                        .await
                        .expect("Failed to send event");
                    info!("Sent browser job");
                    self.job_count
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    sleep(Duration::from_millis(self.sleep_ms)).await;
                }
            }
            Ok(())
        }
    }

    pub struct MyCronBuilder {
        job_count: Arc<AtomicUsize>,
        sleep_ms: u64,
        use_http: bool,
        use_browser: bool,
        events: Sender<ImplementationEvent>,
        queue_job: Option<AsyncChannelSender<QueueJob<Platform>>>,
        stop_flag: Option<Arc<AtomicBool>>,
    }

    impl MyCronBuilder {
        pub fn new(
            job_count: Arc<AtomicUsize>,
            sleep_ms: u64,
            use_http: bool,
            use_browser: bool,
            events: Sender<ImplementationEvent>,
        ) -> Self {
            Self {
                job_count,
                sleep_ms,
                use_http,
                use_browser,
                events,
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
                job_count: self.job_count.clone(),
                sleep_ms: self.sleep_ms,
                use_http: self.use_http,
                use_browser: self.use_browser,
                events: self.events.clone(),
                queue_job: self.queue_job.clone().unwrap(),
                stop_flag: self.stop_flag.clone().unwrap(),
            })
        }
    }
}

mod http {
    use super::{ClientBackend, ImplementationEvent, Platform};
    use crate::server::JobPost;
    use async_channel::Sender;
    use concurrent_tor::{
        execution::{
            client::Client,
            http::{HttpPlatform, HttpPlatformBuilder},
            scheduler::{Job, NotRequested, QueueJob, Requested, WorkerRequest},
        },
        exports::*,
        Result,
    };
    use serde::{Deserialize, Serialize};
    use std::any::Any;

    #[derive(Debug, Serialize, Deserialize, Clone)]
    pub struct MyHttpRequest {
        pub id: usize,
        pub url: String,
    }

    impl MyHttpRequest {
        pub fn new(url: impl Into<String>) -> Self {
            Self {
                id: rand::random(),
                url: url.into(),
            }
        }

        pub fn from_json(json: &str) -> Result<Box<dyn WorkerRequest>> {
            let s: Result<Self> = json_from_str(json).map_err(|e| e.into());
            Ok(Box::new(s?))
        }
    }

    impl WorkerRequest for MyHttpRequest {
        fn as_any(&self) -> &dyn Any {
            self
        }

        fn hash(&self) -> Result<u128> {
            Ok(self.id as u128)
        }

        fn as_json(&self) -> String {
            json_to_string(self).unwrap()
        }
    }

    pub struct MyHttp {
        events: Sender<ImplementationEvent>,
    }

    impl MyHttp {
        pub fn new(events: Sender<ImplementationEvent>) -> Self {
            Self { events }
        }
    }

    #[async_trait]
    impl HttpPlatform<Platform, ClientBackend> for MyHttp {
        async fn process_job(
            &self,
            job: &Job<NotRequested, Platform>,
            client: &ClientBackend,
        ) -> Vec<QueueJob<Platform>> {
            let req: &MyHttpRequest = job.request.as_any().downcast_ref().unwrap();
            // println!("IpHttp job response: {:?}", job);
            let job_post = JobPost {
                job_hash: req.hash().expect("Unable to hash").to_string(),
            };
            let body = Some(json_to_string(&job_post).unwrap());
            let resp = client
                .make_request(
                    HttpMethod::POST,
                    &req.url,
                    Some(hashmap!(
                        "Content-Type".to_string() => "application/json".to_string()
                    )),
                    body,
                )
                .await
                .unwrap();
            let completed: Job<Requested, Platform> = job.into();
            self.events
                .send(ImplementationEvent::MadeHttpRequest((
                    quanta::Instant::now(),
                    req.hash().expect("Unable to hash"),
                    resp.status.is_success(),
                )))
                .await
                .expect("Failed to send event");
            if resp.status.is_success() {
                vec![QueueJob::Completed(completed)]
            } else {
                vec![QueueJob::Retry(completed)]
            }
        }
    }

    pub struct MyHttpBuilder {
        events: Sender<ImplementationEvent>,
    }

    impl MyHttpBuilder {
        pub fn new(events: Sender<ImplementationEvent>) -> Self {
            Self { events }
        }
    }

    impl HttpPlatformBuilder<Platform, ClientBackend> for MyHttpBuilder {
        fn platform(&self) -> Platform {
            Platform::MyHttp
        }

        fn build(&self) -> Box<dyn HttpPlatform<Platform, ClientBackend>> {
            Box::new(MyHttp::new(self.events.clone()))
        }
    }
}

mod browser {
    use super::{ImplementationEvent, Platform, FAILURE_MESSAGE, SUCCESS_MESSAGE};
    use async_channel::Sender;
    use concurrent_tor::{
        execution::{
            browser::{BrowserPlatform, BrowserPlatformBuilder},
            scheduler::{Job, NotRequested, QueueJob, Requested, WorkerRequest},
        },
        exports::*,
        Result,
    };
    use serde::{Deserialize, Serialize};
    use std::{any::Any, sync::Arc};

    #[derive(Serialize, Deserialize, Debug, Clone)]
    pub struct MyBrowserRequest {
        pub id: usize,
        pub url: String,
    }

    impl MyBrowserRequest {
        pub fn new(url: impl Into<String>) -> Self {
            Self {
                id: rand::random(),
                url: url.into(),
            }
        }
        pub fn from_json(json: &str) -> Result<Box<dyn WorkerRequest>> {
            let s: Result<Self> = json_from_str(json).map_err(|e| e.into());
            Ok(Box::new(s?))
        }
    }

    impl WorkerRequest for MyBrowserRequest {
        fn as_any(&self) -> &dyn Any {
            self
        }

        fn hash(&self) -> Result<u128> {
            Ok(self.id as u128)
        }

        fn as_json(&self) -> String {
            json_to_string(self).unwrap()
        }
    }

    pub struct MyBrowser {
        events: Sender<ImplementationEvent>,
    }

    impl MyBrowser {
        pub fn new(events: Sender<ImplementationEvent>) -> Self {
            Self { events }
        }
    }

    #[async_trait]
    impl BrowserPlatform<Platform> for MyBrowser {
        async fn process_job(
            &self,
            job: &Job<NotRequested, Platform>,
            tab: Arc<headless_chrome::Tab>,
        ) -> Vec<QueueJob<Platform>> {
            let req: &MyBrowserRequest = job.request.as_any().downcast_ref().unwrap();
            // println!("IpBrowser job response: {:?}", job);
            let url = req.url.clone();
            let url = format!("{}{}", url, req.hash().unwrap());
            let handle = tokio::task::spawn_blocking(move || {
                tab.navigate_to(&url).unwrap();
                tab.wait_until_navigated().unwrap();
                let resp = tab.get_content().unwrap();
                resp
            });
            let r = handle.await.unwrap();
            let completed: Job<Requested, Platform> = job.into();
            let q_job = if r.contains(SUCCESS_MESSAGE) {
                self.events
                    .send(ImplementationEvent::MadeBrowserRequest((
                        quanta::Instant::now(),
                        req.hash().expect("Unable to hash"),
                        true,
                    )))
                    .await
                    .expect("Failed to send event");
                QueueJob::Completed(completed)
            } else if r.contains(FAILURE_MESSAGE) {
                self.events
                    .send(ImplementationEvent::MadeBrowserRequest((
                        quanta::Instant::now(),
                        req.hash().expect("Unable to hash"),
                        false,
                    )))
                    .await
                    .expect("Failed to send event");
                QueueJob::Retry(completed)
            } else {
                unreachable!("Unexpected response");
            };
            vec![q_job]
        }
    }

    pub struct MyBrowserBuilder {
        events: Sender<ImplementationEvent>,
    }

    impl MyBrowserBuilder {
        pub fn new(events: Sender<ImplementationEvent>) -> Self {
            Self { events }
        }
    }

    impl BrowserPlatformBuilder<Platform> for MyBrowserBuilder {
        fn platform(&self) -> Platform {
            Platform::MyBrowser
        }

        fn build(&self) -> Box<dyn BrowserPlatform<Platform>> {
            Box::new(MyBrowser::new(self.events.clone()))
        }
    }
}

mod monitor {
    use crate::{server::ServerEvent, EventInstant, ImplementationEvent, Platform};
    use async_channel::Receiver;
    use concurrent_tor::{
        execution::monitor::{Event, Monitor},
        exports::{async_trait, AsyncChannelReceiver},
    };
    use std::sync::Arc;
    use tokio::sync::Mutex;

    #[derive(Debug, Clone, Copy)]
    pub enum GlobalEvent {
        Server((EventInstant, ServerEvent)),
        Monitor((EventInstant, Event<Platform>)),
        Implementation((EventInstant, ImplementationEvent)),
    }

    impl GlobalEvent {
        pub fn instant(&self) -> EventInstant {
            match self {
                GlobalEvent::Server((instant, _)) => *instant,
                GlobalEvent::Monitor((instant, _)) => *instant,
                GlobalEvent::Implementation((instant, _)) => *instant,
            }
        }
    }

    pub struct MyMonitor {
        all_events: Arc<Mutex<Vec<GlobalEvent>>>,
        server_event: Receiver<ServerEvent>,
        implementation_events: Receiver<ImplementationEvent>,
    }

    impl MyMonitor {
        pub fn new(
            server_event: Receiver<ServerEvent>,
            implementation_events: Receiver<ImplementationEvent>,
        ) -> Self {
            MyMonitor {
                all_events: Arc::new(Mutex::new(Vec::new())),
                server_event,
                implementation_events,
            }
        }

        pub fn events(&self) -> Arc<Mutex<Vec<GlobalEvent>>> {
            self.all_events.clone()
        }

        async fn start_server_recv(
            all_events: Arc<Mutex<Vec<GlobalEvent>>>,
            server_event: Receiver<ServerEvent>,
        ) {
            loop {
                let server_event = server_event
                    .recv()
                    .await
                    .expect("Failed to receive event from the server");
                let instant = match &server_event {
                    ServerEvent::InfoSuccess(success) => success.ts,
                    ServerEvent::GetSuccess(success) => success.ts,
                    ServerEvent::InfoFailure(fail) => fail.ts,
                    ServerEvent::GetFailure(fail) => fail.ts,
                    ServerEvent::StopServer => {
                        break;
                    }
                };
                let event = GlobalEvent::Server((instant, server_event));
                all_events.lock().await.push(event);
            }
        }

        async fn start_ct_recv(
            all_events: Arc<Mutex<Vec<GlobalEvent>>>,
            event_rx: AsyncChannelReceiver<Event<Platform>>,
        ) {
            loop {
                let ct_event = event_rx
                    .recv()
                    .await
                    .expect("Failed to receive event from the scheduler");
                let instant = match &ct_event {
                    Event::ProcessedJob(process) => process.ts_end,
                    Event::WorkerRateLimited(limited) => limited.ts,
                    Event::WorkerRenewingClient(renew) => renew.ts,
                    Event::NewJob(new) => new.ts,
                    Event::CompletedJob(completed) => completed.ts,
                    Event::FailedJob(fail) => fail.ts,
                    Event::RetryJob(retry) => retry.ts,
                    Event::MaxAttemptsReached(max) => max.ts,
                    Event::BalanceCirculation(balance) => balance.ts,
                    Event::StopMonitor => {
                        break;
                    }
                };
                let event = GlobalEvent::Monitor((instant, ct_event));
                all_events.lock().await.push(event);
            }
        }

        async fn start_implementation_recv(
            all_events: Arc<Mutex<Vec<GlobalEvent>>>,
            implementation_events: Receiver<ImplementationEvent>,
        ) {
            loop {
                let implementation_event = implementation_events
                    .recv()
                    .await
                    .expect("Failed to receive event from the implementation");
                let instant = match &implementation_event {
                    ImplementationEvent::CronSendHttp((instant, _)) => *instant,
                    ImplementationEvent::CronSendBrowser((instant, _)) => *instant,
                    ImplementationEvent::MadeHttpRequest((instant, _, _)) => *instant,
                    ImplementationEvent::MadeBrowserRequest((instant, _, _)) => *instant,
                    ImplementationEvent::StopServer => {
                        break;
                    }
                };
                let event = GlobalEvent::Implementation((instant, implementation_event));
                all_events.lock().await.push(event);
            }
        }
    }

    #[async_trait]
    impl Monitor<Platform> for MyMonitor {
        async fn start(
            self,
            event_rx: AsyncChannelReceiver<Event<Platform>>,
        ) -> concurrent_tor::Result<()> {
            let _server_handle = tokio::task::spawn(MyMonitor::start_server_recv(
                self.all_events.clone(),
                self.server_event.clone(),
            ));
            let ct_handle = tokio::task::spawn(MyMonitor::start_ct_recv(
                self.all_events.clone(),
                event_rx.clone(),
            ));
            let _implementation_handle = tokio::task::spawn(MyMonitor::start_implementation_recv(
                self.all_events.clone(),
                self.implementation_events.clone(),
            ));
            // server_handle.await?;
            ct_handle.await?;
            Ok(())
        }
    }
}

mod server {
    use crate::{Platform, FAILURE_MESSAGE, SUCCESS_MESSAGE};
    use actix_web::{
        dev::ServerHandle, get, http::StatusCode, post, web, App, HttpServer, Responder,
    };
    use async_channel::{Receiver, Sender};
    use log::info;
    use rand::{prelude::SliceRandom, rngs::OsRng};
    use serde::{Deserialize, Serialize};
    use tokio::task::JoinHandle;

    #[allow(dead_code)]
    #[derive(Debug, Clone, Copy)]
    pub struct JobInfo {
        pub job_hash: u128,
        pub platform: Platform,
        pub ts: quanta::Instant,
    }

    impl JobInfo {
        pub fn new(hash: u128, platform: Platform) -> Self {
            Self {
                job_hash: hash,
                platform,
                ts: quanta::Instant::now(),
            }
        }
    }

    #[allow(dead_code)]
    #[derive(Debug, Clone, Copy)]
    pub enum ServerEvent {
        InfoSuccess(JobInfo),
        GetSuccess(JobInfo),
        InfoFailure(JobInfo),
        GetFailure(JobInfo),

        StopServer,
    }

    #[derive(Deserialize, Serialize, Debug)]
    pub struct JobPost {
        pub job_hash: String,
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
        let job_info = JobInfo::new(job_hash.clone(), Platform::MyBrowser);
        if rand_success() {
            events
                .send(ServerEvent::GetSuccess(job_info))
                .await
                .unwrap();
            SUCCESS_MESSAGE.to_string()
        } else {
            events
                .send(ServerEvent::GetFailure(job_info))
                .await
                .unwrap();
            FAILURE_MESSAGE.to_string()
        }
    }

    /// Only called by the the http client
    #[post("/post")]
    async fn post_job(
        job: web::Json<JobPost>,
        events: web::Data<Sender<ServerEvent>>,
    ) -> impl Responder {
        info!("Received POST request for job hash: {}", job.job_hash);
        let job_hash: u128 = job.job_hash.parse().unwrap();
        let job_info = JobInfo::new(job_hash.clone(), Platform::MyHttp);
        if rand_success() {
            events
                .send(ServerEvent::InfoSuccess(job_info))
                .await
                .unwrap();
            let choices = vec![StatusCode::OK, StatusCode::CREATED, StatusCode::ACCEPTED];
            (
                "Successfully posted job".to_string(),
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
                "Failed to post job".to_string(),
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

#[derive(Debug, Copy, Clone)]
struct ExecutionConfig {
    http_workers: u16,
    browser_workers: u16,
    send_req_timeout_ms: u64,
    max_requests: u32,
    timeout_ms: u32,
    shutdown_after_s: u64,
}

pub type EndInstant = quanta::Instant;

async fn my_main(
    rm_db: bool,
    disable_cron: bool,
    job_count: Arc<AtomicUsize>,
    server_addr: (&str, u16),
    server_workers: usize,
    execution_config: ExecutionConfig,
) -> Result<(EndInstant, Vec<GlobalEvent>)> {
    info!("Starting up with config: {:?}", execution_config);
    info!("rm_db: {}, disable_cron: {}", rm_db, disable_cron);

    let (
        http_workers,
        browser_workers,
        send_req_timeout_ms,
        max_requests,
        timeout_ms,
        shutdown_after_s,
    ) = (
        execution_config.http_workers,
        execution_config.browser_workers,
        execution_config.send_req_timeout_ms,
        execution_config.max_requests,
        execution_config.timeout_ms,
        execution_config.shutdown_after_s,
    );

    if rm_db {
        let db_path = PathBuf::from("concurrent_tor.sqlite3");
        if db_path.exists() {
            std::fs::remove_file(&db_path)?;
        }
    }

    let (server_handle, server_join, server_events_rx, server_events_tx) =
        server::start_server(server_addr, server_workers).await?;
    let (implementation_tx, implementation_rx) = async_channel::unbounded::<ImplementationEvent>();

    let ct_config = ScraperConfig {
        workers: WorkerConfig {
            target_circulation: ((http_workers + browser_workers) * 2) as u32,
            http_workers,
            headless_browser_workers: browser_workers,
        },
        http_platforms: hashmap!(
            Platform::MyHttp => HttpPlatformConfig {
                max_requests,
                timeout_ms,
            }
        ),
        browser_platforms: hashmap!(
            Platform::MyBrowser => BrowserPlatformConfig {
                max_requests,
                timeout_ms,
            }
        ),
    };

    let use_http = if !disable_cron {
        http_workers > 0
    } else {
        false
    };
    let use_browser = if !disable_cron {
        browser_workers > 0
    } else {
        false
    };

    let monitor = monitor::MyMonitor::new(server_events_rx, implementation_rx);
    let all_events = monitor.events();
    let ct = CTRuntime::run(
        ct_config.workers,
        monitor,
        SimpleScheduler::new(),
        build_main_client().await?,
        vec![cron_box(cron::MyCronBuilder::new(
            job_count.clone(),
            send_req_timeout_ms,
            use_http,
            use_browser,
            implementation_tx.clone(),
        ))],
        ct_config.http_platforms,
        vec![http_box(http::MyHttpBuilder::new(
            implementation_tx.clone(),
        ))],
        ct_config.browser_platforms,
        vec![browser_box(browser::MyBrowserBuilder::new(
            implementation_tx.clone(),
        ))],
    )
    .await?;

    let stop = ct.graceful_stop_fn();
    let ts_end = tokio::task::spawn(async move {
        tokio::time::sleep(tokio::time::Duration::from_secs(shutdown_after_s)).await;
        let ts_end = quanta::Instant::now();
        stop().await.expect("Failed to stop runtime");
        ts_end
    });

    ct.join().await?;
    server_events_tx.send(ServerEvent::StopServer).await?;
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    server_handle.stop(true).await;
    implementation_tx
        .send(ImplementationEvent::StopServer)
        .await?;
    server_join.await??;

    let mut events = all_events.lock().await;
    events.sort_by(|a, b| a.instant().cmp(&b.instant()));
    let events = events.iter().map(|e| e.clone()).collect();

    let ts_end = ts_end.await?;
    Ok((ts_end, events))
}

mod tests {
    use crate::{
        monitor::GlobalEvent, server::ServerEvent, EndInstant, ExecutionConfig,
        ImplementationEvent, JobHash, Platform,
    };
    use concurrent_tor::{execution::monitor::Event, quanta_zero};
    use log::info;
    use std::{collections::HashMap, sync::atomic::Ordering};

    #[allow(dead_code)]
    #[derive(Debug)]
    struct JobData {
        job_hash: u128,
        platform: Platform,
        ts: quanta::Instant,
    }

    impl JobData {
        fn new(job_hash: u128, platform: Platform, ts: quanta::Instant) -> Self {
            Self {
                job_hash,
                platform,
                ts,
            }
        }
    }

    #[allow(dead_code)]
    #[derive(Debug)]

    enum JobVerificationEvent {
        ImplNewJobSent(JobData),

        CTNewJobReceived(JobData),

        ServerSuccess(JobData),
        ServerFailure(JobData),

        ImplRequestSucceeded(JobData),
        ImplRequestFailed(JobData),

        CTJobProcessed(JobData),

        CTJobCompleted(JobData),
        CTJobRetry(JobData),
        CTJobFailed(JobData),
    }

    #[allow(dead_code)]
    #[derive(Debug)]
    struct PlatformData {
        platform: Platform,
        ts: quanta::Instant,
    }

    impl PlatformData {
        fn new(platform: Platform, ts: quanta::Instant) -> Self {
            Self { platform, ts }
        }
    }

    #[allow(dead_code)]
    #[derive(Debug)]
    enum PlatformVerificationEvent {
        CTRequestMade(PlatformData),
        RenewClient(PlatformData),
        RateLimited(PlatformData),
    }

    fn transform_into_platforms_and_jobs(
        events: &Vec<GlobalEvent>,
    ) -> (
        HashMap<JobHash, Vec<JobVerificationEvent>>,
        HashMap<Platform, Vec<PlatformVerificationEvent>>,
    ) {
        type JVE = JobVerificationEvent;
        type PVE = PlatformVerificationEvent; // hehe, proxmox
        let mut platforms: HashMap<Platform, Vec<PVE>> = HashMap::new();
        let mut jobs: HashMap<JobHash, Vec<JVE>> = HashMap::new();
        for event in events {
            let event = event.clone();
            match event {
                // First send job in cron
                GlobalEvent::Implementation((ts, ImplementationEvent::CronSendHttp((_, hash)))) => {
                    assert!(!jobs.contains_key(&hash));
                    jobs.insert(
                        hash,
                        vec![JVE::ImplNewJobSent(JobData::new(
                            hash,
                            Platform::MyHttp,
                            ts,
                        ))],
                    );
                }
                GlobalEvent::Implementation((
                    _,
                    ImplementationEvent::CronSendBrowser((ts, hash)),
                )) => {
                    assert!(!jobs.contains_key(&hash));
                    jobs.insert(
                        hash,
                        vec![JVE::ImplNewJobSent(JobData::new(
                            hash,
                            Platform::MyBrowser,
                            ts,
                        ))],
                    );
                }

                // Then receive http job in concurrent_tor
                GlobalEvent::Monitor((ts, Event::NewJob(info))) => {
                    assert!(jobs.contains_key(&info.job_hash));
                    jobs.get_mut(&info.job_hash)
                        .unwrap()
                        .push(JVE::CTNewJobReceived(JobData::new(
                            info.job_hash,
                            info.platform,
                            ts,
                        )));
                }

                // Then receive event in server
                GlobalEvent::Server((ts, ServerEvent::InfoSuccess(info))) => {
                    assert!(jobs.contains_key(&info.job_hash));
                    jobs.get_mut(&info.job_hash)
                        .unwrap()
                        .push(JVE::ServerSuccess(JobData::new(
                            info.job_hash,
                            info.platform,
                            ts,
                        )));
                }
                GlobalEvent::Server((ts, ServerEvent::GetSuccess(info))) => {
                    assert!(jobs.contains_key(&info.job_hash));
                    jobs.get_mut(&info.job_hash)
                        .unwrap()
                        .push(JVE::ServerSuccess(JobData::new(
                            info.job_hash,
                            info.platform,
                            ts,
                        )));
                }
                GlobalEvent::Server((ts, ServerEvent::InfoFailure(info))) => {
                    assert!(jobs.contains_key(&info.job_hash));
                    jobs.get_mut(&info.job_hash)
                        .unwrap()
                        .push(JVE::ServerFailure(JobData::new(
                            info.job_hash,
                            info.platform,
                            ts,
                        )));
                }
                GlobalEvent::Server((ts, ServerEvent::GetFailure(info))) => {
                    assert!(jobs.contains_key(&info.job_hash));
                    jobs.get_mut(&info.job_hash)
                        .unwrap()
                        .push(JVE::ServerFailure(JobData::new(
                            info.job_hash,
                            info.platform,
                            ts,
                        )));
                }

                // Then make receive post-request in implementation
                GlobalEvent::Implementation((
                    _,
                    ImplementationEvent::MadeHttpRequest((ts, hash, success)),
                )) => {
                    assert!(jobs.contains_key(&hash));
                    if success {
                        jobs.get_mut(&hash)
                            .unwrap()
                            .push(JVE::ImplRequestSucceeded(JobData::new(
                                hash,
                                Platform::MyHttp,
                                ts,
                            )));
                    } else {
                        jobs.get_mut(&hash)
                            .unwrap()
                            .push(JVE::ImplRequestFailed(JobData::new(
                                hash,
                                Platform::MyHttp,
                                ts,
                            )));
                    }
                }
                GlobalEvent::Implementation((
                    _,
                    ImplementationEvent::MadeBrowserRequest((ts, hash, success)),
                )) => {
                    assert!(jobs.contains_key(&hash));
                    if success {
                        jobs.get_mut(&hash)
                            .unwrap()
                            .push(JVE::ImplRequestSucceeded(JobData::new(
                                hash,
                                Platform::MyBrowser,
                                ts,
                            )));
                    } else {
                        jobs.get_mut(&hash)
                            .unwrap()
                            .push(JVE::ImplRequestFailed(JobData::new(
                                hash,
                                Platform::MyBrowser,
                                ts,
                            )));
                    }
                }

                // Then process job in concurrent_tor
                GlobalEvent::Monitor((ts, Event::ProcessedJob(info))) => {
                    assert!(jobs.contains_key(&info.job_hash));
                    jobs.get_mut(&info.job_hash)
                        .unwrap()
                        .push(JVE::CTJobProcessed(JobData::new(
                            info.job_hash,
                            info.platform,
                            ts,
                        )));
                    platforms
                        .entry(info.platform)
                        .or_insert(vec![])
                        .push(PVE::CTRequestMade(PlatformData::new(info.platform, ts)));
                }

                // Then complete, retry, or fail job in concurrent_tor
                GlobalEvent::Monitor((ts, Event::CompletedJob(info))) => {
                    assert!(jobs.contains_key(&info.job_hash));
                    jobs.get_mut(&info.job_hash)
                        .unwrap()
                        .push(JVE::CTJobCompleted(JobData::new(
                            info.job_hash,
                            info.platform,
                            ts,
                        )));
                }
                GlobalEvent::Monitor((ts, Event::RetryJob(info))) => {
                    assert!(jobs.contains_key(&info.job_hash));
                    jobs.get_mut(&info.job_hash)
                        .unwrap()
                        .push(JVE::CTJobRetry(JobData::new(
                            info.job_hash,
                            info.platform,
                            ts,
                        )));
                }
                GlobalEvent::Monitor((ts, Event::FailedJob(info))) => {
                    assert!(jobs.contains_key(&info.job_hash));
                    jobs.get_mut(&info.job_hash)
                        .unwrap()
                        .push(JVE::CTJobFailed(JobData::new(
                            info.job_hash,
                            info.platform,
                            ts,
                        )));
                }

                // After some time we will need to renew the client
                GlobalEvent::Monitor((ts, Event::WorkerRenewingClient(info))) => {
                    let platform = info.platform;
                    if !platforms.contains_key(&platform) {
                        platforms.insert(platform, vec![]);
                    }
                    platforms
                        .get_mut(&platform)
                        .unwrap()
                        .push(PVE::RenewClient(PlatformData::new(platform, ts)));
                }

                // We could also get rate limited
                GlobalEvent::Monitor((ts, Event::WorkerRateLimited(info))) => {
                    let platform = info.platform;
                    if !platforms.contains_key(&platform) {
                        platforms.insert(platform, vec![]);
                    }
                    platforms
                        .get_mut(&platform)
                        .unwrap()
                        .push(PVE::RateLimited(PlatformData::new(platform, ts)));
                }

                // Hard match the rest, but ignore them
                GlobalEvent::Server((_, ServerEvent::StopServer)) => {}
                GlobalEvent::Implementation((_, ImplementationEvent::StopServer)) => {}
                GlobalEvent::Monitor((_, Event::MaxAttemptsReached(_))) => {}
                GlobalEvent::Monitor((_, Event::BalanceCirculation(_))) => {}
                GlobalEvent::Monitor((_, Event::StopMonitor)) => {}
            }
        }
        (jobs, platforms)
    }

    macro_rules! assert_matches {
        ($expression:expr, $pattern:pat $(if $guard:expr)? $(,)?) => { {
            match $expression {
                $pattern $(if $guard)? => {},
                _ => panic!("Match assertion failed: `{:?} != {:?}`", $expression, stringify!($pattern))
            }
        } };
    }

    macro_rules! assert_matches_any {
        ($expression:expr, $($pattern:pat $(if $guard:expr)?),+ $(,)?) => { {
            let mut matched = false;
            $(
                match $expression {
                    $pattern $(if $guard)? => { matched = true; },
                    _ => {}
                }
            )+
            if !matched {
                panic!("Match assertion failed: `{:?} did not match any of the provided patterns`", $expression);
            }
        } };
    }

    fn verify_job_execution(
        ts_end: EndInstant,
        jobs: &Vec<JobVerificationEvent>,
        run: usize,
        job_count: usize,
    ) {
        let n = jobs.len();
        for ((i, job), nxt_job) in jobs
            .iter()
            .skip(job_count)
            .enumerate()
            .zip(jobs.iter().skip(job_count + 1))
        {
            match job {
                JobVerificationEvent::ImplNewJobSent(_) => {
                    assert_matches!(nxt_job, JobVerificationEvent::CTNewJobReceived(_));
                }
                JobVerificationEvent::CTNewJobReceived(_) => {
                    assert_matches_any!(
                        nxt_job,
                        JobVerificationEvent::ServerSuccess(_),
                        JobVerificationEvent::ServerFailure(_)
                    );
                }
                JobVerificationEvent::ServerSuccess(_) => {
                    assert_matches!(nxt_job, JobVerificationEvent::ImplRequestSucceeded(_));
                }
                JobVerificationEvent::ServerFailure(_) => {
                    assert_matches!(nxt_job, JobVerificationEvent::ImplRequestFailed(_));
                }
                JobVerificationEvent::ImplRequestSucceeded(data) => {
                    assert_matches!(nxt_job, JobVerificationEvent::CTJobProcessed(_));
                    if data.ts < ts_end && run == 0 {
                        assert!(i + 2 < n);
                        let nxt_nxt_job = &jobs[i + 2];
                        assert_matches!(nxt_nxt_job, JobVerificationEvent::CTJobCompleted(_));
                    }
                }
                JobVerificationEvent::ImplRequestFailed(data) => {
                    assert_matches!(nxt_job, JobVerificationEvent::CTJobProcessed(_));
                    if data.ts < ts_end && run == 0 {
                        // i.e. first run. This condition is too difficult to test for in the second run
                        assert!(i + 2 < n);
                        let nxt_nxt_job = &jobs[i + 2];
                        assert_matches_any!(
                            nxt_nxt_job,
                            JobVerificationEvent::CTJobFailed(_),
                            JobVerificationEvent::CTJobRetry(_)
                        );
                    }
                }
                JobVerificationEvent::CTJobProcessed(_) => {
                    assert_matches_any!(
                        nxt_job,
                        JobVerificationEvent::CTJobCompleted(_),
                        JobVerificationEvent::CTJobRetry(_),
                        JobVerificationEvent::CTJobFailed(_)
                    );
                }
                JobVerificationEvent::CTJobCompleted(_) => {
                    unreachable!("Completed job should not be followed by any other job");
                    // Given how we are iterating, this should never be reached
                }
                JobVerificationEvent::CTJobRetry(_) => {
                    assert_matches_any!(
                        nxt_job,
                        JobVerificationEvent::ServerSuccess(_),
                        JobVerificationEvent::ServerFailure(_)
                    );
                }
                JobVerificationEvent::CTJobFailed(_) => {
                    unreachable!("Failed job should not be followed by any other job");
                    // Given how we are iterating, this should never be reached
                }
            }
        }
    }

    fn verify_platform_execution(
        platform: &Vec<PlatformVerificationEvent>,
        execution_config: ExecutionConfig,
        run: usize,
    ) {
        let mut mistakes_made = 0;
        let mut recent_ts = quanta_zero();
        let mut min_ts_diff_ms = u128::MAX;
        let mut current_count = 0;
        for event in platform {
            match event {
                PlatformVerificationEvent::CTRequestMade(data) => {
                    current_count += 1;
                    min_ts_diff_ms = min_ts_diff_ms.min((data.ts - recent_ts).as_micros());
                    recent_ts = data.ts;
                }
                PlatformVerificationEvent::RenewClient(_) => {
                    if run == 0 {
                        assert_eq!(current_count, execution_config.max_requests);
                        recent_ts = quanta_zero();
                        current_count = 0;
                    } else {
                        if current_count != execution_config.max_requests {
                            mistakes_made += 1;
                        }
                        recent_ts = quanta_zero();
                        current_count = 0;
                    }
                }
                PlatformVerificationEvent::RateLimited(_) => {}
            }
        }
        assert!(mistakes_made <= run);
        assert!(min_ts_diff_ms > execution_config.timeout_ms as u128);
    }

    fn verify_execution_of_events(
        ts_end: EndInstant,
        events: &Vec<GlobalEvent>,
        execution_config: ExecutionConfig,
        run: usize,
        job_counts: Option<HashMap<JobHash, usize>>,
    ) -> HashMap<JobHash, usize> {
        let (jobs, platforms) = transform_into_platforms_and_jobs(events);

        let job_counts = if let Some(job_counts) = job_counts {
            job_counts
        } else {
            let mut job_counts = HashMap::new();
            for (hash, _job) in &jobs {
                job_counts.insert(*hash, 0);
            }
            job_counts
        };

        for (hash, job) in &jobs {
            let job_count = job_counts.get(&hash).unwrap();
            verify_job_execution(ts_end, job, run, *job_count);
        }
        for platform in platforms.values() {
            verify_platform_execution(platform, execution_config, run);
        }
        let mut job_counts = HashMap::new();
        for (hash, job) in jobs {
            job_counts.insert(hash, job.len());
        }
        job_counts
    }

    #[test]
    fn runtime_tests() {
        dotenv::dotenv().ok();
        tracing_subscriber::fmt::init();
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .enable_all()
            .build()
            .unwrap();

        use super::*;
        let mut execution_config = ExecutionConfig {
            http_workers: 1,
            browser_workers: 1,
            send_req_timeout_ms: 100,
            timeout_ms: 300,
            max_requests: 3,
            shutdown_after_s: 10,
        };
        let total_job_cnt = Arc::new(AtomicUsize::new(0));
        let (ts_end, events) = {
            let _guard = rt.enter();
            let handle = rt.block_on(async {
                my_main(
                    true,
                    false,
                    total_job_cnt.clone(),
                    SERVER_ADDR,
                    SERVER_WORKERS,
                    execution_config,
                )
                .await
            });
            handle.expect("Failed to run main")
        };

        info!("Verifying {} events.", events.len());
        let job_counts = verify_execution_of_events(ts_end, &events, execution_config, 0, None);
        assert_eq!(job_counts.len(), total_job_cnt.load(Ordering::Relaxed));

        execution_config.shutdown_after_s = 30;
        let (ts_end2, events2) = {
            let _guard = rt.enter();
            let handle = rt.block_on(async {
                my_main(
                    false,
                    true,
                    total_job_cnt.clone(),
                    SERVER_ADDR,
                    SERVER_WORKERS,
                    execution_config,
                )
                .await
            });
            handle.expect("Failed to run main")
        };

        info!("Verifying new {} events.", events2.len());
        let all_events = events.iter().chain(events2.iter()).cloned().collect();
        for (ev1, ev2) in events.iter().zip(events2.iter()) {
            assert!(ev1.instant() < ev2.instant());
        }
        let job_counts =
            verify_execution_of_events(ts_end2, &all_events, execution_config, 1, Some(job_counts));
        assert_eq!(job_counts.len(), total_job_cnt.load(Ordering::Relaxed));
    }
}
