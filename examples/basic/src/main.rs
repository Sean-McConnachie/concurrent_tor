use crate::monitor::GlobalEvent;
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
    IpHttp,
    IpBrowser,
}

impl PlatformT for Platform {
    fn request_from_json(&self, json: &str) -> Result<Box<dyn WorkerRequest>> {
        match self {
            Platform::IpHttp => http::MyHttpRequest::from_json(json),
            Platform::IpBrowser => browser::MyBrowserRequest::from_json(json),
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

#[derive(Debug, Clone)]
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
    use concurrent_tor::execution::scheduler::WorkerRequest;
    use concurrent_tor::{
        execution::{
            cron::{CronPlatform, CronPlatformBuilder},
            scheduler::{Job, NotRequested, QueueJob},
        },
        exports::{async_trait, AsyncChannelSender},
        Result,
    };
    use log::info;
    use std::{
        sync::{atomic::AtomicBool, Arc},
        time::Duration,
    };
    use tokio::time::sleep;

    pub struct Cron {
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
            (hash, Job::init_from_box(Platform::IpHttp, req, 3))
        }

        fn build_browser_job(&self) -> (JobHash, Job<NotRequested, Platform>) {
            let req =
                MyBrowserRequest::new(format!("http://{}:{}/get/", SERVER_ADDR.0, SERVER_ADDR.1));
            let hash = req.hash().expect("Unable to hash");
            (hash, Job::init(Platform::IpBrowser, Box::new(req), 3))
        }
    }

    #[async_trait]
    impl CronPlatform<Platform> for Cron {
        async fn start(self: Box<Self>) -> Result<()> {
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
                    sleep(Duration::from_millis(self.sleep_ms)).await;
                }
            }
            Ok(())
        }
    }

    pub struct MyCronBuilder {
        sleep_ms: u64,
        use_http: bool,
        use_browser: bool,
        events: Sender<ImplementationEvent>,
        queue_job: Option<AsyncChannelSender<QueueJob<Platform>>>,
        stop_flag: Option<Arc<AtomicBool>>,
    }

    impl MyCronBuilder {
        pub fn new(
            sleep_ms: u64,
            use_http: bool,
            use_browser: bool,
            events: Sender<ImplementationEvent>,
        ) -> Self {
            Self {
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
    use concurrent_tor::execution::scheduler::Requested;
    use concurrent_tor::{
        execution::{
            client::Client,
            http::{HttpPlatform, HttpPlatformBuilder},
            scheduler::{Job, NotRequested, QueueJob, WorkerRequest},
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
            vec![QueueJob::Completed(completed)]
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
            Platform::IpHttp
        }

        fn build(&self) -> Box<dyn HttpPlatform<Platform, ClientBackend>> {
            Box::new(MyHttp::new(self.events.clone()))
        }
    }
}

mod browser {
    use super::{ImplementationEvent, Platform, FAILURE_MESSAGE, SUCCESS_MESSAGE};
    use async_channel::Sender;
    use concurrent_tor::execution::scheduler::Requested;
    use concurrent_tor::{
        execution::{
            browser::{BrowserPlatform, BrowserPlatformBuilder},
            scheduler::{Job, NotRequested, QueueJob, WorkerRequest},
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
            if r.contains(SUCCESS_MESSAGE) {
                self.events
                    .send(ImplementationEvent::MadeBrowserRequest((
                        quanta::Instant::now(),
                        req.hash().expect("Unable to hash"),
                        true,
                    )))
                    .await
                    .expect("Failed to send event");
            } else if r.contains(FAILURE_MESSAGE) {
                self.events
                    .send(ImplementationEvent::MadeBrowserRequest((
                        quanta::Instant::now(),
                        req.hash().expect("Unable to hash"),
                        false,
                    )))
                    .await
                    .expect("Failed to send event");
            } else {
                unreachable!("Unexpected response");
            }
            let completed: Job<Requested, Platform> = job.into();
            vec![QueueJob::Completed(completed)]
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
            Platform::IpBrowser
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

    #[derive(Debug, Clone)]
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
    use crate::{FAILURE_MESSAGE, SUCCESS_MESSAGE};
    use actix_web::{
        dev::ServerHandle, get, http::StatusCode, post, web, App, HttpServer, Responder,
    };
    use async_channel::{Receiver, Sender};
    use log::info;
    use rand::{prelude::SliceRandom, rngs::OsRng};
    use serde::{Deserialize, Serialize};
    use tokio::task::JoinHandle;

    #[allow(dead_code)]
    #[derive(Debug, Clone)]
    pub struct JobInfo {
        pub job_hash: u128,
        pub ts: quanta::Instant,
    }

    impl JobInfo {
        pub fn new(hash: u128) -> Self {
            Self {
                job_hash: hash,
                ts: quanta::Instant::now(),
            }
        }
    }

    #[allow(dead_code)]
    #[derive(Debug, Clone)]
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
        let job_info = JobInfo::new(job_hash.clone());
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
        let job_info = JobInfo::new(job_hash.clone());
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

async fn my_main(
    rm_db: bool,
    server_addr: (&str, u16),
    server_workers: usize,
    http_workers: u16,
    browser_workers: u16,
    send_req_timeout_ms: u64,
    timeout_ms: u32,
    shutdown_after_s: u64,
) -> Result<Vec<GlobalEvent>> {
    dotenv::dotenv().ok();
    tracing_subscriber::fmt::init();
    info!("Starting up");

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
            target_circulation: 10,
            http_workers,
            browser_workers,
        },
        http_platforms: hashmap!(
            Platform::IpHttp => HttpPlatformConfig {
                max_requests: 10,
                timeout_ms,
            }
        ),
        browser_platforms: hashmap!(
            Platform::IpBrowser => BrowserPlatformConfig {
                max_requests: 10,
                timeout_ms,
            }
        ),
    };
    let monitor = monitor::MyMonitor::new(server_events_rx, implementation_rx);
    let all_events = monitor.events();
    let ct = CTRuntime::run(
        ct_config.workers,
        monitor,
        SimpleScheduler::new(),
        build_main_client().await?,
        vec![cron_box(cron::MyCronBuilder::new(
            send_req_timeout_ms,
            http_workers > 0,
            browser_workers > 0,
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

    let stop = ct.graceful_stop();
    tokio::task::spawn(async move {
        tokio::time::sleep(tokio::time::Duration::from_secs(shutdown_after_s)).await;
        stop().await.expect("Failed to stop runtime");
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
    Ok(events)
}

mod tests {
    #[tokio::test]
    async fn runtime_tests() {
        use super::*;
        let events = my_main(true, SERVER_ADDR, SERVER_WORKERS, 0, 1, 500, 0, 5)
            .await
            .expect("Failed to run main");

        println!("Events: {:?}", events);
    }
}

fn main() {}
