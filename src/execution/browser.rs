use crate::{
    config::BrowserPlatformConfig,
    execution::{
        client::{
            worker_job_logic_process, worker_job_logic_start, Client, MainClient,
            WorkerLogicAction, WorkerType,
        },
        monitor::Event,
        scheduler::{
            Job, NotRequested, PlatformCanRequest, PlatformHistory, PlatformT, QueueJob,
            QueueJobStatus, WorkerAction,
        },
    },
    quanta_zero, Result,
};
use async_channel::{Receiver, Sender};
use async_trait::async_trait;
use headless_chrome::{Browser, LaunchOptions, Tab};
use log::{debug, info};
use std::{collections::HashMap, sync::Arc};
use tokio::task::JoinHandle;

pub trait BrowserPlatformBuilder<P: PlatformT>: Send {
    fn platform(&self) -> P;
    fn build(&self) -> Box<dyn BrowserPlatform<P>>;
}

#[async_trait]
pub trait BrowserPlatform<P: PlatformT>: Send {
    /// Function should not fail when passed back to the API. Therefore, it should handle all errors itself.
    async fn process_job(&self, job: &Job<NotRequested, P>, tab: Arc<Tab>) -> Vec<QueueJob<P>>;
}

pub struct BrowserPlatformData {
    config: BrowserPlatformConfig,
    last_request: quanta::Instant,
    requests: u32,
}

impl BrowserPlatformData {
    pub fn new(config: BrowserPlatformConfig) -> Self {
        BrowserPlatformData {
            config,
            last_request: quanta_zero(),
            requests: 0,
        }
    }
}

impl PlatformHistory for BrowserPlatformData {
    fn can_request(&self, now: quanta::Instant) -> PlatformCanRequest {
        let diff = now - self.last_request;
        if self.requests >= self.config.max_requests {
            return PlatformCanRequest::MaxRequests;
        }
        if diff.as_micros() > (self.config.timeout_ms as u128) * 1000 {
            return PlatformCanRequest::Ok;
        }
        PlatformCanRequest::MustWait
    }

    fn request_complete(&mut self, ts_end: quanta::Instant) {
        self.requests += 1;
        self.last_request = ts_end;
    }

    fn reset(&mut self) {
        self.requests = 0;
        self.last_request = quanta_zero();
    }
}

pub struct BrowserWorker<P: PlatformT, C: Client, M: MainClient<C>> {
    worker_id: u16,
    /// Monitor to send events to
    monitor: Sender<Event<P>>,
    /// Request job from a queue
    request_job: Sender<QueueJobStatus>,
    /// Receive job from a queue
    recv_job: Receiver<WorkerAction<P>>,
    /// Add job back to queue if the worker is unable to process it (possibly due to rate limiting)
    requeue_job: Sender<WorkerAction<P>>,
    /// Add a new, or existing job to the queue
    queue_job: Sender<QueueJob<P>>,
    /// Here to query a new client when necessary
    main_client: M,
    /// Platforms that store [BrowserPlatform], the last request time, and the number of requests.
    platform_data: HashMap<P, BrowserPlatformData>,
    /// Platform implementations
    platform_impls: HashMap<P, Box<dyn BrowserPlatform<P>>>,
    /// Proxy handle
    proxy_handle: JoinHandle<()>,
    /// Browser handle
    browser: Browser,
    #[allow(dead_code)]
    /// Headless mode
    headless: bool,
    /// Listener port
    port: u16,
    _client: std::marker::PhantomData<C>,
}

impl<P, C, M> BrowserWorker<P, C, M>
where
    P: PlatformT,
    C: Client,
    M: MainClient<C>,
{
    pub fn new(
        worker_id: u16,
        monitor: Sender<Event<P>>,
        request_job: Sender<QueueJobStatus>,
        recv_job: Receiver<WorkerAction<P>>,
        requeue_job: Sender<WorkerAction<P>>,
        queue_job: Sender<QueueJob<P>>,
        main_client: M,
        platform_data: HashMap<P, BrowserPlatformData>,
        platform_impls: HashMap<P, Box<dyn BrowserPlatform<P>>>,
        headless: bool,
        port: u16,
    ) -> Result<Self> {
        debug!("Creating browser worker {}", worker_id);
        let socks_addr = Self::format_socks_proxy_addr(port);
        let mut browser_opts = LaunchOptions::default_builder();
        if M::use_proxy() {
            browser_opts.proxy_server(Some(&socks_addr));
        }
        let browser_opts = browser_opts
            .headless(headless)
            .build()
            .expect("Failed to find chrome executable");
        let browser = Browser::new(browser_opts)?;
        Ok(BrowserWorker {
            worker_id,
            monitor,
            request_job,
            recv_job,
            requeue_job,
            queue_job,
            proxy_handle: Self::start_proxy_handle(worker_id, &main_client, port),
            main_client,
            platform_data,
            platform_impls,
            browser,
            headless,
            port,
            _client: std::marker::PhantomData,
        })
    }

    pub fn format_socks_proxy_addr(port: u16) -> String {
        format!("socks5://localhost:{}", port)
    }

    fn start_proxy_handle(worker_id: u16, main_client: &M, port: u16) -> JoinHandle<()> {
        debug!("Starting proxy for browser worker {}", worker_id);
        if M::use_proxy() {
            let client = main_client.isolated_client();
            client
                .start_proxy(port)
                .expect("Proxy implementation must return a join handle.")
        } else {
            tokio::spawn(async {})
        }
    }

    fn renew_client(self) -> Result<Self> {
        debug!("Renewing client for browser worker {}", self.worker_id);
        self.proxy_handle.abort();
        let proxy_handle = Self::start_proxy_handle(self.worker_id, &self.main_client, self.port);
        Ok(BrowserWorker {
            proxy_handle,
            ..self
        })
    }

    pub(crate) async fn start(mut self) -> Result<()> {
        info!("Starting browser worker {}", self.worker_id);
        loop {
            let action = self.recv_job.recv().await?;
            let job = match action {
                WorkerAction::Job(job) => job,
                WorkerAction::StopProgram => {
                    break;
                }
            };

            match worker_job_logic_start(
                self.worker_id,
                job,
                &mut self.platform_data,
                &self.monitor,
                &self.requeue_job,
            )
            .await?
            {
                WorkerLogicAction::Nothing => {}
                WorkerLogicAction::RenewClient => {
                    self = self.renew_client()?;
                }
                WorkerLogicAction::ProcessJob((ts_start, job)) => {
                    let tab = self.browser.new_tab()?;
                    let jobs = self
                        .platform_impls
                        .get(&job.platform)
                        .unwrap()
                        .process_job(&job, tab.clone())
                        .await;
                    // tokio::task::spawn_blocking(move || match tab.close(false) {
                    //     Ok(_) => {}
                    //     Err(e) => {
                    //         debug!("Failed to close tab: {:?}. Already closed?", e);
                    //     }
                    // })
                    // .await?;
                    worker_job_logic_process(
                        ts_start,
                        self.worker_id,
                        WorkerType::Http,
                        job,
                        jobs,
                        &mut self.platform_data,
                        &self.monitor,
                        &self.queue_job,
                        &self.request_job,
                    )
                    .await?;
                }
            }
        }
        Ok(())
    }
}
