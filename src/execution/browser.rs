use crate::config::BrowserPlatformConfig;
use crate::execution::scheduler::{Job, MainTorClient, NotRequested, QueueJob, QueueJobStatus};
use crate::Result;
use arti::socks::run_socks_proxy;
use async_trait::async_trait;
use chrono::{DateTime, TimeZone, Utc};
use crossbeam::channel::{Receiver, Sender};
use headless_chrome::{Browser, LaunchOptions, Tab};
use log::{debug, error};
use std::collections::HashMap;
use tokio::task::JoinHandle;
use tor_config::Listen;

use super::scheduler::PlatformT;

pub trait BrowserPlatformBuilder<P: PlatformT> {
    fn platform(&self) -> P;
    fn build(&self) -> Box<dyn BrowserPlatform<P>>;
}

#[async_trait]
pub trait BrowserPlatform<P: PlatformT>: Send {
    async fn process_job(&self, job: Job<NotRequested, P>, tab: &Tab) -> Result<Vec<QueueJob<P>>>;
}

enum BrowserPlatformBehaviourError {
    Ok,
    MustWait,
    MaxRequests,
}

pub struct BrowserPlatformData<P: PlatformT> {
    platform_impl: Box<dyn BrowserPlatform<P>>,
    config: BrowserPlatformConfig,
    last_request: DateTime<Utc>,
    requests: u32,
}

impl<P> BrowserPlatformData<P>
where
    P: PlatformT,
{
    pub fn new(platform_impl: Box<dyn BrowserPlatform<P>>, config: BrowserPlatformConfig) -> Self {
        BrowserPlatformData {
            platform_impl,
            config,
            last_request: Utc.timestamp_opt(0, 0).unwrap(),
            requests: 0,
        }
    }

    fn can_request(&self) -> BrowserPlatformBehaviourError {
        let now = Utc::now();
        let diff = now - self.last_request;
        if self.requests >= self.config.max_requests {
            return BrowserPlatformBehaviourError::MaxRequests;
        }
        if diff.num_seconds() > self.config.timeout as i64 {
            return BrowserPlatformBehaviourError::Ok;
        }
        BrowserPlatformBehaviourError::MustWait
    }

    fn request_complete(&mut self) {
        self.requests += 1;
        self.last_request = Utc::now();
    }

    fn reset(self) -> Self {
        BrowserPlatformData {
            last_request: Utc.timestamp_opt(0, 0).unwrap(),
            requests: 0,
            ..self
        }
    }
}

pub struct BrowserWorker<P: PlatformT> {
    worker_id: u32,
    /// Request job from a queue
    request_job: Sender<QueueJobStatus>,
    /// Receive job from a queue
    recv_job: Receiver<Job<NotRequested, P>>,
    /// Add job back to queue if the worker is unable to process it (possibly due to rate limiting)
    requeue_job: Sender<Job<NotRequested, P>>,
    /// Add a new, or existing job to the queue
    queue_job: Sender<QueueJob<P>>,
    /// Here to query a new client when necessary
    main_client: MainTorClient,
    /// Platforms that store [BrowserPlatform], the last request time, and the number of requests.
    platforms: HashMap<P, BrowserPlatformData<P>>,
    /// Proxy handle
    proxy_handle: JoinHandle<()>,
    /// Browser handle
    browser: Browser,
    /// Headless mode
    headless: bool,
    /// Listener port
    port: u16,
}

impl<P> BrowserWorker<P>
where
    P: PlatformT,
{
    pub fn new(
        worker_id: u32,
        request_job: Sender<QueueJobStatus>,
        recv_job: Receiver<Job<NotRequested, P>>,
        requeue_job: Sender<Job<NotRequested, P>>,
        queue_job: Sender<QueueJob<P>>,
        main_client: MainTorClient,
        platforms: HashMap<P, BrowserPlatformData<P>>,
        headless: bool,
        port: u16,
    ) -> Result<Self> {
        debug!("Creating browser worker {}", worker_id);
        let socks_addr = Self::format_socks_proxy_addr(port);
        let browser_opts = LaunchOptions::default_builder()
            .headless(headless)
            .proxy_server(Some(&socks_addr))
            .build()
            .expect("Failed to find chrome executable");

        Ok(BrowserWorker {
            worker_id,
            request_job,
            recv_job,
            requeue_job,
            queue_job,
            proxy_handle: Self::start_proxy_handle(worker_id, &main_client, port),
            main_client,
            platforms,
            browser: Browser::new(browser_opts)?,
            headless,
            port,
        })
    }

    pub fn format_socks_proxy_addr(port: u16) -> String {
        format!("socks5://localhost:{}", port)
    }

    fn start_proxy_handle(
        worker_id: u32,
        main_client: &MainTorClient,
        port: u16,
    ) -> JoinHandle<()> {
        debug!("Starting proxy for browser worker {}", worker_id);
        let client = main_client.isolated_client();
        let listen = Listen::new_localhost(port);
        tokio::spawn(async {
            match run_socks_proxy(client.runtime().clone(), client, listen).await {
                Ok(_) => debug!("Proxy exited successfully"),
                Err(e) => error!("Proxy exited with error: {:?}", e),
            }
        })
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
        debug!("Starting worker {}", self.worker_id);
        loop {
            let job = self.recv_job.recv()?;
            let platform = self.platforms.get_mut(&job.platform).unwrap();
            match platform.can_request() {
                BrowserPlatformBehaviourError::Ok => {
                    debug!("Processing job for browser worker {}", self.worker_id);
                    let tab = self.browser.new_tab()?;
                    let jobs = platform.platform_impl.process_job(job, &tab).await?;
                    for job in jobs {
                        self.queue_job.send(job)?;
                    }
                }
                BrowserPlatformBehaviourError::MustWait => {
                    debug!("Rate limiting for browser worker {}", self.worker_id);
                    // Return the job so another worker can process it, or we can retry later
                    self.requeue_job.send(job)?;
                    // TODO: Add sleep?
                }
                BrowserPlatformBehaviourError::MaxRequests => {
                    debug!("Max requests for browser worker {}", self.worker_id);
                    // Return the job so another worker can process it, or we can retry later
                    self.requeue_job.send(job)?;
                    // Renew the client so we get a new IP
                    self = self.renew_client()?;
                }
            }
        }
    }
}
