use crate::execution::client::WorkerType;
use crate::execution::monitor::{BasicWorkerInfo, ProcessedJobInfo};
use crate::{
    config::BrowserPlatformConfig,
    execution::{
        client::{Client, MainClient},
        monitor::Event,
        scheduler::{Job, NotRequested, PlatformT, QueueJob, QueueJobStatus, WorkerAction},
    },
    Result,
};
use anyhow::anyhow;
use async_trait::async_trait;
use chrono::{DateTime, TimeZone, Utc};
use crossbeam::channel::{Receiver, Sender};
use headless_chrome::{Browser, LaunchOptions, Tab};
use log::{debug, info};
use std::{collections::HashMap, sync::Arc};
use tokio::task::JoinHandle;

pub trait BrowserPlatformBuilder<P: PlatformT> {
    fn platform(&self) -> P;
    fn build(&self) -> Box<dyn BrowserPlatform<P>>;
}

#[async_trait]
pub trait BrowserPlatform<P: PlatformT>: Send {
    /// Function should not fail when passed back to the API. Therefore, it should handle all errors itself.
    async fn process_job(&self, job: Job<NotRequested, P>, tab: Arc<Tab>) -> Vec<QueueJob<P>>;
}

enum BrowserPlatformBehaviourError {
    Ok,
    MustWait,
    MaxRequests,
}

pub struct BrowserPlatformData<P: PlatformT> {
    platform_impl: Box<dyn BrowserPlatform<P>>,
    config: BrowserPlatformConfig,
    last_request: quanta::Instant,
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
            last_request: quanta::Instant::now(),
            requests: 0,
        }
    }

    fn can_request(&self, now: quanta::Instant) -> BrowserPlatformBehaviourError {
        let diff = now - self.last_request;
        if self.requests >= self.config.max_requests {
            return BrowserPlatformBehaviourError::MaxRequests;
        }
        if diff.as_micros() > (self.config.timeout_ms as u128) * 1000 {
            return BrowserPlatformBehaviourError::Ok;
        }
        BrowserPlatformBehaviourError::MustWait
    }

    fn request_complete(&mut self, ts_end: quanta::Instant) {
        self.requests += 1;
        self.last_request = ts_end;
    }

    fn reset(&mut self) {
        self.requests = 0;
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
    platforms: HashMap<P, BrowserPlatformData<P>>,
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
        platforms: HashMap<P, BrowserPlatformData<P>>,
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
            platforms,
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
        debug!("Starting worker {}", self.worker_id);
        loop {
            let action = self.recv_job.recv()?;
            let job = match action {
                WorkerAction::Job(job) => job,
                WorkerAction::StopProgram => {
                    break;
                }
            };

            let ts_start = quanta::Instant::now();
            let platform = self.platforms.get_mut(&job.platform).unwrap();
            let job_platform = job.platform;
            match platform.can_request(ts_start) {
                BrowserPlatformBehaviourError::Ok => {
                    debug!("Processing job for browser worker {}", self.worker_id);
                    let job_hash = job.request.hash()?;

                    let tab = self.browser.new_tab()?;
                    let jobs = platform.platform_impl.process_job(job, tab).await;
                    let ts_end = quanta::Instant::now();
                    self.monitor
                        .send(Event::ProcessedJob(ProcessedJobInfo::new(
                            job_platform,
                            WorkerType::Browser,
                            self.worker_id,
                            ts_start,
                            ts_end,
                            job_hash,
                        )))?;
                    platform.request_complete(ts_end);

                    for job in jobs {
                        self.queue_job.send(job).map_err(|e| {
                            anyhow!(
                                "Failed to send job to queue in browser worker {}: {:?}",
                                self.worker_id,
                                e
                            )
                        })?;
                    }
                }
                BrowserPlatformBehaviourError::MustWait => {
                    debug!("Rate limiting for browser worker {}", self.worker_id);
                    // Return the job so another worker can process it, or we can retry later

                    self.requeue_job.send(WorkerAction::Job(job)).map_err(|e| {
                        anyhow!(
                            "Failed to requeue job in browser worker {}: {:?}",
                            self.worker_id,
                            e
                        )
                    })?;

                    self.monitor
                        .send(Event::WorkerRateLimited(BasicWorkerInfo::new(
                            job_platform,
                            WorkerType::Browser,
                            self.worker_id,
                            quanta::Instant::now(),
                        )))?;
                }
                BrowserPlatformBehaviourError::MaxRequests => {
                    debug!("Max requests for browser worker {}", self.worker_id);
                    // Return the job so another worker can process it, or we can retry later

                    self.requeue_job.send(WorkerAction::Job(job)).map_err(|e| {
                        anyhow!(
                            "Failed to requeue job in browser worker {}: {:?}",
                            self.worker_id,
                            e
                        )
                    })?;

                    // Renew the client so we get a new IP
                    self = self.renew_client()?;
                    // Reset all platforms so we can make more requests
                    for (_, platform) in self.platforms.iter_mut() {
                        platform.reset();
                    }

                    self.monitor
                        .send(Event::WorkerRenewingClient(BasicWorkerInfo::new(
                            job_platform,
                            WorkerType::Browser,
                            self.worker_id,
                            quanta::Instant::now(),
                        )))?;
                }
            }
            self.request_job
                .send(QueueJobStatus::WorkerCompleted {
                    worker_id: self.worker_id,
                })
                .map_err(|e| {
                    anyhow!(
                        "Failed to send worker completed status {}: {:?}",
                        self.worker_id,
                        e
                    )
                })?;
        }
        info!("Stopping browser worker {}", self.worker_id);
        Ok(())
    }
}
