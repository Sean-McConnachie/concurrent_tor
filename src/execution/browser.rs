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
use fantoccini::wd::Capabilities;
use log::{debug, info};
use serde::Serialize;
use std::{collections::HashMap, process::Stdio};
use tokio::{
    process::{Child, Command},
    task::JoinHandle,
};

pub trait BrowserPlatformBuilder<P: PlatformT>: Send {
    fn platform(&self) -> P;
    fn build(&self) -> Box<dyn BrowserPlatform<P>>;
}

#[async_trait]
pub trait BrowserPlatform<P: PlatformT>: Send {
    /// Function should not fail when passed back to the API. Therefore, it should handle all errors itself.
    async fn process_job(
        &self,
        job: &Job<NotRequested, P>,
        client: &fantoccini::Client,
    ) -> Vec<QueueJob<P>>;
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(self) struct FirefoxProxy {
    proxy_type: String,
    socks_proxy: String,
    socks_version: i32,
}

#[derive(Serialize)]
pub(self) struct FirefoxArgs {
    args: Vec<String>,
}

#[derive(Serialize)]
pub(self) struct FirefoxOpts {
    proxy: Option<FirefoxProxy>,
    #[serde(rename = "moz:firefoxOptions")]
    headless: FirefoxArgs,
}

impl FirefoxOpts {
    fn to_capabilities(&self) -> Capabilities {
        let value = serde_json::to_value(self).unwrap();
        if let serde_json::Value::Object(map) = value {
            map
        } else {
            panic!("Expected FirefoxOpts to serialize into an object");
        }
    }
}

impl FirefoxOpts {
    fn new(socks_port: u16, headless: bool, use_proxy: bool) -> Self {
        Self {
            proxy: match use_proxy {
                true => Some(FirefoxProxy {
                    proxy_type: "socks".to_string(),
                    socks_proxy: format!("localhost:{}", socks_port),
                    socks_version: 5,
                }),
                false => None,
            },
            headless: FirefoxArgs {
                args: match headless {
                    true => vec!["--headless".to_string()],
                    false => vec![],
                },
            },
        }
    }
}

pub(crate) struct BrowserPlatformData {
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

pub(crate) struct BrowserWorker<P: PlatformT, C: Client, M: MainClient<C>> {
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
    /// Driver handle
    driver_handle: Child,
    /// Browser client
    browser: fantoccini::Client,
    /// Driver port
    driver_port: u16,
    /// Listener port
    socks_port: u16,
    /// Firefox options
    firefox_opts: FirefoxOpts,
    /// Path to the driver
    driver_fp: String,
    _client: std::marker::PhantomData<C>,
}

impl<P, C, M> BrowserWorker<P, C, M>
where
    P: PlatformT,
    C: Client,
    M: MainClient<C>,
{
    async fn connect_browser(opts: &FirefoxOpts, driver_port: u16) -> Result<fantoccini::Client> {
        let browser = fantoccini::ClientBuilder::native()
            .capabilities(opts.to_capabilities())
            .connect(&format!("http://localhost:{}", driver_port))
            .await?;
        Ok(browser)
    }

    pub async fn new(
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
        driver_port: u16,
        socks_port: u16,
        driver_fp: String,
    ) -> Result<Self> {
        let headless_str = if headless { "headless" } else { "headed" };
        info!("Creating {} browser worker {}", headless_str, worker_id);

        let proxy_handle = Self::start_proxy_handle(worker_id, &main_client, socks_port);
        let driver_handle = Self::start_driver_handle(worker_id, &driver_fp, driver_port);
        let firefox_opts = FirefoxOpts::new(socks_port, headless, M::use_proxy());
        let browser = Self::connect_browser(&firefox_opts, driver_port).await?;

        Ok(BrowserWorker {
            worker_id,
            monitor,
            request_job,
            recv_job,
            requeue_job,
            queue_job,
            proxy_handle,
            main_client,
            platform_data,
            platform_impls,
            driver_handle,
            firefox_opts,
            browser,
            driver_port,
            socks_port,
            driver_fp,
            _client: std::marker::PhantomData,
        })
    }

    fn start_proxy_handle(worker_id: u16, main_client: &M, proxy_port: u16) -> JoinHandle<()> {
        info!("Starting proxy for browser worker {}", worker_id);
        if M::use_proxy() {
            let client = main_client.isolated_client();
            client
                .start_proxy(proxy_port)
                .expect("Proxy implementation must return a join handle.")
        } else {
            tokio::spawn(async {})
        }
    }

    fn start_driver_handle(worker_id: u16, driver_fp: &str, driver_port: u16) -> Child {
        info!("Starting driver for browser worker {}", worker_id);
        Command::new(driver_fp)
            .arg("--port")
            .arg(driver_port.to_string())
            .stdin(Stdio::null())
            .spawn()
            .expect("Failed to start browser")
    }

    async fn renew_client(mut self) -> Result<Self> {
        debug!("Renewing client for browser worker {}", self.worker_id);
        self.browser.close_window().await?;
        self.browser.close().await?;
        self.driver_handle.kill().await?;
        self.proxy_handle.abort();
        let proxy_handle =
            Self::start_proxy_handle(self.worker_id, &self.main_client, self.socks_port);
        let driver_handle =
            Self::start_driver_handle(self.worker_id, &self.driver_fp, self.driver_port);
        let browser = Self::connect_browser(&self.firefox_opts, self.driver_port).await?;
        Ok(BrowserWorker {
            proxy_handle,
            driver_handle,
            browser,
            ..self
        })
    }

    async fn run_loop(mut self) -> Result<()> {
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
                WorkerType::Browser,
                job,
                &mut self.platform_data,
                &self.monitor,
                &self.requeue_job,
            )
            .await?
            {
                WorkerLogicAction::Nothing => {}
                WorkerLogicAction::RenewClient => {
                    self = self.renew_client().await?;
                }
                WorkerLogicAction::ProcessJob((ts_start, job)) => {
                    let platform_impl = self.platform_impls.get(&job.platform).unwrap();
                    let jobs = platform_impl.process_job(&job, &self.browser).await;
                    worker_job_logic_process(
                        ts_start,
                        self.worker_id,
                        WorkerType::Browser,
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

    pub(crate) async fn start(self) -> Result<()> {
        info!("Starting browser worker {}", self.worker_id);
        let worker_id = self.worker_id;
        match self.run_loop().await {
            Ok(_) => {
                info!("Finished browser worker {}", worker_id);
            }
            Err(e) => {
                panic!("Failed in browser worker {}: {}", worker_id, e);
            }
        }
        info!("Stopping browser worker {}", worker_id);
        Ok(())
    }
}
