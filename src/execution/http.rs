use crate::{
    config::HttpPlatformConfig,
    execution::{
        client::{
            worker_job_logic_process, worker_job_logic_start, Client, MainClient,
            WorkerLogicAction, WorkerType,
        },
        monitor::Event,
        scheduler::{
            Job, NotRequested, PlatformCanRequest, PlatformHistory, PlatformT, QueueJob,
            QueueJobStatus, Requested, WorkerAction,
        },
    },
    quanta_zero, Result,
};
use async_channel::{Receiver, Sender};
use async_trait::async_trait;
use hyper::StatusCode;
use log::{debug, info};
use std::collections::HashMap;

pub trait HttpPlatformBuilder<P: PlatformT, C: Client>: Send {
    fn platform(&self) -> P;
    fn build(&self) -> Box<dyn HttpPlatform<P, C>>;
}

#[async_trait]
pub trait HttpPlatform<P: PlatformT, C: Client>: Send {
    /// Function should not fail when passed back to the API. Therefore, it should handle all errors itself.
    async fn process_job(&self, job: &Job<NotRequested, P>, client: &C) -> Vec<QueueJob<P>>;
}

#[derive(Debug)]
pub struct HttpResponse {
    pub status: StatusCode,
    pub headers: HashMap<String, String>,
    pub body: String,
}

#[derive(Debug)]
pub struct HttpJobResponse<P: PlatformT> {
    pub job: Job<Requested, P>,
    pub body: Result<HttpResponse>,
}

pub(crate) struct HttpPlatformData {
    config: HttpPlatformConfig,
    last_request: quanta::Instant,
    requests: u32,
}

impl HttpPlatformData {
    pub fn new(config: HttpPlatformConfig) -> Self {
        HttpPlatformData {
            config,
            last_request: quanta_zero(),
            requests: 0,
        }
    }
}

impl PlatformHistory for HttpPlatformData {
    fn can_request(&self, now: quanta::Instant) -> PlatformCanRequest {
        let diff = now - self.last_request;
        if self.requests >= self.config.max_requests {
            return PlatformCanRequest::MaxRequests;
        }
        if diff.as_millis() > self.config.timeout_ms as u128 {
            return PlatformCanRequest::Ok;
        }
        PlatformCanRequest::MustWait
    }

    fn request_complete(&mut self, now: quanta::Instant) {
        self.requests += 1;
        self.last_request = now;
    }

    fn reset(&mut self) {
        self.requests = 0;
        self.last_request = quanta_zero();
    }
}

pub(crate) struct HttpWorker<P: PlatformT, C: Client, M: MainClient<C>> {
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
    /// Tor client to make requests
    client: C,
    /// Here to query a new client when necessary
    main_client: M,
    /// Platforms that store [HttpPlatform], the last request time, and the number of requests.
    platform_data: HashMap<P, HttpPlatformData>,
    /// The platform implementation
    platform_impls: HashMap<P, Box<dyn HttpPlatform<P, C>>>,
}

impl<P, C, M> HttpWorker<P, C, M>
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
        platform_data: HashMap<P, HttpPlatformData>,
        platform_impls: HashMap<P, Box<dyn HttpPlatform<P, C>>>,
    ) -> Self {
        debug!("Creating http worker {}", worker_id);
        HttpWorker {
            worker_id,
            monitor,
            request_job,
            recv_job,
            requeue_job,
            queue_job,
            client: main_client.isolated_client(),
            main_client,
            platform_data,
            platform_impls,
        }
    }

    fn renew_client(self) -> Result<Self> {
        debug!("Renewing client for http worker {}", self.worker_id);
        let client = self.main_client.isolated_client();
        Ok(HttpWorker { client, ..self })
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
                WorkerType::Http,
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
                    let jobs = self
                        .platform_impls
                        .get(&job.platform)
                        .unwrap()
                        .process_job(&job, &self.client)
                        .await;
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

    pub(crate) async fn start(self) -> Result<()> {
        info!("Starting http worker {}", self.worker_id);
        let worker_id = self.worker_id;
        match self.run_loop().await {
            Ok(_) => info!("Finished http worker {}", worker_id),
            Err(e) => panic!("Failed in http worker {}: {:?}", worker_id, e),
        }
        info!("Stopping http worker {}", worker_id);
        Ok(())
    }
}
