use crate::{
    config::HttpPlatformConfig,
    execution::{
        client::{Client, MainClient},
        scheduler::{Job, NotRequested, PlatformT, QueueJob, QueueJobStatus, Requested},
    },
    Result,
};
use async_trait::async_trait;
use chrono::{DateTime, TimeZone, Utc};
use crossbeam::channel::{Receiver, Sender};
use hyper::StatusCode;
use log::debug;
use std::collections::HashMap;

pub trait HttpPlatformBuilder<P: PlatformT, C: Client> {
    fn platform(&self) -> P;
    fn build(&self) -> Box<dyn HttpPlatform<P, C>>;
}

#[async_trait]
pub trait HttpPlatform<P: PlatformT, C: Client>: Send {
    /// Function should not fail when passed back to the API. Therefore, it should handle all errors itself.
    fn process_job(&self, job: Job<NotRequested, P>, client: &C) -> Vec<QueueJob<P>>;
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

enum HttpPlatformBehaviourError {
    Ok,
    MustWait,
    MaxRequests,
}

pub struct HttpPlatformData<P: PlatformT, C: Client> {
    platform_impl: Box<dyn HttpPlatform<P, C>>,
    config: HttpPlatformConfig,
    last_request: DateTime<Utc>,
    requests: u32,
}

impl<P, C> HttpPlatformData<P, C>
where
    P: PlatformT,
    C: Client,
{
    pub fn new(platform_impl: Box<dyn HttpPlatform<P, C>>, config: HttpPlatformConfig) -> Self {
        HttpPlatformData {
            platform_impl,
            config,
            last_request: Utc.timestamp_opt(0, 0).unwrap(),
            requests: 0,
        }
    }

    fn can_request(&self) -> HttpPlatformBehaviourError {
        let now = Utc::now();
        let diff = now - self.last_request;
        if self.requests >= self.config.max_requests {
            return HttpPlatformBehaviourError::MaxRequests;
        }
        if diff.num_seconds() > self.config.timeout as i64 {
            return HttpPlatformBehaviourError::Ok;
        }
        HttpPlatformBehaviourError::MustWait
    }

    fn request_complete(&mut self) {
        self.requests += 1;
        self.last_request = Utc::now();
    }

    fn reset(&mut self) {
        self.last_request = Utc.timestamp_opt(0, 0).unwrap();
        self.requests = 0;
    }
}

pub struct HttpWorker<P: PlatformT, C: Client, M: MainClient<C>> {
    worker_id: u32,
    /// Request job from a queue
    request_job: Sender<QueueJobStatus>,
    /// Receive job from a queue
    recv_job: Receiver<Job<NotRequested, P>>,
    /// Add job back to queue if the worker is unable to process it (possibly due to rate limiting)
    requeue_job: Sender<Job<NotRequested, P>>,
    /// Add a new, or existing job to the queue
    queue_job: Sender<QueueJob<P>>,
    /// Tor client to make requests
    client: C,
    /// Here to query a new client when necessary
    main_client: M,
    /// Platforms that store [HttpPlatform], the last request time, and the number of requests.
    platforms: HashMap<P, HttpPlatformData<P, C>>,
}

impl<P, C, M> HttpWorker<P, C, M>
where
    P: PlatformT,
    C: Client,
    M: MainClient<C>,
{
    pub fn new(
        worker_id: u32,
        request_job: Sender<QueueJobStatus>,
        recv_job: Receiver<Job<NotRequested, P>>,
        requeue_job: Sender<Job<NotRequested, P>>,
        queue_job: Sender<QueueJob<P>>,
        main_client: M,
        platforms: HashMap<P, HttpPlatformData<P, C>>,
    ) -> Self {
        debug!("Creating http worker {}", worker_id);
        HttpWorker {
            worker_id,
            request_job,
            recv_job,
            requeue_job,
            queue_job,
            client: main_client.isolated_client(),
            main_client,
            platforms,
        }
    }

    fn renew_client(self) -> Result<Self> {
        debug!("Renewing client for http worker {}", self.worker_id);
        let client = self.main_client.isolated_client();
        Ok(HttpWorker { client, ..self })
    }

    pub(crate) async fn start(mut self) -> Result<()> {
        debug!("Starting worker {}", self.worker_id);
        loop {
            let job = self.recv_job.recv()?;
            let platform = self.platforms.get_mut(&job.platform).unwrap();
            match platform.can_request() {
                HttpPlatformBehaviourError::Ok => {
                    debug!("Processing job for http worker {}", self.worker_id);
                    let jobs = platform.platform_impl.process_job(job, &self.client);
                    platform.request_complete();
                    for job in jobs {
                        self.queue_job.send(job)?;
                    }
                    self.request_job.send(QueueJobStatus::WorkerCompleted {
                        worker_id: self.worker_id,
                    })?;
                }
                HttpPlatformBehaviourError::MustWait => {
                    debug!("Rate limiting for http worker {}", self.worker_id);
                    // Return the job so another worker can process it, or we can retry later
                    // TODO: Add sleep?
                    self.requeue_job.send(job)?;
                }
                HttpPlatformBehaviourError::MaxRequests => {
                    debug!("Max requests for http worker {}", self.worker_id);
                    // Return the job so another worker can process it, or we can retry later
                    self.requeue_job.send(job)?;
                    // Renew the client so we get a new IP
                    self = self.renew_client()?;
                    // Reset all platforms so we can make more requests
                    for (_, platform) in self.platforms.iter_mut() {
                        platform.reset();
                    }
                }
            }
        }
    }
}
