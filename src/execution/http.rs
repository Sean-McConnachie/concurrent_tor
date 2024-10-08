use arti_client::TorClient;
use async_trait::async_trait;
use chrono::{DateTime, TimeZone, Utc};
use crossbeam::channel::{Receiver, Sender};
use http_body_util::{BodyExt, Empty};
use hyper::body::Bytes;
use hyper::http::uri::Scheme;
use hyper::{Method, Request, StatusCode, Uri};
use hyper_util::rt::TokioIo;
use log::debug;
use std::collections::HashMap;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_native_tls::native_tls::TlsConnector;
use tor_rtcompat::PreferredRuntime;

use crate::config::HttpPlatformConfig;
use crate::execution::scheduler::{
    Job, MainTorClient, NotRequested, QueueJob, QueueJobStatus, Requested,
};
use crate::Result;

use super::scheduler::PlatformT;

pub trait HttpPlatformBuilder<P: PlatformT> {
    fn platform(&self) -> P;
    fn build(&self) -> Box<dyn HttpPlatform<P>>;
}

#[async_trait]
pub trait HttpPlatform<P: PlatformT>: Send {
    fn process_job(
        &self,
        job: Job<NotRequested, P>,
        client: &TorClient<PreferredRuntime>,
    ) -> Result<Vec<QueueJob<P>>>;
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

pub struct HttpPlatformData<P: PlatformT> {
    platform_impl: Box<dyn HttpPlatform<P>>,
    config: HttpPlatformConfig,
    last_request: DateTime<Utc>,
    requests: u32,
}

impl<P> HttpPlatformData<P>
where
    P: PlatformT,
{
    pub fn new(platform_impl: Box<dyn HttpPlatform<P>>, config: HttpPlatformConfig) -> Self {
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

    fn reset(self) -> Self {
        HttpPlatformData {
            last_request: Utc.timestamp_opt(0, 0).unwrap(),
            requests: 0,
            ..self
        }
    }
}

pub struct HttpWorker<P: PlatformT> {
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
    client: TorClient<PreferredRuntime>,
    /// Here to query a new client when necessary
    main_client: MainTorClient,
    /// Platforms that store [HttpPlatform], the last request time, and the number of requests.
    platforms: HashMap<P, HttpPlatformData<P>>,
}

impl<P> HttpWorker<P>
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
        platforms: HashMap<P, HttpPlatformData<P>>,
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
                    let jobs = platform.platform_impl.process_job(job, &self.client)?;
                    for job in jobs {
                        self.queue_job.send(job)?;
                    }
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
                }
            }
        }
    }

    async fn make_request(
        client: &TorClient<PreferredRuntime>,
        // &self,
        uri: &Uri,
        method: Method,
        headers: Option<HashMap<String, String>>,
    ) -> Result<HttpResponse> {
        let host = uri.host().unwrap();
        let https = uri.scheme() == Some(&Scheme::HTTPS);
        let port = match uri.port_u16() {
            Some(port) => port,
            _ if https => 443,
            _ => 80,
        };

        let stream = client.connect((host, port)).await?;
        if https {
            let cx = TlsConnector::builder().build()?;
            let cx = tokio_native_tls::TlsConnector::from(cx);
            let stream = cx.connect(host, stream).await?;
            Self::query_request(host, headers, method, stream).await
        } else {
            Self::query_request(host, headers, method, stream).await
        }
    }

    async fn query_request(
        host: &str,
        headers: Option<HashMap<String, String>>,
        method: Method,
        stream: impl AsyncRead + AsyncWrite + Unpin + Send + 'static,
    ) -> Result<HttpResponse> {
        let (mut request_sender, connection) =
            hyper::client::conn::http1::handshake(TokioIo::new(stream)).await?;

        tokio::spawn(async move { connection.await });

        let mut request = Request::builder().header("Host", host).method(method);
        if let Some(headers) = headers {
            for (key, value) in headers.iter() {
                request = request.header(key, value);
            }
        }

        let mut resp = request_sender
            .send_request(request.body(Empty::<Bytes>::new()).unwrap())
            .await?;

        let mut full_body = Vec::new();
        while let Some(frame) = resp.body_mut().frame().await {
            let bytes = frame?.into_data().unwrap();
            full_body.extend_from_slice(&bytes);
        }
        let body = String::from_utf8(full_body).unwrap();
        Ok(HttpResponse {
            status: resp.status(),
            headers: resp
                .headers()
                .iter()
                .map(|(k, v)| (k.as_str().to_string(), v.to_str().unwrap().to_string()))
                .collect(),
            body,
        })
    }
}
