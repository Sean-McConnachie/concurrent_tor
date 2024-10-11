use crate::{
    database::JobStatusDb,
    execution::{
        http::HttpResponse,
        monitor::{BasicWorkerInfo, Event, Monitor, ProcessedJobInfo},
        scheduler::{
            Job, NotRequested, PlatformCanRequest, PlatformHistory, PlatformT, QueueJob,
            QueueJobStatus, WorkerAction,
        },
    },
    Result,
};
use anyhow::anyhow;
use arti::socks::run_socks_proxy;
use arti_client::{TorClient, TorClientConfig};
use async_channel::Sender;
use async_trait::async_trait;
use futures_util::TryFutureExt;
use http_body_util::{BodyExt, Empty};
use hyper::{
    body::{Body, Bytes},
    http::uri::Scheme,
    Method, Request, Uri,
};
use hyper_util::rt::TokioIo;
use log::{debug, error};
use reqwest::Url;
use std::{
    collections::HashMap,
    fmt::Debug,
    future::Future,
    str::FromStr,
    sync::{Arc, Mutex},
};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    task::JoinHandle,
};
use tokio_native_tls::native_tls::TlsConnector;
use tor_config::Listen;
use tor_rtcompat::PreferredRuntime;

#[async_trait]
pub trait Client: Send + Sync {
    fn start_proxy(self, port: u16) -> Option<JoinHandle<()>>;

    async fn make_request(
        &self,
        method: Method,
        uri: &str,
        headers: Option<HashMap<String, String>>,
        body: Option<String>,
    ) -> Result<HttpResponse>;
}

pub trait MainClient<C: Client>: Send + Sync + Clone {
    fn use_proxy() -> bool;
    fn isolated_client(&self) -> C;
}

#[derive(Debug, Clone, Copy)]
pub enum WorkerType {
    Http,
    Browser,
}

pub struct CTorClient {
    client: TorClient<PreferredRuntime>,
}

impl CTorClient {
    async fn build_request(
        host: &str,
        headers: Option<HashMap<String, String>>,
        method: Method,
        stream: impl AsyncRead + AsyncWrite + Unpin + Send + 'static,
        body: Option<String>,
    ) -> Result<HttpResponse> {
        let mut request = Request::builder().header("Host", host).method(method);
        if let Some(headers) = headers {
            for (key, value) in headers.iter() {
                request = request.header(key, value);
            }
        }
        if let Some(body) = body {
            let req = request.body(body)?;
            Self::query_request(stream, req).await
        } else {
            let req = request.body(Empty::<Bytes>::new())?;
            Self::query_request(stream, req).await
        }
    }

    async fn query_request<T: Body + Send + 'static>(
        stream: impl AsyncRead + AsyncWrite + Unpin + Send + 'static,
        request: Request<T>,
    ) -> Result<HttpResponse>
    where
        <T as Body>::Data: Send + 'static,
        <T as Body>::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
    {
        let (mut request_sender, connection) =
            hyper::client::conn::http1::handshake(TokioIo::new(stream)).await?;
        tokio::spawn(async move { connection.await });

        let mut resp = request_sender.send_request(request).await?;
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

#[async_trait]
impl Client for CTorClient {
    fn start_proxy(self, port: u16) -> Option<JoinHandle<()>> {
        let listen = Listen::new_localhost(port);
        let client = self.client;
        Some(tokio::spawn(async {
            match run_socks_proxy(client.runtime().clone(), client, listen).await {
                Ok(_) => debug!("Proxy exited successfully"),
                Err(e) => error!("Proxy exited with error: {:?}", e),
            }
        }))
    }

    async fn make_request(
        &self,
        method: Method,
        uri: &str,
        headers: Option<HashMap<String, String>>,
        body: Option<String>,
    ) -> Result<HttpResponse> {
        let uri = Uri::from_str(uri).map_err(|e| anyhow!("Failed to parse URI: {:?}", e))?;
        let host = uri.host().unwrap();
        let https = uri.scheme() == Some(&Scheme::HTTPS);
        let port = match uri.port_u16() {
            Some(port) => port,
            _ if https => 443,
            _ => 80,
        };

        let stream = self.client.connect((host, port)).await?;
        if https {
            let cx = TlsConnector::builder().build()?;
            let cx = tokio_native_tls::TlsConnector::from(cx);
            let stream = cx.connect(host, stream).await?;
            Self::build_request(host, headers, method, stream, body).await
        } else {
            Self::build_request(host, headers, method, stream, body).await
        }
    }
}

#[derive(Clone)]
pub struct MainCTorClient {
    client: Arc<Mutex<TorClient<PreferredRuntime>>>,
}

impl MainClient<CTorClient> for MainCTorClient {
    fn use_proxy() -> bool {
        true
    }

    fn isolated_client(&self) -> CTorClient {
        CTorClient {
            client: self.client.lock().unwrap().isolated_client(),
        }
    }
}

impl MainCTorClient {
    pub async fn new(config: TorClientConfig) -> Result<Self> {
        Ok(MainCTorClient {
            client: Arc::new(Mutex::new(TorClient::create_bootstrapped(config).await?)),
        })
    }

    pub fn isolated_client(&self) -> TorClient<PreferredRuntime> {
        self.client.lock().unwrap().isolated_client()
    }
}

pub struct CStandardClient {
    client: reqwest::Client,
}

#[async_trait]
impl Client for CStandardClient {
    fn start_proxy(self, #[allow(unused_variables)] port: u16) -> Option<JoinHandle<()>> {
        unreachable!("Standard client does not support proxies")
    }

    async fn make_request(
        &self,
        method: Method,
        uri: &str,
        headers: Option<HashMap<String, String>>,
        body: Option<String>,
    ) -> Result<HttpResponse> {
        let url = Url::parse(uri).map_err(|e| anyhow!("Failed to parse URI: {:?}", e))?;
        let mut request = self.client.request(method, url);
        if let Some(headers) = headers {
            for (key, value) in headers.iter() {
                request = request.header(key, value);
            }
        }
        if let Some(body) = body {
            request = request.body(body);
        }

        let resp = self.client.execute(request.build()?).await?;
        let status = resp.status();
        let headers = resp
            .headers()
            .iter()
            .map(|(k, v)| (k.as_str().to_string(), v.to_str().unwrap().to_string()))
            .collect();
        let body = resp.text().await?;
        Ok(HttpResponse {
            status,
            headers,
            body,
        })
    }
}

#[derive(Clone)]
pub struct MainCStandardClient {}

impl MainCStandardClient {
    pub fn new() -> Self {
        MainCStandardClient {}
    }
}

impl MainClient<CStandardClient> for MainCStandardClient {
    fn use_proxy() -> bool {
        false
    }

    fn isolated_client(&self) -> CStandardClient {
        CStandardClient {
            client: reqwest::Client::new(),
        }
    }
}

pub type StartInstant = quanta::Instant;

pub enum WorkerLogicAction<P: PlatformT> {
    RenewClient,
    Nothing,
    ProcessJob((StartInstant, Job<NotRequested, P>)),
}

pub async fn worker_job_logic_start<P, D>(
    worker_id: u16,
    job: Job<NotRequested, P>,
    platforms: &mut HashMap<P, D>,
    monitor: &Sender<Event<P>>,
    requeue_job: &Sender<WorkerAction<P>>,
) -> Result<WorkerLogicAction<P>>
where
    P: PlatformT,
    D: PlatformHistory,
{
    let ts_start = quanta::Instant::now();
    let job_platform = job.platform;
    let platform = platforms.get_mut(&job.platform).unwrap();
    match platform.can_request(ts_start) {
        PlatformCanRequest::Ok => {
            return Ok(WorkerLogicAction::ProcessJob((ts_start, job)));
        }
        PlatformCanRequest::MustWait => {
            debug!("Rate limiting for http worker {}", worker_id);
            // Return the job so another worker can process it, or we can retry later

            requeue_job
                .send(WorkerAction::Job(job))
                .map_err(|e| {
                    anyhow!(
                        "Failed to requeue job in http worker {}: {:?}",
                        worker_id,
                        e
                    )
                })
                .await?;

            monitor
                .send(Event::WorkerRateLimited(BasicWorkerInfo::new(
                    job_platform,
                    WorkerType::Http,
                    worker_id,
                    quanta::Instant::now(),
                )))
                .await?;
        }
        PlatformCanRequest::MaxRequests => {
            debug!("Max requests for http worker {}", worker_id);
            // Return the job so another worker can process it, or we can retry later

            requeue_job
                .send(WorkerAction::Job(job))
                .map_err(|e| {
                    anyhow!(
                        "Failed to requeue job in http worker {}: {:?}",
                        worker_id,
                        e
                    )
                })
                .await?;

            // Reset all platforms so we can make more requests
            for (_, platform) in platforms.iter_mut() {
                platform.reset();
            }

            monitor
                .send(Event::WorkerRenewingClient(BasicWorkerInfo::new(
                    job_platform,
                    WorkerType::Http,
                    worker_id,
                    quanta::Instant::now(),
                )))
                .await?;
            return Ok(WorkerLogicAction::RenewClient);
        }
    }
    Ok(WorkerLogicAction::Nothing)
}

pub async fn worker_job_logic_process<P, D>(
    ts_start: StartInstant,
    worker_id: u16,
    worker_type: WorkerType,
    original_job: Job<NotRequested, P>,
    new_jobs: Vec<QueueJob<P>>,
    platforms: &mut HashMap<P, D>,
    monitor: &Sender<Event<P>>,
    queue_job: &Sender<QueueJob<P>>,
    request_job: &Sender<QueueJobStatus>,
) -> Result<()>
where
    P: PlatformT,
    D: PlatformHistory,
{
    let platform = platforms.get_mut(&original_job.platform).unwrap();
    let ts_end = quanta::Instant::now();
    platform.request_complete(ts_end);

    let job_hash = original_job.request.hash()?;

    let mut original_job_completed = false;
    for job in new_jobs {
        let (current_job_hash, num_attempts, max_attempts) = job
            .inner_job_info()?
            .expect("Bad QueueJob sent to ConcurrentTor runtime.");
        if current_job_hash == job_hash {
            // We found the original job
            if !original_job_completed {
                // We haven't found the original job in a previous iteration
                original_job_completed = true;
                if num_attempts != original_job.num_attempts + 1 {
                    panic!(
                        "Original job has wrong number of attempts in http worker {}. \
                        Expected {}, got {}. Do not forgot to call .into::<Job<Requests, P>>()!",
                        worker_id,
                        original_job.num_attempts + 1,
                        num_attempts
                    );
                }
                monitor
                    .send(Event::ProcessedJob(ProcessedJobInfo::new(
                        original_job.platform,
                        worker_type,
                        worker_id,
                        ts_start,
                        ts_end,
                        job_hash,
                        JobStatusDb::Active,
                        num_attempts,
                        max_attempts,
                    )))
                    .await?;
            } else {
                panic!("Original job found twice in http worker {}", worker_id);
            }
        }

        queue_job
            .send(job)
            .map_err(|e| {
                anyhow!(
                    "Failed to send job to queue in http worker {}: {:?}",
                    worker_id,
                    e
                )
            })
            .await?;
    }

    if !original_job_completed {
        panic!(
            "Original job not found in http worker {}. You must include the \
                        original job with its updated status in the return vector! ",
            worker_id
        );
    }

    request_job
        .send(QueueJobStatus::WorkerCompleted { worker_id })
        .map_err(|e| {
            anyhow!(
                "Failed to send worker completed status in http worker {}: {:?}",
                worker_id,
                e
            )
        })
        .await?;
    Ok(())
}
