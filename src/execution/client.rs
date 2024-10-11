use crate::{execution::http::HttpResponse, Result};
use anyhow::anyhow;
use arti::socks::run_socks_proxy;
use arti_client::{TorClient, TorClientConfig};
use async_trait::async_trait;
use http_body_util::{BodyExt, Empty};
use hyper::{body::Bytes, http::uri::Scheme, Method, Request, Uri};
use hyper_util::rt::TokioIo;
use log::{debug, error};
use reqwest::Url;
use std::str::FromStr;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::task::JoinHandle;
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
    ) -> Result<HttpResponse>;
}

pub trait MainClient<C: Client>: Send + Sync + Clone {
    fn use_proxy() -> bool;
    fn isolated_client(&self) -> C;
}

pub struct CTorClient {
    client: TorClient<PreferredRuntime>,
}

impl CTorClient {
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
            Self::query_request(host, headers, method, stream).await
        } else {
            Self::query_request(host, headers, method, stream).await
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
    ) -> Result<HttpResponse> {
        let url = Url::parse(uri).map_err(|e| anyhow!("Failed to parse URI: {:?}", e))?;
        let mut request = self.client.request(method, url);
        if let Some(headers) = headers {
            for (key, value) in headers.iter() {
                request = request.header(key, value);
            }
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
