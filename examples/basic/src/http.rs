use crate::{backend::ClientBackend, Platform};
use concurrent_tor::{
    execution::{
        client::Client,
        http::{HttpPlatform, HttpPlatformBuilder},
        scheduler::{Job, NotRequested, QueueJob, Requested, WorkerRequest},
    },
    exports::{async_trait, json_from_str, json_to_string, HttpMethod},
    Result,
};
use log::{error, info};
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
            // Random id for the hash since the url is the same for all requests in this case.
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

pub struct MyHttp {}

#[async_trait]
impl HttpPlatform<Platform, ClientBackend> for MyHttp {
    async fn process_job(
        &self,
        job: &Job<NotRequested, Platform>,
        client: &ClientBackend,
    ) -> Vec<QueueJob<Platform>> {
        let req: &MyHttpRequest = job.request.as_any().downcast_ref().unwrap();
        let resp = client
            .make_request(HttpMethod::GET, &req.url, None, None)
            .await
            .unwrap();
        let completed: Job<Requested, Platform> = job.into();
        if resp.status.is_success() {
            info!("Http request return IP: {}", resp.body);
            vec![QueueJob::Completed(completed)]
        } else {
            error!(
                "Http request failed [{:?}]. Body: {}",
                resp.status, resp.body
            );
            vec![QueueJob::Retry(completed)]
        }
    }
}

pub struct MyHttpBuilder {}

impl MyHttpBuilder {
    pub fn new() -> Self {
        Self {}
    }
}

impl HttpPlatformBuilder<Platform, ClientBackend> for MyHttpBuilder {
    fn platform(&self) -> Platform {
        Platform::MyHttp
    }

    fn build(&self) -> Box<dyn HttpPlatform<Platform, ClientBackend>> {
        Box::new(MyHttp {})
    }
}
