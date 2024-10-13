use crate::Platform;
use concurrent_tor::{
    execution::{
        browser::{BrowserPlatform, BrowserPlatformBuilder},
        scheduler::{Job, NotRequested, QueueJob, WorkerRequest},
    },
    exports::{async_trait,  json_from_str, json_to_string},
    Result,
};
use log::{info};
use serde::{Deserialize, Serialize};
use std::{any::Any, };
use concurrent_tor::exports::fantoccini;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct MyHeadlessBrowserRequest {
    pub id: usize,
    pub url: String,
}

impl MyHeadlessBrowserRequest {
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

impl WorkerRequest for MyHeadlessBrowserRequest {
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

pub struct MyHeadlessBrowser {}

#[async_trait]
impl BrowserPlatform<Platform> for MyHeadlessBrowser {
    async fn process_job(
        &self,
        job: &Job<NotRequested, Platform>,
        client: &fantoccini::Client,
    ) -> Vec<QueueJob<Platform>> {
        let req: &MyHeadlessBrowserRequest = job.request.as_any().downcast_ref().unwrap();
        info!("Processing headless browser request: {:?}", req);

        client.goto(&req.url).await.expect("Failed to navigate to url");
        let ip = client.source().await.expect("Failed to get source");
        info!("Browser headless request return ip: {}", ip);

        vec![QueueJob::Completed(job.into())]
    }
}

pub struct MyHeadlessBrowserBuilder {}

impl MyHeadlessBrowserBuilder {
    pub fn new() -> Self {
        Self {}
    }
}

impl BrowserPlatformBuilder<Platform> for MyHeadlessBrowserBuilder {
    fn platform(&self) -> Platform {
        Platform::MyHeadlessBrowser
    }

    fn build(&self) -> Box<dyn BrowserPlatform<Platform>> {
        Box::new(MyHeadlessBrowser {})
    }
}
