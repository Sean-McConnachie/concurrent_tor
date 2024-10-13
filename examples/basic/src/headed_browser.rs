use crate::Platform;
use concurrent_tor::{
    execution::{
        browser::{BrowserPlatform, BrowserPlatformBuilder},
        scheduler::{Job, NotRequested, QueueJob, WorkerRequest},
    },
    exports::{async_trait, json_from_str, json_to_string},
    Result,
};
use log::{info};
use serde::{Deserialize, Serialize};
use std::{any::Any, };
use concurrent_tor::exports::fantoccini;
use concurrent_tor::exports::fantoccini::Locator;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct MyHeadedBrowserRequest {
    pub id: usize,
    pub url: String,
}

impl MyHeadedBrowserRequest {
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

impl WorkerRequest for MyHeadedBrowserRequest {
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

pub struct MyHeadedBrowser {}

#[async_trait]
impl BrowserPlatform<Platform> for MyHeadedBrowser {
    async fn process_job(
        &self,
        job: &Job<NotRequested, Platform>,
        client: &fantoccini::Client,
    ) -> Vec<QueueJob<Platform>> {
        let req: &MyHeadedBrowserRequest = job.request.as_any().downcast_ref().unwrap();
        info!("Processing headed browser request: {:?}", req);

        client.goto(&req.url).await.expect("Failed to navigate to url");
        let ip = client
            .find(Locator::Css("#ipv4 > a:nth-child(1)"))
            .await.expect("Failed to find element");
        let ip = ip.text().await.expect("Failed to get text");
        info!("Browser headed request return ip: {}", ip);

        vec![QueueJob::Completed(job.into())]
    }
}

pub struct MyHeadedBrowserBuilder {}

impl MyHeadedBrowserBuilder {
    pub fn new() -> Self {
        Self {}
    }
}

impl BrowserPlatformBuilder<Platform> for MyHeadedBrowserBuilder {
    fn platform(&self) -> Platform {
        Platform::MyHeadedBrowser
    }

    fn build(&self) -> Box<dyn BrowserPlatform<Platform>> {
        Box::new(MyHeadedBrowser {})
    }
}
