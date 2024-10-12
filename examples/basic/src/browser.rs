use crate::Platform;
use concurrent_tor::{
    execution::{
        browser::{BrowserPlatform, BrowserPlatformBuilder},
        scheduler::{Job, NotRequested, QueueJob, Requested, WorkerRequest},
    },
    exports::{async_trait, headless_chrome::Tab, json_from_str, json_to_string},
    Result,
};
use log::{error, info};
use serde::{Deserialize, Serialize};
use std::{any::Any, sync::Arc};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct MyBrowserRequest {
    pub id: usize,
    pub url: String,
}

impl MyBrowserRequest {
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

impl WorkerRequest for MyBrowserRequest {
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

pub struct MyBrowser {}

#[async_trait]
impl BrowserPlatform<Platform> for MyBrowser {
    async fn process_job(
        &self,
        job: &Job<NotRequested, Platform>,
        tab: Arc<Tab>,
    ) -> Vec<QueueJob<Platform>> {
        let req: &MyBrowserRequest = job.request.as_any().downcast_ref().unwrap();
        info!("Processing browser request: {:?}", req);
        let url = req.url.clone();
        let handle = tokio::task::spawn_blocking(move || {
            tab.navigate_to(&url).unwrap();
            tab.wait_until_navigated().unwrap();
            let ip = tab
                .wait_for_element("#ipv4 > a:nth-child(1)")?
                .get_inner_text()?;
            Result::<_>::Ok(ip)
        });
        match handle.await.expect("Failed to execute browser handle") {
            Ok(ip) => {
                info!("Browser request return ip: {}", ip);
                let completed: Job<Requested, Platform> = job.into();
                vec![QueueJob::Completed(completed)]
            }
            Err(e) => {
                error!("Failed to get ip: {:?}", e);
                let retry: Job<Requested, Platform> = job.into();
                vec![QueueJob::Retry(retry)]
            }
        }
    }
}

pub struct MyBrowserBuilder {}

impl MyBrowserBuilder {
    pub fn new() -> Self {
        Self {}
    }
}

impl BrowserPlatformBuilder<Platform> for MyBrowserBuilder {
    fn platform(&self) -> Platform {
        Platform::MyBrowser
    }

    fn build(&self) -> Box<dyn BrowserPlatform<Platform>> {
        Box::new(MyBrowser {})
    }
}
