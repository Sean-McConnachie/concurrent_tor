use concurrent_tor::{
    execution::{
        browser::{BrowserPlatform, BrowserPlatformBuilder},
        scheduler::{Job, NotRequested, QueueJob, WorkerRequest},
    },
    exports::*,
    Result,
};
use serde::{Deserialize, Serialize};
use std::{any::Any, io::Cursor};

use crate::platforms::Platform;

#[derive(Serialize, Deserialize, Debug)]
pub struct IpBrowserRequest {
    pub url: String,
}

impl IpBrowserRequest {
    pub fn from_json(json: &str) -> Result<Box<dyn WorkerRequest>> {
        let s: Result<Self> = json_from_str(json).map_err(|e| e.into());
        Ok(Box::new(s?))
    }
}

impl WorkerRequest for IpBrowserRequest {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn hash(&self) -> Result<String> {
        Ok(murmur3_x64_128(&mut Cursor::new(&self.url), 0)?.to_string())
    }

    fn as_json(&self) -> String {
        json_to_string(self).unwrap()
    }
}

pub struct IpBrowser {}

impl IpBrowser {
    pub fn new() -> Self {
        Self {}
    }
}

#[async_trait]
impl BrowserPlatform<Platform> for IpBrowser {
    async fn process_job(
        &self,
        job: Job<NotRequested, Platform>,
        tab: &headless_chrome::Tab,
    ) -> Vec<QueueJob<Platform>> {
        let job: &IpBrowserRequest = job.request.as_any().downcast_ref().unwrap();
        println!("IpBrowser job response: {:?}", job);
        vec![]
    }
}

pub struct IpBrowserBuilder {}

impl IpBrowserBuilder {
    pub fn new() -> Self {
        Self {}
    }
}

impl BrowserPlatformBuilder<Platform> for IpBrowserBuilder {
    fn platform(&self) -> Platform {
        Platform::IpBrowser
    }

    fn build(&self) -> Box<dyn BrowserPlatform<Platform>> {
        Box::new(IpBrowser::new())
    }
}
