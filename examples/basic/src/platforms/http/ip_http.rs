use concurrent_tor::{
    execution::{
        http::{HttpPlatform, HttpPlatformBuilder},
        scheduler::{Job, NotRequested, QueueJob, WorkerRequest},
    },
    exports::*,
    Result,
};
use serde::{Deserialize, Serialize};
use std::{any::Any, io::Cursor};

use crate::platforms::Platform;

#[derive(Debug, Serialize, Deserialize)]
pub struct IpHttpRequest {
    url: String,
}

impl IpHttpRequest {
    pub fn from_json(json: &str) -> Result<Box<dyn WorkerRequest>> {
        let s: Result<Self> = json_from_str(json).map_err(|e| e.into());
        Ok(Box::new(s?))
    }
}

impl WorkerRequest for IpHttpRequest {
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

pub struct IpHttp {}

impl IpHttp {
    pub fn new() -> Self {
        Self {}
    }
}

#[async_trait]
impl HttpPlatform<Platform> for IpHttp {
    fn process_job(
        &self,
        job: Job<NotRequested, Platform>,
        client: &TorClient<TorRuntime>,
    ) -> Vec<QueueJob<Platform>> {
        let job: &IpHttpRequest = job.request.as_any().downcast_ref().unwrap();
        println!("IpHttp job response: {:?}", job);
        vec![]
    }
}

pub struct IpHttpBuilder {}

impl IpHttpBuilder {
    pub fn new() -> Self {
        Self {}
    }
}

impl HttpPlatformBuilder<Platform> for IpHttpBuilder {
    fn platform(&self) -> Platform {
        Platform::IpHttp
    }

    fn build(&self) -> Box<dyn HttpPlatform<Platform>> {
        Box::new(IpHttp::new())
    }
}
