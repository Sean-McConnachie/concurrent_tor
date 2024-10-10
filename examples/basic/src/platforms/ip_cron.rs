use concurrent_tor::{
    execution::{
        cron::CronPlatform,
        scheduler::{Job, QueueJob},
    },
    exports::{async_trait, CrossbeamSender},
    Result,
};
use std::time::Duration;
use tokio::time::sleep;

use crate::platforms::{ip_http::IpHttpRequest, Platform};

const IP_HTTP_URL: &str = "https://api.ipify.org?format=json";
const IP_BROWSER_URL: &str = "https://whatismyipaddress.com/";

pub struct IpCron {
    queue_job: Option<CrossbeamSender<QueueJob<Platform>>>,
}

impl IpCron {
    pub fn new() -> Self {
        Self { queue_job: None }
    }
}

#[async_trait]
impl CronPlatform<Platform> for IpCron {
    fn set_queue_job(&mut self, queue_job: CrossbeamSender<QueueJob<Platform>>) {
        self.queue_job = Some(queue_job);
    }

    async fn start(self: Box<Self>) -> Result<()> {
        let queue_job = self.queue_job.unwrap();
        loop {
            let req = IpHttpRequest {
                url: IP_HTTP_URL.to_string(),
            };
            let job = Job::init(Platform::IpHttp, Box::new(req));
            let job = QueueJob::New(job);
            queue_job.send(job).unwrap();
            println!("IpCron job sent");
            sleep(Duration::from_millis(1000)).await;
        }
    }
}
