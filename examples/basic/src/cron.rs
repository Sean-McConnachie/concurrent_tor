use crate::{
    headed_browser::MyHeadedBrowserRequest, headless_browser::MyHeadlessBrowserRequest,
    http::MyHttpRequest, Platform,
};
use concurrent_tor::{
    execution::{
        cron::{CronPlatform, CronPlatformBuilder},
        scheduler::{Job, JobHash, NotRequested, QueueJob, WorkerRequest},
    },
    exports::{async_trait, AsyncChannelSender},
    Result,
};
use log::info;
use std::{
    sync::{atomic::AtomicBool, Arc},
    time::Duration,
};

pub struct Cron {
    sleep_ms: u64,
    stop_flag: Arc<AtomicBool>,
    queue_job: AsyncChannelSender<QueueJob<Platform>>,
}

impl Cron {
    fn build_http_job() -> (JobHash, Job<NotRequested, Platform>) {
        let req = MyHttpRequest::new("https://api.ipify.org/?format=json".to_string());
        let hash = req.hash().expect("Unable to hash");
        (hash, Job::init_from_box(Platform::MyHttp, req, 3))
    }

    fn build_headed_browser_job() -> (JobHash, Job<NotRequested, Platform>) {
        let req = MyHeadedBrowserRequest::new("https://whatismyipaddress.com/");
        let hash = req.hash().expect("Unable to hash");
        (hash, Job::init(Platform::MyHeadedBrowser, Box::new(req), 3))
    }

    fn build_headless_browser_job() -> (JobHash, Job<NotRequested, Platform>) {
        let req = MyHeadlessBrowserRequest::new("https://api.ipify.org/");
        let hash = req.hash().expect("Unable to hash");
        (
            hash,
            Job::init(Platform::MyHeadlessBrowser, Box::new(req), 3),
        )
    }
}

#[async_trait]
impl CronPlatform<Platform> for Cron {
    async fn start(self: Box<Self>) -> Result<()> {
        info!("Starting MyCron.",);
        loop {
            if self.stop_flag.load(std::sync::atomic::Ordering::Relaxed) {
                break;
            }

            for build_job in [
                move || {
                    info!("Sending http job.");
                    Self::build_http_job()
                },
                move || {
                    info!("Sending headed browser job.");
                    Self::build_headed_browser_job()
                },
                move || {
                    info!("Sending headless browser job.");
                    Self::build_headless_browser_job()
                },
            ]
            .iter()
            {
                let (_hash, job) = build_job();
                let job = QueueJob::New(job);
                self.queue_job.send(job).await.expect("Failed to send job");
                tokio::time::sleep(Duration::from_millis(self.sleep_ms)).await;
            }
        }
        info!("Stopping MyCron.");
        Ok(())
    }
}

pub struct MyCronBuilder {
    sleep_ms: u64,
    queue_job: Option<AsyncChannelSender<QueueJob<Platform>>>,
    stop_flag: Option<Arc<AtomicBool>>,
}

impl MyCronBuilder {
    pub fn new(sleep_ms: u64) -> Self {
        Self {
            sleep_ms,
            queue_job: None,
            stop_flag: None,
        }
    }
}

impl CronPlatformBuilder<Platform> for MyCronBuilder {
    fn set_queue_job(&mut self, queue_job: AsyncChannelSender<QueueJob<Platform>>) {
        self.queue_job = Some(queue_job);
    }

    fn set_stop_flag(&mut self, stop_flag: Arc<AtomicBool>) {
        self.stop_flag = Some(stop_flag);
    }

    fn build(&self) -> Box<dyn CronPlatform<Platform>> {
        Box::new(Cron {
            sleep_ms: self.sleep_ms,
            queue_job: self.queue_job.clone().unwrap(),
            stop_flag: self.stop_flag.clone().unwrap(),
        })
    }
}
