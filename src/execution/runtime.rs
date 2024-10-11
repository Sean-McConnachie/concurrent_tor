use crate::{
    config::{BrowserPlatformConfig, HttpPlatformConfig, WorkerConfig},
    database::connect_and_init_db,
    execution::{
        browser::{BrowserPlatformBuilder, BrowserPlatformData, BrowserWorker},
        client::{Client, MainClient},
        cron::CronPlatformBuilder,
        http::{HttpPlatformBuilder, HttpPlatformData, HttpWorker},
        monitor::{Event, Monitor},
        scheduler::{JobDistributor, PlatformT, QueueJob, QueueJobStatus, Scheduler, WorkerAction},
    },
    Result,
};
use anyhow::anyhow;
use async_channel::{unbounded, Sender};
use futures_util::TryFutureExt;
use log::info;
use std::future::Future;
use std::pin::Pin;
use std::{
    collections::HashMap,
    sync::{atomic::AtomicBool, Arc},
};
use tokio::task::JoinHandle;

pub struct CTRuntime<P: PlatformT> {
    event_tx: Sender<Event<P>>,
    stop_queue_worker: Sender<QueueJob<P>>,
    stop_cron_flag: Arc<AtomicBool>,
    queue_handles: [JoinHandle<Result<()>>; 2],
    join_handles: Vec<JoinHandle<Result<()>>>,
    monitor_handle: JoinHandle<Result<()>>,
}

impl<P> CTRuntime<P>
where
    P: PlatformT,
{
    pub async fn run<
        S: Scheduler<P>,
        C: Client + 'static,
        M: MainClient<C> + 'static,
        T: Monitor<P> + 'static,
    >(
        worker_config: WorkerConfig,
        monitor: T,
        scheduler: S,
        main_client: M,

        mut cron_platforms: Vec<Box<dyn CronPlatformBuilder<P>>>,

        http_platform_configs: HashMap<P, HttpPlatformConfig>,
        http_platforms: Vec<Box<dyn HttpPlatformBuilder<P, C>>>,

        browser_platform_configs: HashMap<P, BrowserPlatformConfig>,
        browser_platforms: Vec<Box<dyn BrowserPlatformBuilder<P>>>,
    ) -> Result<Self> {
        let pool = connect_and_init_db().await?;

        let (monitor_tx, monitor_rx) = unbounded::<Event<P>>();
        let (request_job_tx, request_job_rx) = unbounded::<QueueJobStatus>();
        let (queue_job_tx, queue_job_rx) = unbounded::<QueueJob<P>>();

        let (http_worker_tx, http_worker_rx) = unbounded::<WorkerAction<P>>();
        let (browser_worker_tx, browser_worker_rx) = unbounded::<WorkerAction<P>>();

        // Start monitor
        let monitor_handle = tokio::task::spawn(monitor.start(monitor_rx));

        // Build JobDistributor
        let job_distributor = {
            let mut txs = HashMap::new();
            for http_platform in http_platforms.iter() {
                txs.insert(http_platform.platform(), http_worker_tx.clone());
            }
            for browser_platform in browser_platforms.iter() {
                txs.insert(browser_platform.platform(), browser_worker_tx.clone());
            }
            JobDistributor::new(
                monitor_tx.clone(),
                pool,
                request_job_rx,
                request_job_tx.clone(),
                queue_job_rx,
                txs,
                http_worker_tx.clone(),
                browser_worker_tx.clone(),
                scheduler,
                worker_config.clone(),
            )
        };

        // Build Cron workers
        let stop_cron_flag = Arc::new(AtomicBool::new(false));
        for cron_platform in cron_platforms.iter_mut() {
            cron_platform.set_queue_job(queue_job_tx.clone());
            cron_platform.set_stop_flag(stop_cron_flag.clone());
        }
        let cron_workers = cron_platforms
            .into_iter()
            .map(|builder| builder.build())
            .collect::<Vec<_>>();

        // Build HTTP workers
        let http_workers = (0..worker_config.http_workers)
            .map(|worker_id| {
                let platforms = http_platforms
                    .iter()
                    .map(|builder| {
                        let platform = builder.platform();
                        (
                            platform.clone(),
                            HttpPlatformData::new(
                                builder.build(),
                                http_platform_configs.get(&platform).unwrap().clone(),
                            ),
                        )
                    })
                    .collect::<HashMap<_, _>>();
                HttpWorker::new(
                    worker_id,
                    monitor_tx.clone(),
                    request_job_tx.clone(),
                    http_worker_rx.clone(),
                    http_worker_tx.clone(),
                    queue_job_tx.clone(),
                    main_client.clone(),
                    platforms,
                )
            })
            .collect::<Vec<_>>();

        // Build Browser workers
        const STARTING_PORT: u16 = 9050;
        let browser_workers: Vec<BrowserWorker<P, C, M>> = (worker_config.http_workers
            ..worker_config.http_workers + worker_config.browser_workers)
            .map(|worker_id| {
                let platforms = browser_platforms
                    .iter()
                    .map(|builder| {
                        let platform = builder.platform();
                        (
                            platform.clone(),
                            BrowserPlatformData::new(
                                builder.build(),
                                browser_platform_configs.get(&platform).unwrap().clone(),
                            ),
                        )
                    })
                    .collect::<HashMap<_, _>>();
                let socks_port = STARTING_PORT + worker_id as u16;
                BrowserWorker::new(
                    worker_id,
                    monitor_tx.clone(),
                    request_job_tx.clone(),
                    browser_worker_rx.clone(),
                    browser_worker_tx.clone(),
                    queue_job_tx.clone(),
                    main_client.clone(),
                    platforms,
                    true, // TODO: Make this configurable
                    socks_port,
                )
                .unwrap()
            })
            .collect();

        const QUEUE_WORKERS: usize = 2;
        let mut handles = Vec::with_capacity(http_workers.len() + QUEUE_WORKERS);
        info!("Starting {} cron platforms", cron_workers.len());
        for platform in cron_workers {
            handles.push(tokio::task::spawn(platform.start()));
        }
        info!("Starting {} http workers", http_workers.len());
        for worker in http_workers {
            handles.push(tokio::task::spawn(worker.start()));
        }
        info!("Starting {} browser workers", browser_workers.len());
        for worker in browser_workers {
            handles.push(tokio::task::spawn(worker.start()));
        }
        info!("Starting queue");
        let queue_handles = job_distributor.start();

        Ok(CTRuntime {
            event_tx: monitor_tx,
            stop_queue_worker: queue_job_tx,
            stop_cron_flag,
            queue_handles,
            join_handles: handles,
            monitor_handle,
        })
    }

    pub async fn join(self) -> Result<()> {
        let n_worker_handles = self.join_handles.len();
        for (i, handle) in self.join_handles.into_iter().enumerate() {
            handle.await??;
            info!("Joined worker handle {}/{n_worker_handles}", i + 1);
        }
        self.stop_queue_worker
            .send(QueueJob::StopProgram)
            .map_err(|e| anyhow!("Failed to send stop program to queue in runtime: {:?}", e))
            .await?;
        let n_queue_handles = self.queue_handles.len();
        for (i, handle) in self.queue_handles.into_iter().enumerate() {
            handle.await??;
            info!("Joined queue handle {}/{n_queue_handles}", i + 1);
        }
        self.event_tx
            .send(Event::StopMonitor)
            .map_err(|e| anyhow!("Failed to send stop event to monitor: {:?}", e))
            .await?;
        self.monitor_handle.await??;
        info!("Joined monitor handle");
        Ok(())
    }

    pub fn graceful_stop(
        &self,
    ) -> impl FnOnce() -> Pin<Box<dyn Future<Output = Result<()>> + Send>> {
        let stop_queue_worker = self.stop_queue_worker.clone();
        let stop_cron_flag = self.stop_cron_flag.clone();

        // Return a closure that, when called, will return a boxed async future
        move || {
            let stop_queue_worker = stop_queue_worker.clone();
            let stop_cron_flag = stop_cron_flag.clone();

            // Create the async block
            Box::pin(async move {
                if stop_cron_flag.load(std::sync::atomic::Ordering::Relaxed) {
                    info!("Cron flag is already set to stop");
                    return Ok(());
                }
                info!("Stopping program");
                stop_cron_flag.store(true, std::sync::atomic::Ordering::Relaxed);
                stop_queue_worker
                    .send(QueueJob::SendStopProgram)
                    .map_err(|e| {
                        anyhow!("Failed to send stop program to queue in runtime: {:?}", e)
                    })
                    .await?;
                Ok(())
            })
        }
    }
}
