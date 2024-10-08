use crate::config::{BrowserPlatformConfig, HttpPlatformConfig, WorkerConfig};
use crate::execution::browser::{BrowserPlatformBuilder, BrowserPlatformData, BrowserWorker};
use crate::execution::cron::CronPlatform;
use crate::execution::http::{HttpPlatformBuilder, HttpPlatformData, HttpWorker};
use crate::execution::scheduler::{
    Job, JobDistributor, MainTorClient, NotRequested, QueueJob, QueueJobStatus, Scheduler,
};
use crate::Result;
use arti_client::TorClientConfig;
use crossbeam::channel::unbounded;
use log::info;
use std::collections::HashMap;

use super::scheduler::PlatformT;

pub async fn run_scraper_runtime<T: Scheduler<P>, P: PlatformT>(
    pool: &sqlx::PgPool,
    worker_config: WorkerConfig,
    scheduler: T,

    cron_platforms: Vec<Box<dyn CronPlatform>>,

    tor_client_config: TorClientConfig,
    http_platform_configs: HashMap<P, HttpPlatformConfig>,
    http_platforms: Vec<Box<dyn HttpPlatformBuilder<P>>>,

    browser_platform_configs: HashMap<P, BrowserPlatformConfig>,
    browser_platforms: Vec<Box<dyn BrowserPlatformBuilder<P>>>,
) -> Result<()> {
    let (request_job_tx, request_job_rx) = unbounded::<QueueJobStatus>();
    let (queue_job_tx, queue_job_rx) = unbounded::<QueueJob<P>>();

    let (http_worker_tx, http_worker_rx) = unbounded::<Job<NotRequested, P>>();
    let (browser_worker_tx, browser_worker_rx) = unbounded::<Job<NotRequested, P>>();

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
            pool.clone(),
            request_job_rx,
            request_job_tx.clone(),
            queue_job_rx,
            txs,
            scheduler,
            worker_config.clone(),
        )
    };

    // Build HTTP workers
    let main_tor_client = MainTorClient::new(tor_client_config).await?;
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
                request_job_tx.clone(),
                http_worker_rx.clone(),
                http_worker_tx.clone(),
                queue_job_tx.clone(),
                main_tor_client.clone(),
                platforms,
            )
        })
        .collect::<Vec<_>>();

    // Build Browser workers
    const STARTING_PORT: u16 = 9050;
    let browser_workers: Vec<BrowserWorker<P>> = (0..worker_config.browser_workers)
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
                request_job_tx.clone(),
                browser_worker_rx.clone(),
                browser_worker_tx.clone(),
                queue_job_tx.clone(),
                main_tor_client.clone(),
                platforms,
                true, // TODO: Make this configurable
                socks_port,
            )
            .unwrap()
        })
        .collect();

    const QUEUE_WORKERS: usize = 2;
    let mut handles = Vec::with_capacity(http_workers.len() + QUEUE_WORKERS);
    info!("Starting {} cron platforms", cron_platforms.len());
    for platform in cron_platforms {
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
    handles.extend(job_distributor.start());

    for handle in handles {
        handle.await??;
    }

    Ok(())
}
