use crate::execution::client::WorkerType;
use crate::{execution::scheduler::PlatformT, Result};
use async_trait::async_trait;
use crossbeam::channel::Receiver;

#[derive(Debug)]
pub struct ProcessedJobInfo<P: PlatformT> {
    pub platform: P,
    pub worker_type: WorkerType,
    pub worker_id: u16,
    pub ts_start: quanta::Instant,
    pub ts_end: quanta::Instant,
    pub job_hash: u128,
}

impl<P> ProcessedJobInfo<P>
where
    P: PlatformT,
{
    pub fn new(
        platform: P,
        worker_type: WorkerType,
        worker_id: u16,
        ts_start: quanta::Instant,
        ts_end: quanta::Instant,
        job_hash: u128,
    ) -> Self {
        ProcessedJobInfo {
            platform,
            worker_type,
            worker_id,
            ts_start,
            ts_end,
            job_hash,
        }
    }
}

#[derive(Debug)]
pub struct BasicWorkerInfo<P: PlatformT> {
    pub platform: P,
    pub worker_type: WorkerType,
    pub worker_id: u16,
    pub ts: quanta::Instant,
}

impl<P> BasicWorkerInfo<P>
where
    P: PlatformT,
{
    pub fn new(platform: P, worker_type: WorkerType, worker_id: u16, ts: quanta::Instant) -> Self {
        BasicWorkerInfo {
            platform,
            worker_type,
            worker_id,
            ts,
        }
    }
}

#[derive(Debug)]
pub struct QueueJobInfo<P: PlatformT> {
    pub platform: P,
    pub ts: quanta::Instant,
    pub job_hash: u128,
}

impl<P> QueueJobInfo<P>
where
    P: PlatformT,
{
    pub fn new(platform: P, ts: quanta::Instant, job_hash: u128) -> Self {
        QueueJobInfo {
            platform,
            ts,
            job_hash,
        }
    }
}

#[derive(Debug)]
pub struct DequeueInfo {
    pub current_circulation: i32,
    pub scheduler_len: usize,
    pub http_chan_len: usize,
    pub browser_chan_len: usize,
    pub ts: quanta::Instant,
}

impl DequeueInfo {
    pub fn new(
        current_circulation: i32,
        scheduler_len: usize,
        http_chan_len: usize,
        browser_chan_len: usize,
        ts: quanta::Instant,
    ) -> Self {
        DequeueInfo {
            current_circulation,
            scheduler_len,
            http_chan_len,
            browser_chan_len,
            ts,
        }
    }
}

#[derive(Debug)]
pub enum Event<P: PlatformT> {
    ProcessedJob(ProcessedJobInfo<P>),
    WorkerRateLimited(BasicWorkerInfo<P>),
    WorkerRenewingClient(BasicWorkerInfo<P>),

    NewJob(QueueJobInfo<P>),
    CompletedJob(QueueJobInfo<P>),
    FailedJob(QueueJobInfo<P>),
    RetryJob(QueueJobInfo<P>),

    BalanceCirculation(DequeueInfo),

    StopMonitor,
}

#[async_trait]
pub trait Monitor<P: PlatformT>: Send {
    async fn start(self, event_rx: Receiver<Event<P>>) -> Result<()>;
}

pub struct EmptyMonitor {}

impl EmptyMonitor {
    pub fn new() -> Self {
        EmptyMonitor {}
    }
}

#[async_trait]
impl<P> Monitor<P> for EmptyMonitor
where
    P: PlatformT,
{
    async fn start(self, event_rx: Receiver<Event<P>>) -> Result<()> {
        loop {
            if let Event::StopMonitor = event_rx.recv()? {
                break;
            }
        }
        Ok(())
    }
}
