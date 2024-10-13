use crate::{
    config::WorkerConfig,
    database::{JobCache, JobStatusDb, DB},
    execution::monitor::{DequeueInfo, Event, QueueJobInfo},
    Result,
};
use anyhow::anyhow;
use async_channel::{Receiver, Sender};
use dyn_clone::DynClone;
use futures_util::TryFutureExt;
use log::{debug, error, info, warn};
use serde::de::DeserializeOwned;
use std::{fmt::Debug, hash::Hash, sync::Arc};
use strum::IntoEnumIterator;
use tokio::{sync::Mutex, task::JoinHandle};

pub trait PlatformT:
    DeserializeOwned
    + Debug
    + Clone
    + Copy
    + Hash
    + Eq
    + PartialEq
    + Send
    + Sync
    + Unpin
    + 'static
    + IntoEnumIterator
{
    fn request_from_json(&self, json: &str) -> Result<Box<dyn WorkerRequest>>;
    fn to_repr(&self) -> usize;
    fn from_repr(repr: usize) -> Self;
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum SpecificWorkerType {
    Http,
    HeadedBrowser,
    HeadlessBrowser,
}

#[derive(Debug, Clone)]
pub(crate) struct ChannelGroup<C, P>
where
    P: PlatformT,
{
    pub http: C,
    pub headless_browser: C,
    pub headed_browser: C,
    pub(crate) _platform: std::marker::PhantomData<P>,
}

pub type FromJsonFn = fn(&str) -> Result<Box<dyn WorkerRequest>>;

pub type JobHash = u128;

pub trait WorkerRequest: Send + Debug + DynClone {
    fn as_any(&self) -> &dyn std::any::Any;
    fn hash(&self) -> Result<JobHash>;
    fn as_json(&self) -> String;
}
dyn_clone::clone_trait_object!(WorkerRequest);

pub trait PlatformHistory {
    fn can_request(&self, now: quanta::Instant) -> PlatformCanRequest;
    fn request_complete(&mut self, now: quanta::Instant);
    fn reset(&mut self);
}

#[derive(Debug, Eq, PartialEq)]
pub enum PlatformCanRequest {
    Ok,
    MustWait,
    MaxRequests,
}

/// NotRequested means that the job has not been processed by a worker and can be processed.
#[derive(Debug)]
pub struct NotRequested;
/// Requested means that the job has been processed by a worker and cannot be processed until
/// the Platform reinitiates the request. Reinitiating the request is dependent on the platform's
/// logic.
#[derive(Debug)]
pub struct Requested;
#[derive(Debug)]
pub struct Job<RequestStatus, P: PlatformT> {
    pub platform: P,
    pub request: Box<dyn WorkerRequest>,
    pub num_attempts: u32,
    pub max_attempts: u32,

    _status: std::marker::PhantomData<RequestStatus>,
}

unsafe impl<P> Send for Job<NotRequested, P> where P: PlatformT {}
unsafe impl<P> Send for Job<Requested, P> where P: PlatformT {}
unsafe impl<P> Sync for Job<NotRequested, P> where P: PlatformT {}
unsafe impl<P> Sync for Job<Requested, P> where P: PlatformT {}

impl<P> Job<NotRequested, P>
where
    P: PlatformT,
{
    pub fn new(
        platform: P,
        request: Box<dyn WorkerRequest>,
        num_attempts: u32,
        max_attempts: u32,
    ) -> Self {
        Job {
            platform,
            request,
            num_attempts,
            max_attempts,
            _status: std::marker::PhantomData,
        }
    }

    pub fn init(platform: P, request: Box<dyn WorkerRequest>, max_attempts: u32) -> Self {
        Job {
            platform,
            request,
            num_attempts: 0,
            max_attempts,
            _status: std::marker::PhantomData,
        }
    }

    pub fn init_from_box<T: WorkerRequest + 'static>(
        platform: P,
        request: T,
        max_attempts: u32,
    ) -> Self {
        Job {
            platform,
            request: Box::new(request),
            num_attempts: 0,
            max_attempts,
            _status: std::marker::PhantomData,
        }
    }
}

impl<P> Job<Requested, P>
where
    P: PlatformT,
{
    pub(crate) fn to_not_requested(self) -> Job<NotRequested, P> {
        Job {
            platform: self.platform,
            request: self.request,
            num_attempts: self.num_attempts,
            max_attempts: self.max_attempts,
            _status: std::marker::PhantomData,
        }
    }
}

impl<P> From<&Job<NotRequested, P>> for Job<Requested, P>
where
    P: PlatformT,
{
    fn from(job: &Job<NotRequested, P>) -> Self {
        Job {
            platform: job.platform,
            request: job.request.clone(),
            num_attempts: job.num_attempts + 1,
            max_attempts: job.max_attempts,
            _status: std::marker::PhantomData,
        }
    }
}

#[derive(Debug)]
pub enum WorkerAction<P: PlatformT> {
    Job(Job<NotRequested, P>),
    StopProgram,
}

#[derive(Debug)]
pub enum QueueJob<P: PlatformT> {
    New(Job<NotRequested, P>),
    Completed(Job<Requested, P>),
    /// Cancel the job and do not retry.
    Failed(Job<Requested, P>),
    Retry(Job<Requested, P>),
    SendStopProgram,
    StopProgram,
}

impl<P> QueueJob<P>
where
    P: PlatformT,
{
    pub fn inner_job_info(&self) -> Result<Option<(u128, u32, u32)>> {
        Ok(Some(match self {
            QueueJob::New(job) => (job.request.hash()?, job.num_attempts, job.max_attempts),
            QueueJob::Completed(job) => (job.request.hash()?, job.num_attempts, job.max_attempts),
            QueueJob::Failed(job) => (job.request.hash()?, job.num_attempts, job.max_attempts),
            QueueJob::Retry(job) => (job.request.hash()?, job.num_attempts, job.max_attempts),
            _ => return Ok(None),
        }))
    }
}

pub trait Scheduler<P: PlatformT>: Send + 'static {
    fn enqueue(&mut self, job: Job<NotRequested, P>);
    fn enqueue_many(&mut self, jobs: Vec<Job<NotRequested, P>>) {
        for job in jobs {
            self.enqueue(job);
        }
    }
    fn dequeue(&mut self) -> Option<Job<NotRequested, P>>;
    fn size(&self) -> usize;
}

#[derive(Debug)]
pub(crate) enum QueueJobStatus {
    WorkerCompleted { worker_id: u16 },
    NewJobArrived,
    SendStopProgram,
    StopProgram,
}

pub(crate) struct JobDistributor<S: Scheduler<P>, P: PlatformT> {
    /// Monitor
    monitor: Sender<Event<P>>,
    db: DB,
    /// Receive signal from a worker to enqueue a job.
    request_job: Receiver<QueueJobStatus>,
    /// Send a signal to dequeue a job.
    notify_new_job: Sender<QueueJobStatus>,
    /// Receive job from a platform only.
    queue_job: Receiver<QueueJob<P>>,
    /// Send job to a worker only. HashMap because workers are Http or Browser based, not generic.
    dequeue_job: Vec<Sender<WorkerAction<P>>>,
    /// Used only to send stop signal to http workers.
    worker_rxs: ChannelGroup<Sender<WorkerAction<P>>, P>,
    scheduler: Arc<Mutex<S>>,
    worker_config: WorkerConfig,
}

impl<S, P> JobDistributor<S, P>
where
    S: Scheduler<P>,
    P: PlatformT,
{
    // TODO: Fix target circulation
    // TODO: Check if there are duplicate jobs in the queue - remove if so
    pub fn new(
        monitor: Sender<Event<P>>,
        db: DB,
        request_job: Receiver<QueueJobStatus>,
        notify_new_job: Sender<QueueJobStatus>,
        queue_job: Receiver<QueueJob<P>>,
        dequeue_job: Vec<Sender<WorkerAction<P>>>,
        worker_rxs: ChannelGroup<Sender<WorkerAction<P>>, P>,
        scheduler: S,
        worker_config: WorkerConfig,
    ) -> Self {
        JobDistributor {
            monitor,
            db,
            request_job,
            notify_new_job,
            queue_job,
            dequeue_job,
            worker_rxs,
            scheduler: Arc::new(Mutex::new(scheduler)),
            worker_config,
        }
    }

    pub(super) fn start(self) -> [JoinHandle<Result<()>>; 2] {
        let scheduler = self.scheduler.clone();
        let monitor_clone = self.monitor.clone();
        let enqueue_thread = tokio::task::spawn(async move {
            Self::loop_enqueue(
                monitor_clone,
                self.db,
                self.queue_job,
                self.notify_new_job,
                scheduler,
            )
            .await
        });
        let dequeue_thread = tokio::task::spawn(async move {
            Self::loop_dequeue(
                self.monitor,
                self.request_job,
                self.dequeue_job,
                self.worker_rxs,
                self.scheduler,
                self.worker_config,
            )
            .await
        });
        [enqueue_thread, dequeue_thread]
    }

    async fn loop_enqueue(
        monitor: Sender<Event<P>>,
        mut db: DB,
        queue_job: Receiver<QueueJob<P>>,
        notify_new_job: Sender<QueueJobStatus>,
        scheduler: Arc<Mutex<S>>,
    ) -> Result<()> {
        info!("Starting enqueue loop");

        // initialize the scheduler with what is in the database
        let jobs = JobCache::<P>::fetch_all_active(&mut db).await?;
        info!("Fetched {} active jobs from the database", jobs.len());
        for job in jobs {
            let request = job.platform.request_from_json(&job.request)?;
            let job = Job::new(
                job.platform,
                request,
                job.num_attempts as u32,
                job.max_attempts as u32,
            );
            monitor
                .send(Event::NewJob(QueueJobInfo::new(
                    job.platform,
                    quanta::Instant::now(),
                    job.request.hash()?,
                    JobStatusDb::Active,
                    job.num_attempts,
                    job.max_attempts,
                )))
                .await?;
            scheduler.lock().await.enqueue(job);
            notify_new_job
                .send(QueueJobStatus::NewJobArrived)
                .map_err(|e| {
                    anyhow!(
                        "Failed to send new job arrived signal in loop enqueue: {:?}",
                        e
                    )
                })
                .await?;

            // TODO: Handle duplicate jobs in the queue
        }
        let mut shutting_down = false;

        loop {
            let job = queue_job.recv().await?;
            match job {
                QueueJob::New(job) => {
                    let job_hash = job.request.hash()?;
                    match JobCache::insert_new(
                        &mut db,
                        job.num_attempts as i32,
                        job.max_attempts as i32,
                        job_hash,
                        job.platform.clone(),
                        &job.request.as_json(),
                    )
                    .await
                    {
                        Ok(_) => {}
                        Err(e) => {
                            error!("Failed to insert new job into database: {:?}", e);
                            continue;
                        }
                    }
                    if !shutting_down {
                        // TODO: Handle duplicate jobs in the queue
                        monitor
                            .send(Event::NewJob(QueueJobInfo::new(
                                job.platform,
                                quanta::Instant::now(),
                                job_hash,
                                JobStatusDb::Active, // behaviour of [insert_new]
                                job.num_attempts,
                                job.max_attempts,
                            )))
                            .await?;
                        scheduler.lock().await.enqueue(job);
                        notify_new_job
                            .send(QueueJobStatus::NewJobArrived)
                            .map_err(|e| {
                                anyhow!(
                                    "Failed to send new job arrived signal in loop enqueue: {:?}",
                                    e
                                )
                            })
                            .await?;
                    }
                }
                QueueJob::Completed(job) => {
                    let job_hash = job.request.hash()?;
                    JobCache::<P>::update_status_by_hash(
                        &mut db,
                        job_hash,
                        JobStatusDb::Completed,
                        job.num_attempts as i32,
                    )
                    .await?;
                    monitor
                        .send(Event::CompletedJob(QueueJobInfo::new(
                            job.platform,
                            quanta::Instant::now(),
                            job_hash,
                            JobStatusDb::Completed, // behaviour of [update_status_by_hash] in this context
                            job.num_attempts,
                            job.num_attempts,
                        )))
                        .await?;
                }
                QueueJob::Failed(job) => {
                    let job_hash = job.request.hash()?;
                    JobCache::<P>::update_status_by_hash(
                        &mut db,
                        job_hash,
                        JobStatusDb::Failed,
                        job.num_attempts as i32,
                    )
                    .await?;
                    monitor
                        .send(Event::FailedJob(QueueJobInfo::new(
                            job.platform,
                            quanta::Instant::now(),
                            job_hash,
                            JobStatusDb::Failed, // behaviour of [update_status_by_hash] in this context
                            job.num_attempts,
                            job.max_attempts,
                        )))
                        .await?;
                }
                QueueJob::Retry(job) => {
                    let job_hash = job.request.hash()?;
                    JobCache::<P>::update_status_by_hash(
                        &mut db,
                        job.request.hash()?,
                        JobStatusDb::Active,
                        job.num_attempts as i32,
                    )
                    .await?;
                    if !shutting_down {
                        if job.num_attempts >= job.max_attempts {
                            warn!(
                                "Job with hash {} has reached max attempts. Not retrying and setting \
                                status to failed.",
                                job_hash
                            );
                            monitor
                                .send(Event::MaxAttemptsReached(QueueJobInfo::new(
                                    job.platform,
                                    quanta::Instant::now(),
                                    job_hash,
                                    JobStatusDb::Active, // behaviour of [update_status_by_hash] in this context
                                    job.num_attempts,
                                    job.max_attempts,
                                )))
                                .await?;
                            JobCache::<P>::update_status_by_hash(
                                &mut db,
                                job_hash,
                                JobStatusDb::Failed,
                                job.num_attempts as i32,
                            )
                            .await?;
                        } else {
                            monitor
                                .send(Event::RetryJob(QueueJobInfo::new(
                                    job.platform,
                                    quanta::Instant::now(),
                                    job_hash,
                                    JobStatusDb::Active, // behaviour of [update_status_by_hash] in this context
                                    job.num_attempts,
                                    job.max_attempts,
                                )))
                                .await?;
                            scheduler.lock().await.enqueue(job.to_not_requested());
                            notify_new_job
                                .send(QueueJobStatus::NewJobArrived)
                                .map_err(|e| {
                                    anyhow!(
                                        "Failed to send new job arrived signal in loop enqueue: {:?}",
                                        e
                                    )
                                })
                                .await?;
                        }
                    }
                }
                QueueJob::SendStopProgram => {
                    info!("Sending stop signal to dequeue loop");
                    shutting_down = true;
                    notify_new_job
                        .send(QueueJobStatus::SendStopProgram)
                        .map_err(|e| {
                            anyhow!(
                                "Failed to send preliminary stop program signal in loop enqueue: {:?}",
                                e
                            )
                        })
                        .await?;
                }
                QueueJob::StopProgram => {
                    info!("Stopping enqueue loop");
                    notify_new_job
                        .send(QueueJobStatus::StopProgram)
                        .map_err(|e| {
                            anyhow!(
                                "Failed to send stop program signal in loop enqueue: {:?}",
                                e
                            )
                        })
                        .await?;
                    break;
                }
            }
        }
        info!("Stopping enqueue loop");
        Ok(())
    }

    async fn loop_dequeue(
        monitor: Sender<Event<P>>,
        request_job: Receiver<QueueJobStatus>,
        dequeue_job: Vec<Sender<WorkerAction<P>>>,
        worker_rxs: ChannelGroup<Sender<WorkerAction<P>>, P>,
        scheduler: Arc<Mutex<S>>,
        worker_config: WorkerConfig,
    ) -> Result<()> {
        debug!("Starting dequeue loop");
        // Negative circulation means that we are waiting for jobs to arrive.
        // Zero circulation means that we are in equilibrium and all workers are busy.
        // Positive circulation means that we are waiting for workers to complete jobs in the channel.
        let mut current_circulation = -(worker_config.target_circulation as i32);
        let mut shutting_down = false;
        loop {
            let status = request_job.recv().await?;
            match status {
                QueueJobStatus::NewJobArrived => {
                    if !shutting_down {
                        Self::balance_circulation(
                            worker_config.target_circulation as i32,
                            &mut current_circulation,
                            &scheduler,
                            &worker_rxs,
                            &monitor,
                            &dequeue_job,
                        )
                        .await?;
                    }
                }
                QueueJobStatus::WorkerCompleted {
                    worker_id: _worker_id,
                } => {
                    current_circulation -= 1;
                    if !shutting_down {
                        Self::balance_circulation(
                            worker_config.target_circulation as i32,
                            &mut current_circulation,
                            &scheduler,
                            &worker_rxs,
                            &monitor,
                            &dequeue_job,
                        )
                        .await?;
                    }
                }
                QueueJobStatus::SendStopProgram => {
                    info!("Sending stop signal to http workers");
                    for _http_platform in 0..worker_config.http_workers {
                        worker_rxs
                            .http
                            .send(WorkerAction::StopProgram)
                            .map_err(|e| {
                                anyhow!("Failed to send stop program to http workers: {:?}", e)
                            })
                            .await?;
                    }
                    info!("Sending stop signal to headless browser workers");
                    for _browser_platform in 0..worker_config.headless_browser_workers {
                        worker_rxs
                            .headless_browser
                            .send(WorkerAction::StopProgram)
                            .map_err(|e| {
                                anyhow!("Failed to send stop program to browser workers: {:?}", e)
                            })
                            .await?;
                    }
                    info!("Sending stop signal to headed browser workers");
                    for _browser_platform in 0..worker_config.headed_browser_workers {
                        worker_rxs
                            .headed_browser
                            .send(WorkerAction::StopProgram)
                            .map_err(|e| {
                                anyhow!("Failed to send stop program to browser workers: {:?}", e)
                            })
                            .await?;
                    }
                    info!("Sent all stop signals to workers");
                    shutting_down = true;
                }
                QueueJobStatus::StopProgram => {
                    break;
                }
            }
        }
        info!("Stopping dequeue loop");
        Ok(())
    }

    async fn balance_circulation(
        target_circulation: i32,
        circulation: &mut i32,
        scheduler: &Arc<Mutex<S>>,
        worker_rxs: &ChannelGroup<Sender<WorkerAction<P>>, P>,
        monitor: &Sender<Event<P>>,
        dequeue_job: &Vec<Sender<WorkerAction<P>>>,
    ) -> Result<()> {
        monitor
            .send(Event::BalanceCirculation(DequeueInfo::new(
                *circulation,
                scheduler.lock().await.size(),
                worker_rxs.http.len(),
                worker_rxs.headless_browser.len(),
                worker_rxs.headed_browser.len(),
                quanta::Instant::now(),
            )))
            .await?;
        if *circulation < target_circulation {
            if let Some(job) = scheduler.lock().await.dequeue() {
                let sender = &dequeue_job[job.platform.to_repr()];
                sender
                    .send(WorkerAction::Job(job))
                    .map_err(|e| anyhow!("Failed to send job to worker in dequeue loop: {:?}", e))
                    .await?;
                *circulation += 1;
            }
        }
        Ok(())
    }
}

pub struct SimpleScheduler<P: PlatformT> {
    queue: Vec<Job<NotRequested, P>>,
}

impl<P> SimpleScheduler<P>
where
    P: PlatformT,
{
    pub fn new() -> Self {
        SimpleScheduler { queue: Vec::new() }
    }
}

impl<P> Scheduler<P> for SimpleScheduler<P>
where
    P: PlatformT,
{
    fn enqueue(&mut self, job: Job<NotRequested, P>) {
        self.queue.push(job);
    }
    fn enqueue_many(&mut self, jobs: Vec<Job<NotRequested, P>>) {
        self.queue.extend(jobs);
    }
    fn dequeue(&mut self) -> Option<Job<NotRequested, P>> {
        self.queue.pop()
    }
    fn size(&self) -> usize {
        self.queue.len()
    }
}
