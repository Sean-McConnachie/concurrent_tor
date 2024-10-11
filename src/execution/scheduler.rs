use crate::{
    config::WorkerConfig,
    database::{JobCache, JobStatusDb, DB},
    Result,
};
use anyhow::anyhow;
use crossbeam::channel::{Receiver, Sender};
use log::{debug, info};
use serde::de::DeserializeOwned;
use std::collections::HashMap;
use std::fmt::Debug;
use std::hash::Hash;
use std::sync::{Arc, Mutex};
use tokio::task::JoinHandle;

pub trait PlatformT:
    DeserializeOwned + Debug + Clone + Copy + Hash + Eq + PartialEq + Send + Sync + Unpin + 'static
{
    fn request_from_json(&self, json: &str) -> Result<Box<dyn WorkerRequest>>;
    fn to_repr(&self) -> usize;
    fn from_repr(repr: usize) -> Self;
}

pub type FromJsonFn = fn(&str) -> Result<Box<dyn WorkerRequest>>;

pub trait WorkerRequest: Send + Debug {
    fn as_any(&self) -> &dyn std::any::Any;
    fn hash(&self) -> Result<String>;
    fn as_json(&self) -> String;
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
    pub fn new(platform: P, request: Box<dyn WorkerRequest>, num_attempts: u32) -> Self {
        Job {
            platform,
            request,
            num_attempts,
            _status: std::marker::PhantomData,
        }
    }

    pub fn init(platform: P, request: Box<dyn WorkerRequest>) -> Self {
        Job {
            platform,
            request,
            num_attempts: 0,
            _status: std::marker::PhantomData,
        }
    }

    pub fn init_from_box<T: WorkerRequest + 'static>(platform: P, request: T) -> Self {
        Job {
            platform,
            request: Box::new(request),
            num_attempts: 0,
            _status: std::marker::PhantomData,
        }
    }
}

impl<P> From<Job<NotRequested, P>> for Job<Requested, P>
where
    P: PlatformT,
{
    fn from(job: Job<NotRequested, P>) -> Self {
        Job {
            platform: job.platform,
            request: job.request,
            num_attempts: job.num_attempts + 1,
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
    Retry(Job<NotRequested, P>),
    SendStopProgram,
    StopProgram,
}

pub trait Scheduler<P: PlatformT>: Send + 'static {
    fn enqueue(&mut self, job: Job<NotRequested, P>);
    fn enqueue_many(&mut self, jobs: Vec<Job<NotRequested, P>>) {
        for job in jobs {
            self.enqueue(job);
        }
    }
    fn dequeue(&mut self) -> Option<Job<NotRequested, P>>;
}

#[derive(Debug)]
pub enum QueueJobStatus {
    WorkerCompleted { worker_id: u32 },
    NewJobArrived,
    SendStopProgram,
    StopProgram,
}

pub struct JobDistributor<S: Scheduler<P>, P: PlatformT> {
    db: DB,
    /// Receive signal from a worker to enqueue a job.
    request_job: Receiver<QueueJobStatus>,
    /// Send a signal to dequeue a job.
    notify_new_job: Sender<QueueJobStatus>,
    /// Used only to send stop signal to http workers.
    http_tx: Sender<WorkerAction<P>>,
    /// Used only to send stop signal to browser workers.
    browser_tx: Sender<WorkerAction<P>>,
    /// Receive job from a platform only.
    queue_job: Receiver<QueueJob<P>>,
    /// Send job to a worker only. HashMap because workers are Http or Browser based, not generic.
    dequeue_job: HashMap<P, Sender<WorkerAction<P>>>,
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
        db: DB,
        request_job: Receiver<QueueJobStatus>,
        notify_new_job: Sender<QueueJobStatus>,
        queue_job: Receiver<QueueJob<P>>,
        dequeue_job: HashMap<P, Sender<WorkerAction<P>>>,
        http_tx: Sender<WorkerAction<P>>,
        browser_tx: Sender<WorkerAction<P>>,
        scheduler: S,
        worker_config: WorkerConfig,
    ) -> Self {
        JobDistributor {
            db,
            request_job,
            notify_new_job,
            queue_job,
            dequeue_job,
            http_tx,
            browser_tx,
            scheduler: Arc::new(Mutex::new(scheduler)),
            worker_config,
        }
    }

    pub(super) fn start(self) -> [JoinHandle<Result<()>>; 2] {
        let scheduler = self.scheduler.clone();
        let enqueue_thread = tokio::task::spawn(async move {
            Self::loop_enqueue(self.db, self.queue_job, self.notify_new_job, scheduler).await
        });
        let dequeue_thread = tokio::task::spawn_blocking(move || {
            Self::loop_dequeue(
                self.request_job,
                self.dequeue_job,
                self.http_tx,
                self.browser_tx,
                self.scheduler,
                self.worker_config,
            )
        });
        [enqueue_thread, dequeue_thread]
    }

    async fn loop_enqueue(
        mut db: DB,
        queue_job: Receiver<QueueJob<P>>,
        notify_new_job: Sender<QueueJobStatus>,
        scheduler: Arc<Mutex<S>>,
    ) -> Result<()> {
        debug!("Starting enqueue loop");

        // initialize the scheduler with what is in the database
        let jobs = JobCache::<P>::fetch_all_active(&mut db).await?;
        for job in jobs {
            let request = job.platform.request_from_json(&job.request)?;
            let job = Job::new(job.platform, request, job.num_attempts as u32);
            scheduler.lock().unwrap().enqueue(job);
            notify_new_job
                .send(QueueJobStatus::NewJobArrived)
                .map_err(|e| {
                    anyhow!(
                        "Failed to send new job arrived signal in loop enqueue: {:?}",
                        e
                    )
                })?;
        }
        let mut shutting_down = false;

        loop {
            let job = queue_job.recv()?;
            match job {
                QueueJob::New(job) => {
                    JobCache::insert_new(
                        &mut db,
                        job.num_attempts as i32,
                        &job.request.hash()?,
                        job.platform.clone(),
                        &job.request.as_json(),
                    )
                    .await?;
                    if !shutting_down {
                        scheduler.lock().unwrap().enqueue(job);
                        notify_new_job
                            .send(QueueJobStatus::NewJobArrived)
                            .map_err(|e| {
                                anyhow!(
                                    "Failed to send new job arrived signal in loop enqueue: {:?}",
                                    e
                                )
                            })?;
                    }
                }
                QueueJob::Completed(job) => {
                    JobCache::<P>::update_status_by_hash(
                        &mut db,
                        &job.request.hash()?,
                        JobStatusDb::Completed,
                        job.num_attempts as i32,
                    )
                    .await?;
                }
                QueueJob::Failed(job) => {
                    JobCache::<P>::update_status_by_hash(
                        &mut db,
                        &job.request.hash()?,
                        JobStatusDb::Failed,
                        job.num_attempts as i32,
                    )
                    .await?;
                }
                QueueJob::Retry(job) => {
                    JobCache::<P>::update_status_by_hash(
                        &mut db,
                        &job.request.hash()?,
                        JobStatusDb::Active,
                        job.num_attempts as i32,
                    )
                    .await?;
                    scheduler.lock().unwrap().enqueue(job);
                    notify_new_job
                        .send(QueueJobStatus::NewJobArrived)
                        .map_err(|e| {
                            anyhow!(
                                "Failed to send new job arrived signal in loop enqueue: {:?}",
                                e
                            )
                        })?;
                }
                QueueJob::SendStopProgram => {
                    info!("Sending stop signal to dequeue loop");
                    shutting_down = true;
                    notify_new_job
                        .send(QueueJobStatus::SendStopProgram)
                        .map_err(|e| {
                            anyhow!(
                                "Failed to send stop program signal in loop enqueue: {:?}",
                                e
                            )
                        })?;
                }
                QueueJob::StopProgram => {
                    notify_new_job
                        .send(QueueJobStatus::StopProgram)
                        .map_err(|e| {
                            anyhow!(
                                "Failed to send stop program signal in loop enqueue: {:?}",
                                e
                            )
                        })?;
                    break;
                }
            }
        }
        Ok(())
    }

    fn loop_dequeue(
        request_job: Receiver<QueueJobStatus>,
        dequeue_job: HashMap<P, Sender<WorkerAction<P>>>,
        mut http_tx: Sender<WorkerAction<P>>,
        mut browser_tx: Sender<WorkerAction<P>>,
        scheduler: Arc<Mutex<S>>,
        worker_config: WorkerConfig,
    ) -> Result<()> {
        debug!("Starting dequeue loop");
        const TARGET_CIRCULATION: i32 = 10;
        // Negative circulation means that we are waiting for jobs to arrive.
        // Zero circulation means that we are in equilibrium and all workers are busy.
        // Positive circulation means that we are waiting for workers to complete jobs in the channel.
        let mut current_circulation = -(worker_config.target_circulation as i32);
        let balance = |circulation: &mut i32| -> Result<()> {
            if *circulation < TARGET_CIRCULATION {
                if let Some(job) = scheduler.lock().unwrap().dequeue() {
                    let sender = dequeue_job.get(&job.platform).unwrap();
                    sender.send(WorkerAction::Job(job)).map_err(|e| {
                        anyhow!("Failed to send job to worker in dequeue loop: {:?}", e)
                    })?;
                    *circulation += 1;
                }
            }
            Ok(())
        };
        loop {
            let status = request_job.recv()?;
            match status {
                QueueJobStatus::NewJobArrived => {
                    balance(&mut current_circulation)?;
                }
                QueueJobStatus::WorkerCompleted {
                    worker_id: _worker_id,
                } => {
                    current_circulation -= 1;
                    balance(&mut current_circulation)?;
                }
                QueueJobStatus::SendStopProgram => {
                    info!("Sending stop signal to http workers");
                    for _http_platform in 0..worker_config.http_workers {
                        http_tx.send(WorkerAction::StopProgram).map_err(|e| {
                            anyhow!("Failed to send stop program to http workers: {:?}", e)
                        })?;
                    }
                    info!("Sending stop signal to browser workers");
                    for _browser_platform in 0..worker_config.browser_workers {
                        browser_tx.send(WorkerAction::StopProgram).map_err(|e| {
                            anyhow!("Failed to send stop program to browser workers: {:?}", e)
                        })?;
                    }
                }
                QueueJobStatus::StopProgram => {
                    break;
                }
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
}
