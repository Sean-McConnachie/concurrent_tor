use crate::{
    execution::scheduler::{PlatformT, QueueJob},
    Result,
};
use async_trait::async_trait;
use crossbeam::channel::Sender;

#[async_trait]
pub trait CronPlatform<P: PlatformT>: Send {
    fn set_queue_job(&mut self, queue_job: Sender<QueueJob<P>>);
    async fn start(self: Box<Self>) -> Result<()>;
}
