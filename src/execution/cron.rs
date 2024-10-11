use crate::{
    execution::scheduler::{PlatformT, QueueJob},
    Result,
};
use async_channel::Sender;
use async_trait::async_trait;
use std::sync::{atomic::AtomicBool, Arc};

pub trait CronPlatformBuilder<P: PlatformT> {
    fn set_queue_job(&mut self, queue_job: Sender<QueueJob<P>>);
    fn set_stop_flag(&mut self, stop_flag: Arc<AtomicBool>);
    fn build(&self) -> Box<dyn CronPlatform<P>>;
}

#[async_trait]
pub trait CronPlatform<P: PlatformT>: Send {
    async fn start(self: Box<Self>) -> Result<()>;
}
