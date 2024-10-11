use crate::{execution::scheduler::PlatformT, Result};
use async_trait::async_trait;
use crossbeam::channel::Receiver;

#[derive(Debug)]
pub enum Event<P: PlatformT> {
    StartWorker { platform: P, worker_id: u16 },
    StopWorker { platform: P, worker_id: u16 },
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
