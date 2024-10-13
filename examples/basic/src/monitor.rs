use crate::Platform;
use concurrent_tor::{
    execution::monitor::{Event, Monitor},
    exports::{async_trait, AsyncChannelReceiver},
};
use log::info;

pub struct MyMonitor {}

impl MyMonitor {
    pub fn new() -> Self {
        MyMonitor {}
    }
}

#[async_trait]
impl Monitor<Platform> for MyMonitor {
    async fn start(
        self,
        event_rx: AsyncChannelReceiver<Event<Platform>>,
    ) -> concurrent_tor::Result<()> {
        loop {
            let ct_event = event_rx
                .recv()
                .await
                .expect("Failed to receive event from the scheduler");
            match ct_event {
                Event::WorkerRateLimited(_) => {}
                Event::BalanceCirculation(_) => {}
                Event::StopMonitor => {
                    break;
                }
                _ => {
                    info!("Monitor received event: {:?}", ct_event);
                }
            }
        }
        Ok(())
    }
}
