use async_trait::async_trait;
use crate::Result;

#[async_trait]
pub trait CronPlatform: Send {
    async fn start(self: Box<Self>) -> Result<()>;
}