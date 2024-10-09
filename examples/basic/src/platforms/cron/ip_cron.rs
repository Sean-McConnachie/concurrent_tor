use concurrent_tor::{execution::cron::CronPlatform, exports::async_trait, Result};

const IP_HTTP_URL: &str = "https://api.ipify.org?format=json";
const IP_BROWSER_URL: &str = "https://whatismyipaddress.com/";

pub struct IpCron {}

impl IpCron {
    pub fn new() -> Self {
        Self {}
    }
}

#[async_trait]
impl CronPlatform for IpCron {
    async fn start(self: Box<Self>) -> Result<()> {
        loop {}
    }
}
