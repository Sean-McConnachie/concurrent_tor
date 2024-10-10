use basic::platforms::{ip_browser, ip_cron, ip_http};
use concurrent_tor::{
    config::ScraperConfig,
    execution::runtime::run_scraper_runtime,
    execution::scheduler::{MainTorClient, QueueScheduler},
    exports::TorClientConfig,
    *,
};
use log::info;

#[tokio::main]
async fn main() -> Result<()> {
    dotenv::dotenv().ok();
    tracing_subscriber::fmt::init();
    info!("Starting up");

    let config = ScraperConfig::init("Config.toml")?;
    run_scraper_runtime(
        config.workers,
        QueueScheduler::new(),
        MainTorClient::new(TorClientConfig::default()).await?,
        vec![cron_box(ip_cron::IpCron::new())],
        config.http_platforms,
        vec![http_box(ip_http::IpHttpBuilder::new())],
        config.browser_platforms,
        vec![browser_box(ip_browser::IpBrowserBuilder::new())],
    )
    .await
}
