use basic::platforms::{browser, cron, http};
use concurrent_tor::{
    config::ScraperConfig, execution::runtime::run_scraper_runtime,
    execution::scheduler::QueueScheduler, exports::TorClientConfig, *,
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
        vec![cron_box(cron::IpCron::new())],
        TorClientConfig::default(),
        config.http_platforms,
        vec![http_box(http::IpHttpBuilder::new())],
        config.browser_platforms,
        vec![browser_box(browser::IpBrowserBuilder::new())],
    )
    .await
}
