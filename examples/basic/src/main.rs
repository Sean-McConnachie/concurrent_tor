use basic::{backend, cron, headed_browser, headless_browser, http, monitor, Platform};
use concurrent_tor::{
    browser_box,
    config::ScraperConfig,
    cron_box,
    execution::{runtime::CTRuntime, scheduler::SimpleScheduler},
    http_box,
};
use std::fs;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenv::dotenv().ok();
    tracing_subscriber::fmt::init();

    // Remove database for this example
    const DATABASE_FP: &str = "concurrent_tor.sqlite3";
    if fs::exists(DATABASE_FP)? {
        fs::remove_file(DATABASE_FP)?;
    }

    const STOP_AFTER_S: u64 = 7;
    const SEND_TIMEOUT_MS: u64 = 500;

    let ct_config = ScraperConfig::<Platform>::init("CTConfig.toml")?;
    let ct_rt = CTRuntime::run(
        ct_config.workers,
        monitor::MyMonitor::new(),
        SimpleScheduler::new(),
        backend::build_main_client().await?,
        vec![cron_box(cron::MyCronBuilder::new(SEND_TIMEOUT_MS))],
        ct_config.http_platforms,
        vec![http_box(http::MyHttpBuilder::new())],
        ct_config.browser_platforms,
        vec![
            browser_box(headed_browser::MyHeadedBrowserBuilder::new()),
            browser_box(headless_browser::MyHeadlessBrowserBuilder::new()),
        ],
    )
    .await?;

    let stop_fn = ct_rt.graceful_stop_fn();
    tokio::task::spawn(async move {
        tokio::time::sleep(tokio::time::Duration::from_secs(STOP_AFTER_S)).await;
        stop_fn().await.expect("Failed to stop runtime");
    });

    ct_rt.join().await?;

    Ok(())
}
