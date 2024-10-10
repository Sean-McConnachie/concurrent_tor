use crate::execution::{
    browser::BrowserPlatformBuilder, cron::CronPlatform, http::HttpPlatformBuilder,
    scheduler::PlatformT,
};

pub fn cron_box<T: CronPlatform<P> + 'static, P>(builder: T) -> Box<dyn CronPlatform<P>>
where
    P: PlatformT,
{
    Box::new(builder) as Box<dyn CronPlatform<P>>
}

pub fn http_box<T: HttpPlatformBuilder<P> + 'static, P>(
    builder: T,
) -> Box<dyn HttpPlatformBuilder<P>>
where
    P: PlatformT,
{
    Box::new(builder) as Box<dyn HttpPlatformBuilder<P>>
}

pub fn browser_box<T: BrowserPlatformBuilder<P> + 'static, P>(
    builder: T,
) -> Box<dyn BrowserPlatformBuilder<P>>
where
    P: PlatformT,
{
    Box::new(builder) as Box<dyn BrowserPlatformBuilder<P>>
}
