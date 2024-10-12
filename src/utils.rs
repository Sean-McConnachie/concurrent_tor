use crate::execution::{
    browser::BrowserPlatformBuilder, client::Client, cron::CronPlatformBuilder,
    http::HttpPlatformBuilder, scheduler::PlatformT,
};

pub fn cron_box<T: CronPlatformBuilder<P> + 'static, P>(
    builder: T,
) -> Box<dyn CronPlatformBuilder<P>>
where
    P: PlatformT,
{
    Box::new(builder) as Box<dyn CronPlatformBuilder<P>>
}

pub fn http_box<T: HttpPlatformBuilder<P, C> + 'static, P, C>(
    builder: T,
) -> Box<dyn HttpPlatformBuilder<P, C>>
where
    P: PlatformT,
    C: Client,
{
    Box::new(builder) as Box<dyn HttpPlatformBuilder<P, C>>
}

pub fn browser_box<T: BrowserPlatformBuilder<P> + 'static, P>(
    builder: T,
) -> Box<dyn BrowserPlatformBuilder<P>>
where
    P: PlatformT,
{
    Box::new(builder) as Box<dyn BrowserPlatformBuilder<P>>
}

pub fn quanta_zero() -> quanta::Instant {
    unsafe { std::mem::zeroed() }
}

#[macro_export]
macro_rules! hashmap {
    () => {
        std::collections::HashMap::new()
    };

    ($($key:expr => $val:expr),* $(,)?) => {
        {
            let mut map = std::collections::HashMap::new();
            $(
                map.insert($key, $val);
            )*
            map
        }
    };
}
