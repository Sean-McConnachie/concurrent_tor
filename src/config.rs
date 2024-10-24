use crate::{execution::scheduler::PlatformT, Error, Result};
use chrono::{Datelike, TimeZone, Timelike};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, hash::Hash, str::FromStr};

#[derive(Deserialize, Clone, Debug)]
pub struct WorkerConfig {
    pub target_circulation: u32,
    pub http_workers: u16,
    pub headless_browser_workers: u16,
    pub headed_browser_workers: u16,
    pub database_fp: String,
    pub driver_fp: String,
    pub socks_start_port: u16,
    pub driver_start_port: u16,
}

/// Configuration for every platform
#[derive(Deserialize, Clone, Debug)]
pub struct HttpPlatformConfig {
    /// Maximum number of requests per IP
    pub max_requests: u32,
    /// Rate limiting in seconds
    pub timeout_ms: u32,
}

/// Configuration for the headless browser platform
#[derive(Deserialize, Clone, Debug)]
pub struct HeadlessBrowserPlatformConfig {
    /// Maximum number of requests per IP
    pub max_requests: u32,
    /// Rate limiting in seconds
    pub timeout_ms: u32,
}

/// Configuration for the browser platform
#[derive(Deserialize, Clone, Debug)]
pub struct BrowserPlatformConfig {
    /// Maximum number of requests per IP
    pub max_requests: u32,
    /// Rate limiting in seconds
    pub timeout_ms: u32,
    /// Headless browser configuration
    pub headless: bool,
}

#[derive(Deserialize, Debug)]
pub struct CTConfig<P>
where
    P: 'static + Hash + Eq,
{
    pub workers: WorkerConfig,
    pub http_platforms: HashMap<P, HttpPlatformConfig>,
    pub browser_platforms: HashMap<P, BrowserPlatformConfig>,
}

impl<P> CTConfig<P>
where
    P: PlatformT + Eq + Hash,
{
    pub fn init<T: AsRef<std::path::Path>>(path: T) -> Result<Self> {
        let config = std::fs::read_to_string(&path).map_err(|e| {
            format!(
                "Could not read the configuration file at path: {:?}, error: {}",
                path.as_ref().display(),
                e
            )
        })?;
        let config: CTConfig<P> = toml::from_str(&config)?;
        Ok(config)
    }
}

trait CronTime {
    fn validate(p: &CronParam<Self>) -> Result<()>;
}

#[derive(Debug, Copy, Clone, Serialize, Deserialize)]
pub struct CronMinute;
#[derive(Debug, Copy, Clone, Serialize, Deserialize)]
pub struct CronHour;
#[derive(Debug, Copy, Clone, Serialize, Deserialize)]
pub struct CronDay;
#[derive(Debug, Copy, Clone, Serialize, Deserialize)]
pub struct CronMonth;
#[derive(Debug, Copy, Clone, Serialize, Deserialize)]
pub struct CronWeekday;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct CronConfig {
    pub minute: CronParam<CronMinute>,
    pub hour: CronParam<CronHour>,
    pub day: CronParam<CronDay>,
    pub month: CronParam<CronMonth>,
    pub weekday: CronParam<CronWeekday>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum CronParam<Time: ?Sized> {
    Every,
    At(u32),
    Phantom(std::marker::PhantomData<Time>),
}

impl CronConfig {
    fn month_has_day(year: i32, month: u32, day: u32) -> bool {
        chrono::NaiveDate::from_ymd_opt(year, month, day).is_some()
    }

    fn get_days_from_month(year: i32, month: u32) -> u32 {
        chrono::NaiveDate::from_ymd_opt(
            match month {
                12 => year + 1,
                _ => year,
            },
            match month {
                12 => 1,
                _ => month + 1,
            },
            1,
        )
        .unwrap()
        .signed_duration_since(chrono::NaiveDate::from_ymd_opt(year, month, 1).unwrap())
        .num_days() as u32
    }

    fn add_months(date: chrono::NaiveDate, num_months: u32) -> chrono::NaiveDate {
        let mut month = date.month() + num_months;
        let year = date.year() + (month / 12) as i32;
        month = month % 12;
        let mut day = date.day();
        let max_days = Self::get_days_from_month(year, month);
        day = if day > max_days { max_days } else { day };
        chrono::NaiveDate::from_ymd_opt(year, month, day).unwrap()
    }

    /// Returns the next time the cron should run. Accepts NaiveDateTime for the current time.
    /// This is to allow the user to pass in whatever timezones they want.
    pub fn time_until_next<Tz: TimeZone>(&self, now: &chrono::DateTime<Tz>) -> std::time::Duration {
        let mut next = now.naive_utc();
        next = next + chrono::Duration::minutes(1);
        next = next.with_second(0).unwrap();
        next = next.with_nanosecond(0).unwrap();

        let mut done = false;

        while !done {
            if let CronParam::At(cron_minute) = self.minute {
                if next.minute() != cron_minute {
                    if next.minute() > cron_minute {
                        next = next + chrono::Duration::hours(1);
                    }
                    next = next.with_minute(cron_minute).unwrap();
                }
            }

            if let CronParam::At(cron_hour) = self.hour {
                if next.hour() != cron_hour {
                    if next.hour() > cron_hour {
                        next = next + chrono::Duration::days(1);
                        next = next.with_hour(cron_hour).unwrap();
                        next = next.with_minute(0).unwrap();
                        continue;
                    }
                    next = next.with_hour(cron_hour).unwrap();
                    next = next.with_minute(0).unwrap();
                    continue;
                }
            }

            if let CronParam::At(cron_weekday) = self.weekday {
                if next.weekday().num_days_from_sunday() != cron_weekday {
                    let delta_days =
                        (cron_weekday as i64 - next.weekday().num_days_from_sunday() as i64 + 7)
                            % 7;
                    next = next + chrono::Duration::days(delta_days);
                    next = next.with_hour(0).unwrap();
                    next = next.with_minute(0).unwrap();
                    continue;
                }
            }

            if let CronParam::At(cron_day) = self.day {
                if next.day() != cron_day {
                    if next.day() > cron_day
                        || !Self::month_has_day(next.year(), next.month(), cron_day)
                    {
                        next = Self::add_months(next.date(), 1)
                            .and_hms_opt(0, 0, 0)
                            .unwrap();
                        next = next.with_day(1).unwrap();
                        continue;
                    }
                    next = next.with_day(cron_day).unwrap();
                    next = next.with_hour(0).unwrap();
                    next = next.with_minute(0).unwrap();
                    continue;
                }
            }

            if let CronParam::At(cron_month) = self.month {
                if next.month() != cron_month {
                    if next.month() > cron_month {
                        next = next.with_year(next.year() + 1).unwrap();
                    }
                    next = next.with_month(cron_month).unwrap();
                    next = next.with_day(1).unwrap();
                    next = next.with_hour(0).unwrap();
                    next = next.with_minute(0).unwrap();
                    continue;
                }
            }

            done = true;
        }

        (next - now.naive_utc()).to_std().unwrap()
    }
}

impl<Time> CronParam<Time> {
    fn serialize(&self) -> String {
        match self {
            CronParam::Every => "*".to_string(),
            CronParam::At(min) => min.to_string(),
            _ => panic!("Invalid serialization. Do not use phantom marker."),
        }
    }

    fn from_str(s: &str) -> Result<Self> {
        if s == "*" {
            Ok(CronParam::Every)
        } else {
            let min = u32::from_str(s).map_err(|e| Error::Other(e.into()))?;
            Ok(CronParam::At(min))
        }
    }
}

impl CronTime for CronMinute {
    fn validate(p: &CronParam<Self>) -> Result<()> {
        match p {
            CronParam::At(min) if *min >= 60 => {
                Err(Error::Other("Cron Minute must be less than 60".into()))
            }
            _ => Ok(()),
        }
    }
}

impl CronTime for CronHour {
    fn validate(p: &CronParam<Self>) -> Result<()> {
        match p {
            CronParam::At(hour) if *hour >= 24 => {
                Err(Error::Other("Cron Hour must be less than 24".into()))
            }
            _ => Ok(()),
        }
    }
}

impl CronTime for CronDay {
    fn validate(p: &CronParam<Self>) -> Result<()> {
        match p {
            CronParam::At(day) if *day == 0 || *day >= 32 => {
                Err(Error::Other("Cron Day must be < 32 and > 0".into()))
            }
            _ => Ok(()),
        }
    }
}

impl CronTime for CronMonth {
    fn validate(p: &CronParam<Self>) -> Result<()> {
        match p {
            CronParam::At(month) if *month >= 13 => {
                Err(Error::Other("Cron Month must be less than 13".into()))
            }
            _ => Ok(()),
        }
    }
}

impl CronTime for CronWeekday {
    fn validate(p: &CronParam<Self>) -> Result<()> {
        match p {
            CronParam::At(weekday) if *weekday >= 8 => {
                Err(Error::Other("Cron Weekday must be less than 8".into()))
            }
            _ => Ok(()),
        }
    }
}

impl<T> Serialize for CronParam<T>
where
    T: CronTime,
{
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        T::validate(self).map_err(serde::ser::Error::custom)?;
        self.serialize().serialize(serializer)
    }
}

impl<'de, T> Deserialize<'de> for CronParam<T>
where
    T: CronTime,
{
    fn deserialize<D>(deserializer: D) -> std::result::Result<CronParam<T>, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let p = CronParam::from_str(&String::deserialize(deserializer)?)
            .map_err(serde::de::Error::custom)?;
        T::validate(&p).map_err(serde::de::Error::custom)?;
        Ok(p)
    }
}
