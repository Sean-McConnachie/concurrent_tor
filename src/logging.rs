use log::{LevelFilter, Log, Metadata, Record};

use crate::bx;

pub struct BasicLogger {
    log_level: LevelFilter,
}

impl BasicLogger {
    pub fn new(log_level: LevelFilter) -> BasicLogger {
        BasicLogger { log_level }
    }
}

impl Default for BasicLogger {
    fn default() -> Self {
        BasicLogger {
            log_level: LevelFilter::Info,
        }
    }
}

impl Log for BasicLogger {
    fn enabled(&self, metadata: &Metadata) -> bool {
        metadata.level() <= self.log_level
    }

    fn log(&self, record: &Record) {
        if self.enabled(record.metadata()) {
            println!("{} - {}", record.level(), record.args());
        }
    }

    fn flush(&self) {}
}

pub fn init_logging<L: Log + 'static>(logger: L, level: LevelFilter) {
    log::set_boxed_logger(bx!(logger)).expect("failed to set logger");
    log::set_max_level(level);
}
