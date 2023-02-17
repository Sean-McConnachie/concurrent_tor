//! ## Concurrent Tor
//! This library was built to allow for multiple tor clients, with different ips to run simultaneously,
//! getting their data from a user-defined dispatcher (which could implement a queue system). It
//! has supports for user-defined "Tasks" which must implement the `Task` trait. This allows for a
//! callback when a request has been completed, for example, to save to a file/insert to a database.
//!
//! There is also support for scraping the web archive, if, for exampple, a page has been deleted.

/// External libraries
pub use anyhow;
pub use async_trait::async_trait;
pub use enum_delegate::delegate;
pub use hyper;
pub use log;
pub use tokio;

/// Library modules
pub mod circuit;
pub mod circuit_handler;
pub mod configs;
pub mod dispatcher;
pub mod errors;
pub mod logging;
pub mod macros;
pub mod request;
pub mod task;
