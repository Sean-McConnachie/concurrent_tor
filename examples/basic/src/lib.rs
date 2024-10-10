pub mod platforms;

use concurrent_tor::execution::scheduler::{MainTorClient, TorClientImpl};

pub type Client = TorClientImpl;
pub type MainClient = MainTorClient;
