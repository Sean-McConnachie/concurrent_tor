pub mod platforms;

use concurrent_tor::{
    execution::client::{CStandardClient, MainCStandardClient},
    Result,
};

pub type Client = CStandardClient;
pub type MainClient = MainCStandardClient;

pub async fn build_main_client() -> Result<MainClient> {
    Ok(MainCStandardClient::new())
}

// pub type Client = CTorClient;
// pub type MainClient = MainCTorClient;
//
// pub async fn build_main_client() -> Result<MainClient> {
//     MainCTorClient::new(TorClientConfig::default()).await
// }
