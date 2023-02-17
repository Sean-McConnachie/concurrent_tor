use log::{debug, info};
use std::sync::Arc;

use tls_api::{TlsConnector as TlsConnectorTrait, TlsConnectorBuilder};
use tls_api_openssl::TlsConnector;

use crate::circuit;
use crate::configs;
use crate::dispatcher::Dispatcher;
use crate::task;

pub struct CircuitHandler<D: Dispatcher<T>, T: task::Task> {
    config: configs::CircuitHandlerConfig,
    circuits: Vec<circuit::Circuit<D, T>>,
}

impl<D, T> CircuitHandler<D, T>
where
    D: Dispatcher<T>,
    T: task::Task,
{
    pub fn new(config: configs::CircuitHandlerConfig) -> Self {
        debug!(
            "Created new circuit handler with {} workers",
            config.num_workers
        );
        CircuitHandler {
            config,
            circuits: vec![],
        }
    }

    /// Default circuits. Default config. Nothing special...
    pub async fn build_circuits(&mut self, task_dispatcher: Arc<D>) -> Result<(), anyhow::Error> {
        debug!("Building tor client");
        let tor_config = arti_client::TorClientConfig::default();
        let tor_client = arti_client::TorClient::create_bootstrapped(tor_config).await?;

        self.circuits = (0..self.config.num_workers)
            .map(|worker| {
                debug!("Building worker {}", worker);
                let tor_circuit = tor_client.isolated_client();
                let tls_connector = TlsConnector::builder()?.build()?;
                let tor_connector = arti_hyper::ArtiHttpConnector::new(tor_circuit, tls_connector);
                let client = hyper::Client::builder().build::<_, hyper::Body>(tor_connector);
                let t = Arc::clone(&task_dispatcher);
                Ok::<circuit::Circuit<D, T>, anyhow::Error>(circuit::Circuit::new(
                    worker,
                    client,
                    t,
                    self.config.task_buffer.clone(),
                    self.config.no_task_sleep.clone(),
                ))
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(())
    }

    /// Set custom tor clients, useful when setting custom configs.
    pub fn set_circuits(&mut self, circuits: Vec<circuit::Circuit<D, T>>) {
        debug!("Setting {} circuits", circuits.len());
        self.config.num_workers = circuits.len();
        self.circuits = circuits
    }

    // TODO: Add a clean exit
    /// Calls `.run` on each `Circuit` to start accepting tasks from the dispatcher.
    pub async fn run(&self) -> Result<(), anyhow::Error> {
        info!("Running circuit handler");
        let handles = self
            .circuits
            .iter()
            .map(|circuit| circuit.run())
            .collect::<Vec<_>>();
        futures::future::join_all(handles).await;
        Ok(())
    }
}
