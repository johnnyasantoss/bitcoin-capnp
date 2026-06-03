//! # bitcoin-ipc
//!
//! A minimal, general-purpose Cap'n Proto IPC client for Bitcoin Core.
//! Consumers subscribe to tip change events via `tokio::sync::broadcast`
//! and fetch raw block/coinbase data.

/// Error types returned by the Bitcoin Core IPC client.
pub mod error;

mod gen;

/// # Auto-generated Cap'n Proto modules
/// Rust code generated directly from `*.capnp`
pub use gen::*;

use crate::gen::mining_capnp::block_template::Client as BlockTemplateIpcClient;
use crate::gen::mining_capnp::mining::Client as MiningIpcClient;
use crate::gen::proxy_capnp::thread::Client as ThreadIpcClient;
use crate::gen::proxy_capnp::thread_map::Client as ThreadMapIpcClient;

use capnp_rpc::{rpc_twoparty_capnp, twoparty, RpcSystem};
use error::BitcoinIpcError;
use std::path::Path;
use std::sync::{Arc, Mutex};
use tokio::net::UnixStream;
use tokio::sync::broadcast;
use tokio_util::compat::*;
use tokio_util::sync::CancellationToken;

use tracing::info;

/// Emitted when the chain tip changes.
#[derive(Clone, Debug)]
pub struct TipChange {
    pub height: u32,
    pub hash: Vec<u8>,
}

#[derive(Clone)]
pub struct BitcoinCoreIpc {
    coinbase_output_max_additional_size: u32,
    coinbase_output_max_additional_sigops: u16,
    mining_ipc_client: MiningIpcClient,
    thread_ipc_client: ThreadIpcClient,
    template_ipc_client: Arc<Mutex<BlockTemplateIpcClient>>,
    cancellation_token: CancellationToken,
    tip_change_tx: broadcast::Sender<TipChange>,
}

impl BitcoinCoreIpc {
    pub async fn new(
        bitcoin_core_unix_socket_path: &Path,
        cancellation_token: CancellationToken,
        coinbase_output_max_additional_size: u32,
        coinbase_output_max_additional_sigops: u16,
    ) -> Result<Self, BitcoinIpcError> {
        info!(
            "Creating new IPC Bitcoin Core Connection over UNIX socket: {}",
            bitcoin_core_unix_socket_path.display()
        );
        info!(
            "Coinbase output max additional size: {}",
            coinbase_output_max_additional_size
        );
        info!(
            "Coinbase output max additional sigops: {}",
            coinbase_output_max_additional_sigops
        );

        let stream = UnixStream::connect(bitcoin_core_unix_socket_path).await?;
        let (reader, writer) = stream.into_split();
        let reader_compat = reader.compat();
        let writer_compat = writer.compat_write();

        let rpc_network = Box::new(twoparty::VatNetwork::new(
            reader_compat,
            writer_compat,
            rpc_twoparty_capnp::Side::Client,
            Default::default(),
        ));

        let mut rpc_system = RpcSystem::new(rpc_network, None);
        let bootstrap_client: crate::gen::init_capnp::init::Client =
            rpc_system.bootstrap(rpc_twoparty_capnp::Side::Server);

        tokio::task::spawn_local(rpc_system);

        let construct_response = bootstrap_client.construct_request().send().promise.await?;

        let thread_map: ThreadMapIpcClient = construct_response.get()?.get_thread_map()?;
        let thread_request = thread_map.make_thread_request();
        let thread_response = thread_request.send().promise.await?;

        let thread_ipc_client: ThreadIpcClient = thread_response.get()?.get_result()?;

        info!("IPC execution thread client successfully created.");

        let mut mining_client_request = bootstrap_client.make_mining_request();
        mining_client_request
            .get()
            .get_context()?
            .set_thread(thread_ipc_client.clone());
        let mining_client_response = mining_client_request.send().promise.await?;
        let mining_ipc_client: MiningIpcClient = mining_client_response.get()?.get_result()?;

        info!("IPC mining client successfully created.");

        let mut template_ipc_client_request = mining_ipc_client.create_new_block_request();
        let mut template_ipc_client_request_options =
            template_ipc_client_request.get().get_options()?;

        let coinbase_weight = (coinbase_output_max_additional_size * 4) as u64;
        let block_reserved_weight = coinbase_weight.max(2000); // 2000 is the minimum block reserved weight
        template_ipc_client_request_options.set_block_reserved_weight(block_reserved_weight);
        template_ipc_client_request_options.set_coinbase_output_max_additional_sigops(
            coinbase_output_max_additional_sigops as u64,
        );
        template_ipc_client_request_options.set_use_mempool(true);

        let template_ipc_client = template_ipc_client_request
            .send()
            .promise
            .await?
            .get()?
            .get_result()?;

        let (tip_change_tx, _) = broadcast::channel(16);

        Ok(Self {
            coinbase_output_max_additional_size,
            coinbase_output_max_additional_sigops,
            mining_ipc_client,
            thread_ipc_client,
            template_ipc_client: Arc::new(Mutex::new(template_ipc_client)),
            cancellation_token,
            tip_change_tx,
        })
    }

    pub async fn run(&self) {
        self.monitor_tip_changes();
        self.cancellation_token.cancelled().await;
    }

    async fn refresh_template_ipc_client(&self) -> Result<(), BitcoinIpcError> {
        info!("Refreshing template IPC client");

        let mut template_ipc_client_request = self.mining_ipc_client.create_new_block_request();
        let mut template_ipc_client_request_options =
            template_ipc_client_request.get().get_options()?;

        let coinbase_weight = (self.coinbase_output_max_additional_size * 4) as u64;
        let block_reserved_weight = coinbase_weight.max(2000);
        template_ipc_client_request_options.set_block_reserved_weight(block_reserved_weight);
        template_ipc_client_request_options.set_coinbase_output_max_additional_sigops(
            self.coinbase_output_max_additional_sigops as u64,
        );
        template_ipc_client_request_options.set_use_mempool(true);

        let new_client = template_ipc_client_request
            .send()
            .promise
            .await?
            .get()?
            .get_result()?;

        *self.template_ipc_client.lock().unwrap() = new_client;
        Ok(())
    }

    /// Subscribe to chain tip change events.
    pub fn subscribe_tip_changes(&self) -> broadcast::Receiver<TipChange> {
        self.tip_change_tx.subscribe()
    }

    /// Fetch the current block template and coinbase from Bitcoin Core.
    /// Returns (block_bytes, coinbase_bytes).
    pub async fn fetch_block_template(&self) -> Result<(Vec<u8>, Vec<u8>), BitcoinIpcError> {
        let client = self.template_ipc_client.lock().unwrap();
        let mut block_req = client.get_block_request();
        block_req
            .get()
            .get_context()?
            .set_thread(self.thread_ipc_client.clone());
        let block_bytes = block_req
            .send()
            .promise
            .await?
            .get()?
            .get_result()?
            .to_vec();

        let mut coinbase_req = client.get_coinbase_tx_request();
        coinbase_req
            .get()
            .get_context()?
            .set_thread(self.thread_ipc_client.clone());
        let coinbase_bytes = coinbase_req
            .send()
            .promise
            .await?
            .get()?
            .get_result()?
            .to_vec();

        Ok((block_bytes, coinbase_bytes))
    }

    fn monitor_tip_changes(&self) {
        let self_clone = self.clone();
        tokio::task::spawn_local(async move {
            let mut get_tip_request = self_clone.mining_ipc_client.get_tip_request();
            match get_tip_request.get().get_context() {
                Ok(mut context) => context.set_thread(self_clone.thread_ipc_client.clone()),
                Err(e) => {
                    tracing::error!("Failed to set thread: {}", e);
                    tracing::error!("Activating cancellation token");
                    self_clone.cancellation_token.cancel();
                    return;
                }
            }

            // First, get the current tip before entering the loop
            let get_tip_response = match get_tip_request.send().promise.await {
                Ok(response) => response,
                Err(e) => {
                    tracing::error!("Failed to get initial tip: {}", e);
                    tracing::error!("Activating cancellation token");
                    self_clone.cancellation_token.cancel();
                    return;
                }
            };

            let current_tip = match get_tip_response.get() {
                Ok(result) => match result.get_result() {
                    Ok(tip) => tip,
                    Err(e) => {
                        tracing::error!("Failed to extract tip from response: {}", e);
                        tracing::error!("Activating cancellation token");
                        self_clone.cancellation_token.cancel();
                        return;
                    }
                },
                Err(e) => {
                    tracing::error!("Failed to get tip response: {}", e);
                    tracing::error!("Activating cancellation token");
                    self_clone.cancellation_token.cancel();
                    return;
                }
            };

            let mut current_tip_height = current_tip.get_height();
            let mut current_tip_hash = match current_tip.get_hash() {
                Ok(hash) => hash.to_vec(), // Convert to owned Vec<u8>
                Err(e) => {
                    tracing::error!("Failed to get tip hash: {}", e);
                    tracing::error!("Activating cancellation token");
                    self_clone.cancellation_token.cancel();
                    return;
                }
            };

            loop {
                // Create a new request for each iteration
                let mut wait_tip_changed_request =
                    self_clone.mining_ipc_client.wait_tip_changed_request();

                match wait_tip_changed_request.get().get_context() {
                    Ok(mut context) => context.set_thread(self_clone.thread_ipc_client.clone()),
                    Err(e) => {
                        tracing::error!("Failed to set thread: {}", e);
                        tracing::error!("Activating cancellation token");
                        self_clone.cancellation_token.cancel();
                        return;
                    }
                }

                wait_tip_changed_request
                    .get()
                    .set_current_tip(&current_tip_hash);
                wait_tip_changed_request.get().set_timeout(f64::MAX); // no timeout, wait forever

                tokio::select! {
                    _ = self_clone.cancellation_token.cancelled() => {
                        tracing::info!("Cancellation token cancelled, exiting tip change monitoring loop");
                        break;
                    }
                    wait_tip_changed_response = wait_tip_changed_request.send().promise => {
                        match wait_tip_changed_response {
                            Ok(response) => {
                                let result = match response.get() {
                                    Ok(result) => result,
                                    Err(e) => {
                                        tracing::error!("Failed to get response: {}", e);
                                        continue;
                                    }
                                };

                                let new_tip = match result.get_result() {
                                    Ok(new_tip) => new_tip,
                                    Err(e) => {
                                        tracing::error!("Failed to get new tip: {}", e);
                                        continue;
                                    }
                                };

                                let new_height = new_tip.get_height();
                                let new_hash = match new_tip.get_hash() {
                                    Ok(hash) => hash.to_vec(), // Convert to owned Vec<u8>
                                    Err(e) => {
                                        tracing::error!("Failed to get new tip hash: {}", e);
                                        continue;
                                    }
                                };

                                // don't update the tip if the height is the same
                                if new_height > current_tip_height {
                                    info!("Tip changed! New height: {}", new_height);
                                    current_tip_height = new_height;
                                    let tip = TipChange {
                                        height: new_height as u32,
                                        hash: new_hash.clone(),
                                    };
                                    current_tip_hash = new_hash;
                                    let _ = self_clone.tip_change_tx.send(tip);
                                }
                            }
                            Err(e) => {
                                tracing::error!("Failed to get response: {}", e);
                                // Continue the loop to retry
                                continue;
                            }
                        }
                    }
                }
            }
        });
    }
}
