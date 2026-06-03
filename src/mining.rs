//! Wraps types from Bitcoin Core's `capnp/mining.capnp`.
//! Do not modify — upstream schema changes belong in Bitcoin Core repository.
//!
//! High-level, Rust-native API for block template fetching and chain tip
//! monitoring. Wraps auto-generated `gen::mining_capnp` types.

use crate::client::{IntoCapnp, PublicClient};
use crate::error::BitcoinIpcError;
use crate::gen::common_capnp::block_ref;
use crate::gen::mining_capnp::block_template::Client as BlockTemplateIpcClient;
use crate::gen::mining_capnp::mining::Client as MiningIpcClient;
use crate::gen::proxy_capnp::thread::Client as ThreadIpcClient;
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;

use tracing::info;

/// Hash and height of a block — mirrors `capnp/common.capnp:BlockRef`.
#[derive(Clone, Debug)]
pub struct BlockRef {
    pub hash: Vec<u8>,
    pub height: i32,
}

impl<'a> From<block_ref::Reader<'a>> for BlockRef {
    fn from(r: block_ref::Reader<'a>) -> Self {
        Self {
            hash: r.get_hash().expect("block ref should have hash").to_vec(),
            height: r.get_height(),
        }
    }
}

/// Options for creating a new block template.
/// Mirrors `capnp/mining.capnp:BlockCreateOptions`.
#[derive(Clone, Debug)]
pub struct BlockCreateOptions {
    pub use_mempool: bool,
    pub block_reserved_weight: u64,
    pub coinbase_output_max_additional_sigops: u64,
}

impl<'a> IntoCapnp<crate::gen::mining_capnp::block_create_options::Builder<'a>>
    for BlockCreateOptions
{
    fn apply(&self, b: &mut crate::gen::mining_capnp::block_create_options::Builder<'a>) {
        b.set_use_mempool(self.use_mempool);
        b.set_block_reserved_weight(self.block_reserved_weight);
        b.set_coinbase_output_max_additional_sigops(self.coinbase_output_max_additional_sigops);
    }
}

/// Options for waiting on a new block template.
/// Mirrors `capnp/mining.capnp:BlockWaitOptions`.
#[derive(Clone, Debug)]
pub struct BlockWaitOptions {
    pub timeout: f64,
    pub fee_threshold: i64,
}

impl<'a> IntoCapnp<crate::gen::mining_capnp::block_wait_options::Builder<'a>> for BlockWaitOptions {
    fn apply(&self, b: &mut crate::gen::mining_capnp::block_wait_options::Builder<'a>) {
        b.set_timeout(self.timeout);
        b.set_fee_threshold(self.fee_threshold);
    }
}

/// Result of block validation.
/// Mirrors `capnp/mining.capnp:BlockValidationState`.
#[derive(Clone, Debug)]
pub struct BlockValidationState {
    pub mode: i32,
    pub result: i32,
    pub reject_reason: String,
    pub debug_message: String,
}

/// Emitted when the chain tip changes.
#[derive(Clone, Debug)]
pub struct TipChange {
    pub height: u32,
    pub hash: Vec<u8>,
}

/// High-level client for Bitcoin Core's Mining interface.
///
/// Provides block template creation and chain tip change monitoring.
/// Cloneable — all fields are reference-counted or `Clone`.
///
/// Created by `BitcoinIpc::new()` during bootstrap. Do not construct directly.
#[derive(Clone)]
pub struct MiningClient {
    mining_ipc_client: MiningIpcClient,
    thread_ipc_client: ThreadIpcClient,
}

impl PublicClient for MiningClient {
    type Ipc = MiningIpcClient;

    fn get_inner(&self) -> &Self::Ipc {
        &self.mining_ipc_client
    }

    fn get_thread(&self) -> &ThreadIpcClient {
        &self.thread_ipc_client
    }
}

impl MiningClient {
    /// Create a mining client from the bootstrap Init interface.
    ///
    /// Pure construction — no side effects. Call [`start_monitoring`](Self::start_monitoring)
    /// to begin tip change monitoring and obtain a [`Monitor`].
    pub(crate) async fn new(
        init_client: &crate::gen::init_capnp::init::Client,
        thread_client: &ThreadIpcClient,
    ) -> Result<Self, BitcoinIpcError> {
        let mut mining_client_request = init_client.make_mining_request();
        mining_client_request
            .get()
            .get_context()?
            .set_thread(thread_client.clone());
        let mining_client_response = mining_client_request.send().promise.await?;
        let mining_ipc_client: MiningIpcClient = mining_client_response.get()?.get_result()?;

        info!("IPC mining client successfully created.");

        Ok(Self {
            mining_ipc_client,
            thread_ipc_client: thread_client.clone(),
        })
    }

    /// Check if the node is running on testnet/regtest.
    pub async fn is_test_chain(&self) -> Result<bool, BitcoinIpcError> {
        self.request(|c| c.is_test_chain_request())?
            .send()
            .await
            .and_then(|response| Ok(response.get()?.get_result()))
    }

    /// Check if the node is still in Initial Block Download.
    pub async fn is_initial_block_download(&self) -> Result<bool, BitcoinIpcError> {
        self.request(|c| c.is_initial_block_download_request())?
            .send()
            .await
            .and_then(|response| Ok(response.get()?.get_result()))
    }

    /// Get the current chain tip.
    pub async fn get_tip(&self) -> Result<Option<BlockRef>, BitcoinIpcError> {
        self.request(|c| c.get_tip_request())?
            .send()
            .await
            .and_then(|response| {
                let reader = response.get()?;
                if !reader.get_has_result() {
                    return Ok(None);
                }
                Ok(Some(reader.get_result()?.into()))
            })
    }

    /// Wait for the chain tip to change from `current_tip`, with a timeout in seconds.
    /// Returns the new tip.
    pub async fn wait_tip_changed(
        &self,
        current_tip: &[u8],
        timeout: f64,
    ) -> Result<BlockRef, BitcoinIpcError> {
        self.request(|c| c.wait_tip_changed_request())?
            .set(|b| {
                b.set_current_tip(current_tip);
                b.set_timeout(timeout);
            })
            .send()
            .await
            .and_then(|response| Ok(response.get()?.get_result()?.into()))
    }

    /// Create a new block template from the node.
    ///
    /// Returns a raw `BlockTemplate` client. For the high-level version
    /// with tip monitoring and subscription, use [`start_monitoring`](Self::start_monitoring).
    pub async fn create_new_block(
        &self,
        options: &BlockCreateOptions,
    ) -> Result<crate::gen::mining_capnp::block_template::Client, BitcoinIpcError> {
        let mut req = self.mining_ipc_client.create_new_block_request();
        options.apply(&mut req.get().get_options()?);
        let result = req.send().promise.await?;
        Ok(result.get()?.get_result()?)
    }

    /// Start tip change monitoring and create a block template handle.
    ///
    /// Spawns a background task that watches the chain tip via
    /// `mining.waitTipChanged()`. Returns a [`Monitor`] that provides
    /// `subscribe_tip_changes()` and `fetch_block_template()`.
    ///
    /// The monitoring loop runs until the `CancellationToken` passed to
    /// [`MiningClient::new`] is cancelled.
    pub async fn start_monitoring(
        &self,
        coinbase_output_max_additional_size: u32,
        coinbase_output_max_additional_sigops: u16,
    ) -> Result<MonitorClient, BitcoinIpcError> {
        info!(
            "Coinbase output max additional size: {}",
            coinbase_output_max_additional_size
        );
        info!(
            "Coinbase output max additional sigops: {}",
            coinbase_output_max_additional_sigops
        );

        let coinbase_weight = (coinbase_output_max_additional_size * 4) as u64;
        let block_reserved_weight = coinbase_weight.max(2000);
        let options = BlockCreateOptions {
            use_mempool: true,
            block_reserved_weight,
            coinbase_output_max_additional_sigops: coinbase_output_max_additional_sigops as u64,
        };

        let template_ipc_client = self.create_new_block(&options).await?;

        let (tip_change_tx, _) = broadcast::channel(16);

        let monitor = MonitorClient {
            template_ipc_client,
            thread_ipc_client: self.thread_ipc_client.clone(),
            tip_change_tx: tip_change_tx.clone(),
        };

        let stop = CancellationToken::new();
        spawn_monitor(self.clone(), tip_change_tx, stop);

        Ok(monitor)
    }
}

/// Handle for an active chain tip monitor and block template.
///
/// Created by [`MiningClient::start_monitoring`]. Provides
/// subscription to tip changes and block template fetching.
///
/// Cloning a `Monitor` shares the same underlying template client
/// and broadcast sender.
#[derive(Clone)]
pub struct MonitorClient {
    template_ipc_client: BlockTemplateIpcClient,
    thread_ipc_client: ThreadIpcClient,
    tip_change_tx: broadcast::Sender<TipChange>,
}

impl PublicClient for MonitorClient {
    type Ipc = BlockTemplateIpcClient;

    fn get_inner(&self) -> &Self::Ipc {
        &self.template_ipc_client
    }

    fn get_thread(&self) -> &ThreadIpcClient {
        &self.thread_ipc_client
    }
}

impl MonitorClient {
    /// Subscribe to chain tip change events.
    pub fn subscribe_tip_changes(&self) -> broadcast::Receiver<TipChange> {
        self.tip_change_tx.subscribe()
    }

    /// Fetch the current block template and coinbase from Bitcoin Core.
    /// Returns (block_bytes, coinbase_bytes).
    pub async fn fetch_block_template(&self) -> Result<(Vec<u8>, Vec<u8>), BitcoinIpcError> {
        let block = self
            .request(|c| c.get_block_request())?
            .send()
            .await
            .and_then(|response| Ok(response.get()?.get_result()?.to_vec()))?;
        let coinbase = self
            .request(|c| c.get_coinbase_tx_request())?
            .send()
            .await
            .and_then(|response| Ok(response.get()?.get_result()?.to_vec()))?;
        Ok((block, coinbase))
    }

    /// Get the raw block header bytes (80 bytes).
    pub async fn get_block_header(&self) -> Result<Vec<u8>, BitcoinIpcError> {
        self.request(|c| c.get_block_header_request())?
            .send()
            .await
            .and_then(|response| Ok(response.get()?.get_result()?.to_vec()))
    }

    /// Get the raw block bytes (full serialized block).
    pub async fn get_block(&self) -> Result<Vec<u8>, BitcoinIpcError> {
        self.request(|c| c.get_block_request())?
            .send()
            .await
            .and_then(|response| Ok(response.get()?.get_result()?.to_vec()))
    }

    /// Get per-transaction fees for all transactions in the block template.
    pub async fn get_tx_fees(&self) -> Result<Vec<i64>, BitcoinIpcError> {
        self.request(|c| c.get_tx_fees_request())?
            .send()
            .await
            .and_then(|response| {
                let result = response.get()?.get_result()?;
                Ok((0..result.len()).map(|i| result.get(i)).collect())
            })
    }

    /// Get per-transaction sigop counts for all transactions in the block template.
    pub async fn get_tx_sigops(&self) -> Result<Vec<i64>, BitcoinIpcError> {
        self.request(|c| c.get_tx_sigops_request())?
            .send()
            .await
            .and_then(|response| {
                let result = response.get()?.get_result()?;
                Ok((0..result.len()).map(|i| result.get(i)).collect())
            })
    }

    /// Get the serialized coinbase transaction.
    pub async fn get_coinbase_tx(&self) -> Result<Vec<u8>, BitcoinIpcError> {
        self.request(|c| c.get_coinbase_tx_request())?
            .send()
            .await
            .and_then(|response| Ok(response.get()?.get_result()?.to_vec()))
    }

    /// Get the witness commitment hash from the coinbase transaction.
    pub async fn get_coinbase_commitment(&self) -> Result<Vec<u8>, BitcoinIpcError> {
        self.request(|c| c.get_coinbase_commitment_request())?
            .send()
            .await
            .and_then(|response| Ok(response.get()?.get_result()?.to_vec()))
    }

    /// Get the index of the witness commitment output in the coinbase.
    pub async fn get_witness_commitment_index(&self) -> Result<i32, BitcoinIpcError> {
        self.request(|c| c.get_witness_commitment_index_request())?
            .send()
            .await
            .and_then(|response| Ok(response.get()?.get_result()))
    }

    /// Get the coinbase transaction's merkle proof path.
    pub async fn get_coinbase_merkle_path(&self) -> Result<Vec<Vec<u8>>, BitcoinIpcError> {
        self.request(|c| c.get_coinbase_merkle_path_request())?
            .send()
            .await
            .and_then(|response| {
                let result = response.get()?.get_result()?;
                let mut path = Vec::new();
                for i in 0..result.len() {
                    path.push(result.get(i)?.to_vec());
                }
                Ok(path)
            })
    }

    /// Submit a block solution to the node.
    pub async fn submit_solution(
        &self,
        version: u32,
        timestamp: u32,
        nonce: u32,
        coinbase: &[u8],
    ) -> Result<bool, BitcoinIpcError> {
        self.request(|c| c.submit_solution_request())?
            .set(|b| {
                b.set_version(version);
                b.set_timestamp(timestamp);
                b.set_nonce(nonce);
                b.set_coinbase(coinbase);
            })
            .send()
            .await
            .and_then(|response| Ok(response.get()?.get_result()))
    }

    /// Wait for the block template to change (new transactions, new fee thresholds, etc).
    /// Returns a new BlockTemplate client — caller should re-create the Monitor.
    pub async fn wait_next(
        &self,
        options: Option<&BlockWaitOptions>,
    ) -> Result<crate::gen::mining_capnp::block_template::Client, BitcoinIpcError> {
        self.request(|c| c.wait_next_request())?
            .set(|b| {
                if let Some(o) = options {
                    let mut opts = b.reborrow().init_options();
                    o.apply(&mut opts);
                }
            })
            .send()
            .await
            .and_then(|response| Ok(response.get()?.get_result()?))
    }
}

/// Spawn a background task that watches the chain tip via
/// [`mining.waitTipChanged()`](MiningIpcClient::wait_tip_changed_request).
///
/// The loop runs until a fatal error occurs or the local cancellation
/// token fires. On each iteration:
///
/// 1. A new `wait_tip_changed` request is prepared via [`PublicClient::request`]
/// 2. The request is sent with [`IpcRequest::send_cancellable`], racing the RPC
///    against the `stop` cancellation token
/// 3. If the token fires while waiting, the capnp `Promise` drops, a `Finish`
///    message is sent to the server, and the loop exits
/// 4. If the RPC completes, the tip is compared with the cached height and a
///    [`TipChange`] is broadcast via `tip_change_tx` if the chain advanced
///
/// Fatal errors (request creation failure, initial tip fetch failure)
/// cancel the token to abort any in-flight RPC before returning.
fn spawn_monitor(
    mining_client: MiningClient,
    tip_change_tx: broadcast::Sender<TipChange>,
    stop: CancellationToken,
) {
    tokio::task::spawn_local(async move {
        let req = match mining_client.request(|c| c.get_tip_request()) {
            Ok(r) => r,
            Err(e) => {
                tracing::error!("Failed to prepare get_tip request: {}", e);
                stop.cancel();
                return;
            }
        };

        let response = match req.send().await {
            Ok(r) => r,
            Err(e) => {
                tracing::error!("Failed to get initial tip: {}", e);
                stop.cancel();
                return;
            }
        };

        let current_tip = match response.get() {
            Ok(result) => match result.get_result() {
                Ok(tip) => tip,
                Err(e) => {
                    tracing::error!("Failed to extract tip from response: {}", e);
                    stop.cancel();
                    return;
                }
            },
            Err(e) => {
                tracing::error!("Failed to get tip response: {}", e);
                stop.cancel();
                return;
            }
        };

        let mut current_tip_height = current_tip.get_height();
        let mut current_tip_hash = match current_tip.get_hash() {
            Ok(hash) => hash.to_vec(),
            Err(e) => {
                tracing::error!("Failed to get tip hash: {}", e);
                stop.cancel();
                return;
            }
        };

        loop {
            let token = stop.clone();
            let req = match mining_client.request(|c| c.wait_tip_changed_request()) {
                Ok(r) => r,
                Err(e) => {
                    tracing::error!("Failed to prepare wait_tip_changed request: {}", e);
                    stop.cancel();
                    return;
                }
            }
            .set(|b| {
                b.set_current_tip(&current_tip_hash);
                b.set_timeout(f64::MAX);
            });

            match req.send_cancellable(token).await {
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
                        Ok(hash) => hash.to_vec(),
                        Err(e) => {
                            tracing::error!("Failed to get new tip hash: {}", e);
                            continue;
                        }
                    };

                    if new_height > current_tip_height {
                        info!("Tip changed! New height: {}", new_height);
                        current_tip_height = new_height;
                        let tip = TipChange {
                            height: new_height as u32,
                            hash: new_hash.clone(),
                        };
                        current_tip_hash = new_hash;
                        let _ = tip_change_tx.send(tip);
                    }
                }
                Err(BitcoinIpcError::Cancelled) => {
                    info!("Local cancellation token fired, exiting tip change monitoring loop");
                    break;
                }
                Err(e) => {
                    tracing::error!("Failed to get response: {}", e);
                    continue;
                }
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::gen::common_capnp::block_ref;
    use crate::gen::mining_capnp::{block_create_options, block_wait_options};

    #[test]
    fn test_block_ref_from_capnp() {
        let mut message = ::capnp::message::Builder::new_default();
        let mut builder = message.init_root::<block_ref::Builder<'_>>();
        builder.set_hash(b"\x00\x01\x02\xff");
        builder.set_height(42);

        let reader = builder.into_reader();
        let result: BlockRef = reader.into();

        assert_eq!(result.hash, vec![0x00, 0x01, 0x02, 0xff]);
        assert_eq!(result.height, 42);
    }

    #[test]
    fn test_block_create_options_to_capnp() {
        let options = BlockCreateOptions {
            use_mempool: true,
            block_reserved_weight: 2000,
            coinbase_output_max_additional_sigops: 100,
        };

        let mut message = ::capnp::message::Builder::new_default();
        let mut builder = message.init_root::<block_create_options::Builder<'_>>();
        options.apply(&mut builder);

        let reader = builder.into_reader();
        assert!(reader.get_use_mempool());
        assert_eq!(reader.get_block_reserved_weight(), 2000);
        assert_eq!(reader.get_coinbase_output_max_additional_sigops(), 100);
    }

    #[test]
    fn test_block_wait_options_to_capnp() {
        let options = BlockWaitOptions {
            timeout: 30.5,
            fee_threshold: 1000,
        };

        let mut message = ::capnp::message::Builder::new_default();
        let mut builder = message.init_root::<block_wait_options::Builder<'_>>();
        options.apply(&mut builder);

        let reader = builder.into_reader();
        assert_eq!(reader.get_timeout(), 30.5);
        assert_eq!(reader.get_fee_threshold(), 1000);
    }
}
