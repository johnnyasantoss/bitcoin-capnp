//! Wraps types from Bitcoin Core's `capnp/mining.capnp`.
//! Do not modify — upstream schema changes belong in Bitcoin Core repository.
//!
//! High-level, Rust-native API for block template fetching and chain tip
//! monitoring. Wraps auto-generated `gen::mining_capnp` types.

use crate::actor::{ActorTx, Command};
use crate::client::IntoCapnp;
use crate::error::BitcoinCapnpError;
use crate::gen::common_capnp::block_ref;
use tokio::sync::{broadcast, oneshot};
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
    pub height: u64,
    pub hash: Vec<u8>,
}

/// High-level client for Bitcoin Core's Mining interface.
///
/// Provides block template creation and chain tip change monitoring.
/// `Send` and `Clone` — safe to use across async tasks.
///
/// Created by `BitcoinCapnp::new()` during bootstrap. Do not construct directly.
#[derive(Clone)]
pub struct MiningClient {
    cmd_tx: ActorTx,
}

impl MiningClient {
    pub(crate) fn new(cmd_tx: ActorTx) -> Self {
        Self { cmd_tx }
    }

    /// Check if the node is running on testnet/regtest.
    pub async fn is_test_chain(&self) -> Result<bool, BitcoinCapnpError> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(Command::MiningIsTestChain { reply: tx })
            .map_err(|_| BitcoinCapnpError::ActorDisconnected)?;
        rx.await.map_err(|_| BitcoinCapnpError::ActorDisconnected)?
    }

    /// Check if the node is still in Initial Block Download.
    pub async fn is_initial_block_download(&self) -> Result<bool, BitcoinCapnpError> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(Command::MiningIsInitialBlockDownload { reply: tx })
            .map_err(|_| BitcoinCapnpError::ActorDisconnected)?;
        rx.await.map_err(|_| BitcoinCapnpError::ActorDisconnected)?
    }

    /// Get the current chain tip.
    pub async fn get_tip(&self) -> Result<Option<BlockRef>, BitcoinCapnpError> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(Command::MiningGetTip { reply: tx })
            .map_err(|_| BitcoinCapnpError::ActorDisconnected)?;
        rx.await.map_err(|_| BitcoinCapnpError::ActorDisconnected)?
    }

    /// Wait for the chain tip to change from `current_tip`, with a timeout in seconds.
    /// Returns the new tip.
    pub async fn wait_tip_changed(
        &self,
        current_tip: &[u8],
        timeout: f64,
    ) -> Result<BlockRef, BitcoinCapnpError> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(Command::MiningWaitTipChanged {
                current_tip: current_tip.to_vec(),
                timeout,
                reply: tx,
            })
            .map_err(|_| BitcoinCapnpError::ActorDisconnected)?;
        rx.await.map_err(|_| BitcoinCapnpError::ActorDisconnected)?
    }

    /// Create a new block template from the node.
    ///
    /// Updates the template for **all** active monitors.
    pub async fn create_new_block(
        &self,
        options: &BlockCreateOptions,
    ) -> Result<(), BitcoinCapnpError> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(Command::MiningCreateNewBlock {
                options: options.clone(),
                reply: tx,
            })
            .map_err(|_| BitcoinCapnpError::ActorDisconnected)?;
        rx.await.map_err(|_| BitcoinCapnpError::ActorDisconnected)?
    }

    /// Start tip change monitoring and create a block template handle.
    ///
    /// Spawns a background task that watches the chain tip.
    /// Returns a [`MonitorClient`] that provides
    /// `subscribe_tip_changes()` and block template fetching.
    pub async fn start_monitoring(
        &self,
        coinbase_output_max_additional_size: u32,
        coinbase_output_max_additional_sigops: u16,
    ) -> Result<MonitorClient, BitcoinCapnpError> {
        info!(
            "Coinbase output max additional size: {}",
            coinbase_output_max_additional_size
        );
        info!(
            "Coinbase output max additional sigops: {}",
            coinbase_output_max_additional_sigops
        );

        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(Command::MiningStartMonitoring {
                coinbase_output_max_additional_size,
                coinbase_output_max_additional_sigops,
                reply: tx,
            })
            .map_err(|_| BitcoinCapnpError::ActorDisconnected)?;
        let (monitor_id, tip_change_tx) = rx
            .await
            .map_err(|_| BitcoinCapnpError::ActorDisconnected)??;

        Ok(MonitorClient {
            cmd_tx: self.cmd_tx.clone(),
            monitor_id,
            tip_change_tx,
        })
    }
}

/// Handle for an active chain tip monitor and block template.
///
/// Created by [`MiningClient::start_monitoring`]. Provides
/// subscription to tip changes and block template fetching.
///
/// `Send` and `Clone`.
#[derive(Clone)]
pub struct MonitorClient {
    cmd_tx: ActorTx,
    monitor_id: u64,
    tip_change_tx: broadcast::Sender<TipChange>,
}

impl MonitorClient {
    /// Subscribe to chain tip change events.
    pub fn subscribe_tip_changes(&self) -> broadcast::Receiver<TipChange> {
        self.tip_change_tx.subscribe()
    }

    /// Fetch the current block template and coinbase from Bitcoin Core.
    /// Returns (block_bytes, coinbase_bytes).
    pub async fn fetch_block_template(&self) -> Result<(Vec<u8>, Vec<u8>), BitcoinCapnpError> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(Command::MonitorFetchBlockTemplate {
                monitor_id: self.monitor_id,
                reply: tx,
            })
            .map_err(|_| BitcoinCapnpError::ActorDisconnected)?;
        rx.await.map_err(|_| BitcoinCapnpError::ActorDisconnected)?
    }

    /// Get the raw block header bytes (80 bytes).
    pub async fn get_block_header(&self) -> Result<Vec<u8>, BitcoinCapnpError> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(Command::MonitorGetBlockHeader {
                monitor_id: self.monitor_id,
                reply: tx,
            })
            .map_err(|_| BitcoinCapnpError::ActorDisconnected)?;
        rx.await.map_err(|_| BitcoinCapnpError::ActorDisconnected)?
    }

    /// Get the raw block bytes (full serialized block).
    pub async fn get_block(&self) -> Result<Vec<u8>, BitcoinCapnpError> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(Command::MonitorGetBlock {
                monitor_id: self.monitor_id,
                reply: tx,
            })
            .map_err(|_| BitcoinCapnpError::ActorDisconnected)?;
        rx.await.map_err(|_| BitcoinCapnpError::ActorDisconnected)?
    }

    /// Get per-transaction fees for all transactions in the block template.
    pub async fn get_tx_fees(&self) -> Result<Vec<i64>, BitcoinCapnpError> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(Command::MonitorGetTxFees {
                monitor_id: self.monitor_id,
                reply: tx,
            })
            .map_err(|_| BitcoinCapnpError::ActorDisconnected)?;
        rx.await.map_err(|_| BitcoinCapnpError::ActorDisconnected)?
    }

    /// Get per-transaction sigop counts for all transactions in the block template.
    pub async fn get_tx_sigops(&self) -> Result<Vec<i64>, BitcoinCapnpError> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(Command::MonitorGetTxSigops {
                monitor_id: self.monitor_id,
                reply: tx,
            })
            .map_err(|_| BitcoinCapnpError::ActorDisconnected)?;
        rx.await.map_err(|_| BitcoinCapnpError::ActorDisconnected)?
    }

    /// Get the serialized coinbase transaction.
    pub async fn get_coinbase_tx(&self) -> Result<Vec<u8>, BitcoinCapnpError> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(Command::MonitorGetCoinbaseTx {
                monitor_id: self.monitor_id,
                reply: tx,
            })
            .map_err(|_| BitcoinCapnpError::ActorDisconnected)?;
        rx.await.map_err(|_| BitcoinCapnpError::ActorDisconnected)?
    }

    /// Get the witness commitment hash from the coinbase transaction.
    pub async fn get_coinbase_commitment(&self) -> Result<Vec<u8>, BitcoinCapnpError> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(Command::MonitorGetCoinbaseCommitment {
                monitor_id: self.monitor_id,
                reply: tx,
            })
            .map_err(|_| BitcoinCapnpError::ActorDisconnected)?;
        rx.await.map_err(|_| BitcoinCapnpError::ActorDisconnected)?
    }

    /// Get the index of the witness commitment output in the coinbase.
    pub async fn get_witness_commitment_index(&self) -> Result<i32, BitcoinCapnpError> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(Command::MonitorGetWitnessCommitmentIndex {
                monitor_id: self.monitor_id,
                reply: tx,
            })
            .map_err(|_| BitcoinCapnpError::ActorDisconnected)?;
        rx.await.map_err(|_| BitcoinCapnpError::ActorDisconnected)?
    }

    /// Get the coinbase transaction's merkle proof path.
    pub async fn get_coinbase_merkle_path(&self) -> Result<Vec<Vec<u8>>, BitcoinCapnpError> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(Command::MonitorGetCoinbaseMerklePath {
                monitor_id: self.monitor_id,
                reply: tx,
            })
            .map_err(|_| BitcoinCapnpError::ActorDisconnected)?;
        rx.await.map_err(|_| BitcoinCapnpError::ActorDisconnected)?
    }

    /// Submit a block solution to the node.
    pub async fn submit_solution(
        &self,
        version: u32,
        timestamp: u32,
        nonce: u32,
        coinbase: &[u8],
    ) -> Result<bool, BitcoinCapnpError> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(Command::MonitorSubmitSolution {
                monitor_id: self.monitor_id,
                version,
                timestamp,
                nonce,
                coinbase: coinbase.to_vec(),
                reply: tx,
            })
            .map_err(|_| BitcoinCapnpError::ActorDisconnected)?;
        rx.await.map_err(|_| BitcoinCapnpError::ActorDisconnected)?
    }

    /// Wait for the block template to change (new transactions, new fee thresholds, etc).
    /// Updates this monitor's internal template — old template is dropped.
    pub async fn wait_next(
        &self,
        options: Option<&BlockWaitOptions>,
    ) -> Result<(), BitcoinCapnpError> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(Command::MonitorWaitNext {
                monitor_id: self.monitor_id,
                options: options.cloned(),
                reply: tx,
            })
            .map_err(|_| BitcoinCapnpError::ActorDisconnected)?;
        rx.await.map_err(|_| BitcoinCapnpError::ActorDisconnected)?
    }
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
