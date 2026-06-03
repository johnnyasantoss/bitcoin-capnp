//! # bitcoin-ipc
//!
//! Rust Cap'n Proto IPC client for Bitcoin Core.
//!
//! Provides high-level, Rust-native wrappers around Bitcoin Core's
//! Cap'n Proto schemas. Consumers subscribe to tip change events and
//! fetch raw block/coinbase data.
//!
//! # Architecture
//!
//! Each public module mirrors one upstream Cap'n Proto schema file from
//! Bitcoin Core's repository. Internal plumbing lives in `libmp` (crate-private).
//!
//! ```no_run
//! use bitcoin_ipc::BitcoinIpc;
//!
//! async {
//!     let ipc = BitcoinIpc::new("/path/to/bitcoin.sock".as_ref()).await.unwrap();
//!     let _monitor = ipc.mining.start_monitoring(1, 1).await.unwrap();
//! };
//! ```

/// Error types returned by the Bitcoin Core IPC client.
pub mod error;

/// Auto-generated Cap'n Proto modules.
///
/// Low-level types generated directly from `*.capnp` schemas.
/// Prefer the high-level wrappers in [`mining`], [`echo`], and [`proxy`].
pub mod gen;

/// Re-exported so that auto-generated Cap'n Proto code can resolve
/// `crate::proxy_capnp::`, `crate::mining_capnp::`, etc.
pub use gen::*;

mod client;
pub mod echo;
mod libmp;
pub mod mining;
pub mod proxy;

use error::BitcoinIpcError;
pub use mining::{
    BlockCreateOptions, BlockRef, BlockValidationState, BlockWaitOptions, MonitorClient, TipChange,
};
use std::path::Path;

use tracing::info;

/// IPC client for Bitcoin Core's Cap'n Proto interface.
///
/// Connects to a Bitcoin Core node via UNIX socket. After construction,
/// call [`MiningClient::start_monitoring`] to begin tip change monitoring
/// and obtain a [`Monitor`](mining::Monitor) handle.
///
/// Requires a `tokio::task::LocalSet` runtime (Cap'n Proto futures are `!Send`).
/// See `examples/logger.rs`.
///
/// # Example
///
/// ```no_run
/// use bitcoin_ipc::BitcoinIpc;
///
/// async {
///     let ipc = BitcoinIpc::new("/path/to/bitcoin.sock".as_ref()).await.unwrap();
///     let monitor = ipc.mining.start_monitoring(1, 1).await.unwrap();
/// };
/// ```
#[derive(Clone)]
pub struct BitcoinIpc {
    pub mining: mining::MiningClient,
    pub echo: echo::EchoClient,
}

impl BitcoinIpc {
    /// Create a new IPC connection to a Bitcoin Core node.
    ///
    /// Bootstraps the Cap'n Proto connection and creates all IPC clients.
    /// Call [`MiningClient::start_monitoring`] on [`Self::mining`] to begin
    /// tip change monitoring and block template management.
    pub async fn new(node_socket_path: &Path) -> Result<Self, BitcoinIpcError> {
        info!(
            "Creating new Bitcoin IPC connection over UNIX socket: {}",
            node_socket_path.display()
        );

        let init_client = libmp::connect(node_socket_path).await?;
        let thread_client = libmp::make_thread(&init_client).await?;

        let mining = mining::MiningClient::new(&init_client, &thread_client).await?;

        let echo = echo::EchoClient::new(&init_client, &thread_client).await?;

        Ok(Self { mining, echo })
    }
}
