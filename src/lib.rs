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
//! use bitcoin_capnp::BitcoinCapnp;
//!
//! # #[tokio::main]
//! # async fn main() {
//! let ipc = BitcoinCapnp::new("/path/to/bitcoin.sock".as_ref());
//! let _monitor = ipc.mining.start_monitoring(1, 1).await.unwrap();
//! # }
//! ```

/// Error types returned by the Bitcoin Core IPC client.
pub mod error;

/// Auto-generated Cap'n Proto modules.
///
/// Low-level types generated directly from `*.capnp` schemas.
/// Prefer the high-level wrappers in [`mining`], [`echo`], and [`proxy`].
pub mod generated;

/// Re-exported so that auto-generated Cap'n Proto code can resolve
/// `crate::proxy_capnp::`, `crate::mining_capnp::`, etc.
pub use generated::*;

mod actor;
pub mod chain;
mod client;
pub mod echo;
mod libmp;
pub mod mining;
pub mod proxy;

pub use error::BitcoinCapnpError;
pub use mining::{
    BlockCreateOptions, BlockRef, BlockValidationState, BlockWaitOptions, MonitorClient, TipChange,
};
use std::path::Path;

use tracing::info;

/// IPC client for Bitcoin Core's Cap'n Proto interface.
///
/// Connects to a Bitcoin Core node via UNIX socket. After construction,
/// call [`MiningClient::start_monitoring`] to begin tip change monitoring
/// and obtain a [`MonitorClient`] handle.
///
/// `Send` and `Clone` — safe to move between async tasks. No `LocalSet`
/// required.
///
/// # Example
///
/// ```no_run
/// use bitcoin_capnp::BitcoinCapnp;
///
/// # #[tokio::main]
/// # async fn main() {
/// let ipc = BitcoinCapnp::new("/path/to/bitcoin.sock".as_ref());
/// let monitor = ipc.mining.start_monitoring(1, 1).await.unwrap();
/// # }
/// ```
#[derive(Clone)]
pub struct BitcoinCapnp {
    pub mining: mining::MiningClient,
    pub chain: chain::ChainClient,
    pub echo: echo::EchoClient,
}

impl BitcoinCapnp {
    /// Create a new Capnp connection to a Bitcoin Core node.
    ///
    /// Spawns an internal actor thread that owns all Cap'n Proto state.
    /// The connection is established asynchronously inside the thread;
    /// method calls block until the actor is ready.
    pub fn new(node_socket_path: &Path) -> Self {
        info!(
            "Creating new Bitcoin IPC connection over UNIX socket: {}",
            node_socket_path.display()
        );

        let cmd_tx = actor::spawn(node_socket_path);

        let mining = mining::MiningClient::new(cmd_tx.clone());
        let echo = echo::EchoClient::new(cmd_tx.clone());
        let chain = chain::ChainClient::new(cmd_tx);

        Self {
            mining,
            echo,
            chain,
        }
    }
}
