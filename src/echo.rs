//! Wraps types from Bitcoin Core's `capnp/echo.capnp`.
//! Do not modify — upstream schema changes belong in Bitcoin Core repository.
//!
//! High-level, Rust-native API for the Echo interface.
//! Primarily used for testing and debugging the IPC connection.

use tokio_util::sync::CancellationToken;

use crate::actor::{ActorTx, Command};
use crate::error::BitcoinCapnpError;
use tokio::sync::oneshot;

/// High-level client for Bitcoin Core's Echo interface.
///
/// Sends text messages to the Bitcoin Core node and receives them back.
/// Useful for verifying the IPC connection is healthy.
///
/// `Send` and `Clone`.
///
/// Created by `BitcoinCapnp::new()` during bootstrap. Do not construct directly.
#[derive(Clone)]
pub struct EchoClient {
    cmd_tx: ActorTx,
}

impl EchoClient {
    pub(crate) fn new(cmd_tx: ActorTx) -> Self {
        Self { cmd_tx }
    }

    /// Send a text message to the node and receive it back.
    ///
    /// Returns the echoed message on success.
    pub async fn echo(
        &self,
        message: &str,
        cancel: CancellationToken,
    ) -> Result<String, BitcoinCapnpError> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(Command::EchoEcho {
                message: message.to_string(),
                cancel,
                reply: tx,
            })
            .map_err(|_| BitcoinCapnpError::ActorDisconnected)?;
        rx.await.map_err(|_| BitcoinCapnpError::ActorDisconnected)?
    }
}
