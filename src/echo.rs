//! Wraps types from Bitcoin Core's `capnp/echo.capnp`.
//! Do not modify — upstream schema changes belong in Bitcoin Core repository.
//!
//! High-level, Rust-native API for the Echo interface.
//! Primarily used for testing and debugging the IPC connection.

use tokio_util::sync::CancellationToken;

use crate::client::PublicClient;
use crate::error::BitcoinIpcError;
use crate::gen::echo_capnp::echo::Client as EchoIpcClient;
use crate::gen::proxy_capnp::thread::Client as ThreadIpcClient;

/// High-level client for Bitcoin Core's Echo interface.
///
/// Sends text messages to the Bitcoin Core node and receives them back.
/// Useful for verifying the IPC connection is healthy.
///
/// Created by `BitcoinIpc::new()` during bootstrap. Do not construct directly.
#[derive(Clone)]
pub struct EchoClient {
    echo_ipc_client: EchoIpcClient,
    thread_ipc_client: ThreadIpcClient,
}

impl PublicClient for EchoClient {
    type Ipc = EchoIpcClient;

    fn get_inner(&self) -> &Self::Ipc {
        &self.echo_ipc_client
    }

    fn get_thread(&self) -> &ThreadIpcClient {
        &self.thread_ipc_client
    }
}

impl EchoClient {
    /// Create an Echo client from the bootstrap Init interface.
    pub(crate) async fn new(
        init_client: &crate::gen::init_capnp::init::Client,
        thread_client: &ThreadIpcClient,
    ) -> Result<Self, BitcoinIpcError> {
        let mut echo_request = init_client.make_echo_request();
        echo_request
            .get()
            .get_context()?
            .set_thread(thread_client.clone());
        let echo_response = echo_request.send().promise.await?;
        let echo_ipc_client: EchoIpcClient = echo_response.get()?.get_result()?;

        tracing::info!("IPC echo client successfully created.");

        Ok(Self {
            echo_ipc_client,
            thread_ipc_client: thread_client.clone(),
        })
    }

    /// Send a text message to the node and receive it back.
    ///
    /// Returns the echoed message on success.
    pub async fn echo(
        &self,
        message: &str,
        cancel: CancellationToken,
    ) -> Result<String, BitcoinIpcError> {
        self.request(|c| c.echo_request())?
            .set(|b| b.set_echo(message))
            .send_cancellable(cancel)
            .await
            .and_then(|response| {
                let echoed = response
                    .get()?
                    .get_result()?
                    .to_str()
                    .map_err(|e| capnp::Error::failed(format!("UTF-8 error: {}", e)))?;
                Ok(echoed.to_owned())
            })
    }

    /// Explicitly destroy the echo interface on the server side.
    pub async fn destroy(&self) -> Result<(), BitcoinIpcError> {
        self.request(|c| c.destroy_request())?.send().await?;
        Ok(())
    }
}
