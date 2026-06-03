//! Context mechanism for Cap'n Proto IPC requests.
//!
//! Provides a [`ContextBuilder`] trait implemented by all generated capnp
//! params builders that carry a `context :Proxy.Context` field. A single
//! [`IpcRequest`] fluent wrapper eliminates the repetitive
//! "get_context → set_thread → send → await" boilerplate.
//!
//! # Cancellation
//!
//! [`PublicClient`] exposes two cancellation mechanisms:
//!
//! - **Client-level** [`cancel()`](PublicClient::cancel) — signals the client to
//!   abort all background activity (e.g. tip monitoring). `MiningClient` forwards
//!   this to its inner [`CancellationToken`].
//!
//! - **Per-request** [`send_cancellable()`](IpcRequest::send_cancellable) — races
//!   the RPC against a [`CancellationToken`]. If the token fires first, the capnp
//!   `Promise` is dropped, which drops the attached `QuestionRef`, which sends a
//!   `Finish` message to the server. The server then cancels the handler by
//!   taking `answer.pipeline` and `answer.call_completion_promise`.
//!
//! ```text
//! send_cancellable()   tokio::select! wins
//!         │                    │
//!   Drop capnp Promise         │
//!         │                    │
//!   Drop QuestionRef           │
//!         │                    │
//!   Send Finish message  ──────┤
//!         │                    │
//!   Server handle_finish()     │
//!         │                    │
//!   Server cancels handler  <──┘
//! ```
//!
//! [`CancellationToken`]: tokio_util::sync::CancellationToken
//!
//! Do not modify — the 17 trait impls mirror the capnp schemas exactly.

use crate::error::BitcoinIpcError;
use crate::proxy_capnp::context::Builder;
use crate::proxy_capnp::thread::Client;
use capnp::capability::{FromTypelessPipeline, Response};
use capnp::traits::{Owned, Pipelined};
use tokio_util::sync::CancellationToken;
use tracing::debug;

/// Trait for high-level clients that make context-ful Cap'n Proto requests.
///
/// Implementors (e.g. `MiningClient`, `EchoClient`, `Monitor`) provide their
/// inner IPC client and thread handle. The default [`request`](PublicClient::request)
/// method creates a request, sets context/thread, and returns an [`IpcRequest`].
pub(crate) trait PublicClient {
    type Ipc;

    fn get_inner(&self) -> &Self::Ipc;
    fn get_thread(&self) -> &Client;

    fn request<P, R>(
        &self,
        f: impl FnOnce(&Self::Ipc) -> capnp::capability::Request<P, R>,
    ) -> Result<IpcRequest<P, R>, BitcoinIpcError>
    where
        P: Owned,
        R: Owned,
        for<'a> <P as Owned>::Builder<'a>: ContextBuilder,
    {
        let mut req = f(self.get_inner());
        ContextBuilder::set_thread(&mut req.get(), self.get_thread())?;
        Ok(IpcRequest { req })
    }
}

/// Converts a Rust-native options struct into its capnp Builder equivalent.
///
/// Used internally by [`MiningClient`] and [`Monitor`] methods that accept
/// options parameters (e.g. [`BlockCreateOptions`], [`BlockWaitOptions`]).
///
/// `B` is the target capnp builder type (e.g. `BlockCreateOptions::Builder<'_>`).
pub(crate) trait IntoCapnp<B> {
    fn apply(&self, builder: &mut B);
}

/// Implemented for generated capnp params builders that carry a
/// `context :Proxy.Context` field (the first pointer field in the params
/// struct). Provides automatic thread-context injection.
pub(crate) trait ContextBuilder {
    /// Return the context sub-builder of this params struct.
    fn context_mut(&mut self) -> capnp::Result<Builder<'_>>;

    /// Set the execution thread on this request's context.
    fn set_thread(&mut self, thread: &Client) -> capnp::Result<()> {
        self.context_mut()?.set_thread(thread.clone());
        Ok(())
    }
}

macro_rules! impl_context_builder {
    ($($ty:ty),+ $(,)?) => {
        $(
            impl ContextBuilder for $ty {
                fn context_mut(&mut self) -> capnp::Result<Builder<'_>> {
                    self.reborrow().get_context()
                }
            }
        )+
    };
}

impl_context_builder!(
    // — Mining interface (4) —
    crate::gen::mining_capnp::mining::is_test_chain_params::Builder<'_>,
    crate::gen::mining_capnp::mining::is_initial_block_download_params::Builder<'_>,
    crate::gen::mining_capnp::mining::get_tip_params::Builder<'_>,
    crate::gen::mining_capnp::mining::wait_tip_changed_params::Builder<'_>,
    // — BlockTemplate interface (11) —
    crate::gen::mining_capnp::block_template::destroy_params::Builder<'_>,
    crate::gen::mining_capnp::block_template::get_block_header_params::Builder<'_>,
    crate::gen::mining_capnp::block_template::get_block_params::Builder<'_>,
    crate::gen::mining_capnp::block_template::get_tx_fees_params::Builder<'_>,
    crate::gen::mining_capnp::block_template::get_tx_sigops_params::Builder<'_>,
    crate::gen::mining_capnp::block_template::get_coinbase_tx_params::Builder<'_>,
    crate::gen::mining_capnp::block_template::get_coinbase_commitment_params::Builder<'_>,
    crate::gen::mining_capnp::block_template::get_witness_commitment_index_params::Builder<'_>,
    crate::gen::mining_capnp::block_template::get_coinbase_merkle_path_params::Builder<'_>,
    crate::gen::mining_capnp::block_template::submit_solution_params::Builder<'_>,
    crate::gen::mining_capnp::block_template::wait_next_params::Builder<'_>,
    // — Echo interface (2) —
    crate::gen::echo_capnp::echo::echo_params::Builder<'_>,
    crate::gen::echo_capnp::echo::destroy_params::Builder<'_>,
);

/// A capnp request with context + thread already set.
///
/// Returned by [`crate::mining::MiningClient::request`] and
/// [`crate::echo::EchoClient::request`]. Provides a fluent chain:
/// `.set(|b| { ... }).send().await` followed by result extraction.
pub(crate) struct IpcRequest<P: Owned, R: Owned> {
    pub(crate) req: capnp::capability::Request<P, R>,
}

impl<P: Owned, R: Owned> IpcRequest<P, R> {
    /// Set additional request parameters via a closure on the params builder.
    pub fn set(mut self, f: impl FnOnce(&mut P::Builder<'_>)) -> Self {
        f(&mut self.req.get());
        self
    }

    /// Send the request, await the response, return the response.
    pub async fn send(self) -> Result<Response<R>, BitcoinIpcError>
    where
        R: Pipelined + Unpin + 'static,
        <R as Pipelined>::Pipeline: FromTypelessPipeline,
    {
        let response = self.req.send().promise.await?;
        Ok(response)
    }

    /// Send the request with an optional cancellation token.
    ///
    /// If `cancel` is provided and fires before the RPC completes, returns
    /// [`BitcoinIpcError::Cancelled`] and a `Finish` message is sent to the
    /// server to cancel the handler.
    ///
    /// # Mechanism
    ///
    /// Uses `tokio::select!` to race the capnp `Promise` against the token.
    /// When the token wins, the `Promise` is dropped:
    ///
    /// 1. Drop drops the attached [`QuestionRef`]
    /// 2. [`QuestionRef::drop`] sends a `Finish{releaseResultCaps: true}` message
    /// 3. Server [`handle_finish`] calls `answer.pipeline.take()` and
    ///    `answer.call_completion_promise.take()`, halting the handler
    /// 4. If the handler already completed, `ResultsDone` sends a `Canceled` return
    ///
    /// # One-shot
    ///
    /// `IpcRequest` is consumed by `send`. To race multiple RPCs against the
    /// same token, create a new `IpcRequest` per call (the token is `Clone`).
    pub async fn send_cancellable(
        self,
        cancel: CancellationToken,
    ) -> Result<Response<R>, BitcoinIpcError>
    where
        R: Pipelined + Unpin + 'static,
        <R as Pipelined>::Pipeline: FromTypelessPipeline,
    {
        tokio::select! {
            _ = cancel.cancelled() => {
                debug!("cancellation token triggered");
                Err(BitcoinIpcError::Cancelled)
            }
            response = self.req.send().promise => Ok(response?),
        }
    }
}
