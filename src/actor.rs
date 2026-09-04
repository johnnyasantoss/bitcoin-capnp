//! Internal actor thread that owns all Cap'n Proto state.
//!
//! The actor runs on a dedicated `std::thread` with a `tokio::runtime`
//! configured for the current thread and a `tokio::task::LocalSet`.
//! All `!Send` capnp clients live inside the actor; public handles
//! communicate via an `mpsc::UnboundedSender<Command>`.

use std::collections::HashMap;
use std::path::Path;
use std::thread;

use tokio::runtime::Builder;
use tokio::sync::{broadcast, mpsc, oneshot};
use tokio::task::LocalSet;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info};

use crate::client::IntoCapnp;
use crate::error::BitcoinCapnpError;
use crate::generated::echo_capnp::echo::Client as EchoIpcClient;
use crate::generated::mining_capnp::block_template::Client as BlockTemplateIpcClient;
use crate::generated::mining_capnp::mining::Client as MiningIpcClient;
use crate::generated::proxy_capnp::thread::Client as ThreadIpcClient;
use crate::mining::{BlockCreateOptions, BlockRef, BlockWaitOptions, TipChange};

/// Sender half exposed to the public clients.
pub(crate) type ActorTx = mpsc::UnboundedSender<Command>;

/// Commands sent from public clients to the actor thread.
pub(crate) enum Command {
    MiningIsTestChain {
        reply: oneshot::Sender<Result<bool, BitcoinCapnpError>>,
    },
    MiningIsInitialBlockDownload {
        reply: oneshot::Sender<Result<bool, BitcoinCapnpError>>,
    },
    MiningGetTip {
        reply: oneshot::Sender<Result<Option<BlockRef>, BitcoinCapnpError>>,
    },
    MiningWaitTipChanged {
        current_tip: Vec<u8>,
        timeout: f64,
        reply: oneshot::Sender<Result<BlockRef, BitcoinCapnpError>>,
    },
    MiningCreateNewBlock {
        options: BlockCreateOptions,
        reply: oneshot::Sender<Result<(), BitcoinCapnpError>>,
    },
    MiningStartMonitoring {
        coinbase_output_max_additional_size: u32,
        coinbase_output_max_additional_sigops: u16,
        reply: oneshot::Sender<Result<(u64, broadcast::Sender<TipChange>), BitcoinCapnpError>>,
    },

    EchoEcho {
        message: String,
        cancel: CancellationToken,
        reply: oneshot::Sender<Result<String, BitcoinCapnpError>>,
    },

    MonitorFetchBlockTemplate {
        monitor_id: u64,
        reply: oneshot::Sender<Result<(Vec<u8>, Vec<u8>), BitcoinCapnpError>>,
    },
    MonitorGetBlockHeader {
        monitor_id: u64,
        reply: oneshot::Sender<Result<Vec<u8>, BitcoinCapnpError>>,
    },
    MonitorGetBlock {
        monitor_id: u64,
        reply: oneshot::Sender<Result<Vec<u8>, BitcoinCapnpError>>,
    },
    MonitorGetTxFees {
        monitor_id: u64,
        reply: oneshot::Sender<Result<Vec<i64>, BitcoinCapnpError>>,
    },
    MonitorGetTxSigops {
        monitor_id: u64,
        reply: oneshot::Sender<Result<Vec<i64>, BitcoinCapnpError>>,
    },
    MonitorGetCoinbaseTx {
        monitor_id: u64,
        reply: oneshot::Sender<Result<Vec<u8>, BitcoinCapnpError>>,
    },
    MonitorGetCoinbaseCommitment {
        monitor_id: u64,
        reply: oneshot::Sender<Result<Vec<u8>, BitcoinCapnpError>>,
    },
    MonitorGetWitnessCommitmentIndex {
        monitor_id: u64,
        reply: oneshot::Sender<Result<i32, BitcoinCapnpError>>,
    },
    MonitorGetCoinbaseMerklePath {
        monitor_id: u64,
        reply: oneshot::Sender<Result<Vec<Vec<u8>>, BitcoinCapnpError>>,
    },
    MonitorSubmitSolution {
        monitor_id: u64,
        version: u32,
        timestamp: u32,
        nonce: u32,
        coinbase: Vec<u8>,
        reply: oneshot::Sender<Result<bool, BitcoinCapnpError>>,
    },
    MonitorWaitNext {
        monitor_id: u64,
        options: Option<BlockWaitOptions>,
        reply: oneshot::Sender<Result<(), BitcoinCapnpError>>,
    },
}

struct MonitorState {
    template_ipc_client: BlockTemplateIpcClient,
    tip_change_tx: broadcast::Sender<TipChange>,
    stop: CancellationToken,
}

struct Actor {
    mining_ipc_client: MiningIpcClient,
    thread_ipc_client: ThreadIpcClient,
    echo_ipc_client: EchoIpcClient,
    monitors: HashMap<u64, MonitorState>,
    next_monitor_id: u64,
    cmd_rx: mpsc::UnboundedReceiver<Command>,
}

/// Spawn the actor thread and return the command sender.
///
/// Connection setup happens inside the thread; the returned sender
/// can be used immediately — commands queue until the actor is ready.
pub(crate) fn spawn(path: &Path) -> ActorTx {
    let path = path.to_path_buf();
    let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();

    thread::Builder::new()
        .name("bitcoin-capnp-actor".into())
        .spawn(move || {
            let rt = Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("actor runtime");

            rt.block_on(async {
                let local = LocalSet::new();
                local
                    .run_until(async {
                        match init_actor(&path, cmd_rx).await {
                            Ok(actor) => actor.run().await,
                            Err(e) => {
                                error!("Actor initialization failed: {}", e);
                            }
                        }
                        debug!("Actor thread exiting");
                    })
                    .await;
            });
        })
        .expect("failed to spawn actor thread");

    cmd_tx
}

async fn init_actor(
    path: &Path,
    cmd_rx: mpsc::UnboundedReceiver<Command>,
) -> Result<Actor, BitcoinCapnpError> {
    // TODO(johnnyasantoss): Add cancel token to this fn
    let init_client = crate::libmp::connect(path).await?;
    let thread_client = crate::libmp::make_thread(&init_client).await?;

    let mining_prom = send_request(
        &thread_client,
        || init_client.make_mining_request(),
        |_| Ok(()),
    )?;
    let echo_prom = send_request(
        &thread_client,
        || init_client.make_echo_request(),
        |_| Ok(()),
    )?;
    let echo_response = await_response(echo_prom, None).await?;
    let echo_ipc_client: EchoIpcClient = echo_response.get()?.get_result()?;

    info!("Actor initialized: mining and echo clients created");

    Ok(Actor {
        mining_ipc_client,
        thread_ipc_client: thread_client,
        echo_ipc_client,
        monitors: HashMap::new(),
        next_monitor_id: 1,
        cmd_rx,
    })
}

/// Trait for Cap'n Proto params builders that carry a `context` field.
/// The `set_thread` default method sets the thread client, which every
/// IPC call to Bitcoin Core requires.
trait ContextBuilder {
    fn context_mut(&mut self)
    -> capnp::Result<crate::generated::proxy_capnp::context::Builder<'_>>;
    fn set_thread(&mut self, thread_client: ThreadIpcClient) -> capnp::Result<()> {
        self.context_mut()?.set_thread(thread_client);
        Ok(())
    }
}

macro_rules! impl_context_builder {
    ($($ty:ty),+ $(,)?) => {
        $(
            impl ContextBuilder for $ty {
                fn context_mut(&mut self) -> capnp::Result<crate::generated::proxy_capnp::context::Builder<'_>> {
                    self.reborrow().get_context()
                }
            }
        )+
    };
}

impl_context_builder!(
    crate::generated::mining_capnp::mining::is_test_chain_params::Builder<'_>,
    crate::generated::mining_capnp::mining::is_initial_block_download_params::Builder<'_>,
    crate::generated::mining_capnp::mining::get_tip_params::Builder<'_>,
    crate::generated::mining_capnp::mining::wait_tip_changed_params::Builder<'_>,
    crate::generated::mining_capnp::mining::create_new_block_params::Builder<'_>,
    crate::generated::mining_capnp::block_template::get_block_header_params::Builder<'_>,
    crate::generated::mining_capnp::block_template::get_block_params::Builder<'_>,
    crate::generated::mining_capnp::block_template::get_tx_fees_params::Builder<'_>,
    crate::generated::mining_capnp::block_template::get_tx_sigops_params::Builder<'_>,
    crate::generated::mining_capnp::block_template::get_coinbase_tx_params::Builder<'_>,
    crate::generated::mining_capnp::block_template::get_coinbase_merkle_path_params::Builder<'_>,
    crate::generated::mining_capnp::block_template::submit_solution_params::Builder<'_>,
    crate::generated::mining_capnp::block_template::wait_next_params::Builder<'_>,
    crate::generated::echo_capnp::echo::echo_params::Builder<'_>,
    crate::generated::echo_capnp::echo::destroy_params::Builder<'_>,
    crate::generated::init_capnp::init::make_mining_params::Builder<'_>,
    crate::generated::init_capnp::init::make_echo_params::Builder<'_>,
);

/// Build, fill thread context, and send a Cap'n Proto request synchronously.
/// Returns the `RemotePromise` immediately (no `.await`), allowing the
/// caller to pipeline chained requests on `prom.pipeline` before awaiting.
fn send_request<P, R>(
    thread_client: &ThreadIpcClient,
    mk: impl FnOnce() -> capnp::capability::Request<P, R>,
    fill: impl FnOnce(&mut P::Builder<'_>) -> capnp::Result<()>,
) -> Result<capnp::capability::RemotePromise<R>, BitcoinCapnpError>
where
    P: capnp::traits::Owned,
    for<'a> P::Builder<'a>: ContextBuilder,
    R: capnp::traits::Pipelined + capnp::traits::Owned + 'static + Unpin,
    <R as capnp::traits::Pipelined>::Pipeline: capnp::capability::FromTypelessPipeline,
{
    let mut req = mk();
    let mut builder = req.get();
    builder.set_thread(thread_client.clone())?;
    fill(&mut builder)?;
    drop(builder);
    Ok(req.send())
}
/// Await a `RemotePromise` with optional cancellation.
/// When `cancel.is_some()`, a `tokio::select!` races the promise against
/// the cancellation token; cancellation sends a Finish message to the
/// server (promise dropped), cancelling the server handler.
async fn await_response<R>(
    prom: capnp::capability::RemotePromise<R>,
    cancel: Option<CancellationToken>,
) -> Result<capnp::capability::Response<R>, BitcoinCapnpError>
where
    R: capnp::traits::Pipelined + capnp::traits::Owned + 'static,
{
    match cancel {
        Some(token) => tokio::select! {
            _ = token.cancelled() => Err(BitcoinCapnpError::Cancelled),
            resp = prom.promise => resp.map_err(BitcoinCapnpError::from),
        },
        None => prom.promise.await.map_err(BitcoinCapnpError::from),
    }
}

/// Result-extraction marker trait. Zero-sized marker types implement this
/// per-`Results::Owned` type so dispatch is resolved at compile time.
trait ExtractResult<R: capnp::traits::Owned> {
    type Output;
    fn extract(
        reader: <R as capnp::traits::Owned>::Reader<'_>,
    ) -> Result<Self::Output, BitcoinCapnpError>;
}

// --- Zero-sized marker types ---
struct Bool;
struct OptionBlockRef;
struct BlockRefMarker;
struct Bytes;
struct Int64List;
struct DataList;
struct Unit;
struct Text;

// Bool: is_test_chain, is_initial_block_download, submit_solution
impl ExtractResult<crate::generated::mining_capnp::mining::is_test_chain_results::Owned> for Bool {
    type Output = bool;
    fn extract(
        r: <crate::generated::mining_capnp::mining::is_test_chain_results::Owned as capnp::traits::Owned>::Reader<'_>,
    ) -> Result<bool, BitcoinCapnpError> {
        Ok(r.get_result())
    }
}
impl ExtractResult<crate::generated::mining_capnp::mining::is_initial_block_download_results::Owned>
    for Bool
{
    type Output = bool;
    fn extract(
        r: <crate::generated::mining_capnp::mining::is_initial_block_download_results::Owned as capnp::traits::Owned>::Reader<'_>,
    ) -> Result<bool, BitcoinCapnpError> {
        Ok(r.get_result())
    }
}
impl ExtractResult<crate::generated::mining_capnp::block_template::submit_solution_results::Owned>
    for Bool
{
    type Output = bool;
    fn extract(
        r: <crate::generated::mining_capnp::block_template::submit_solution_results::Owned as capnp::traits::Owned>::Reader<'_>,
    ) -> Result<bool, BitcoinCapnpError> {
        Ok(r.get_result())
    }
}

// OptionBlockRef: get_tip (may or may not have a result)
impl ExtractResult<crate::generated::mining_capnp::mining::get_tip_results::Owned>
    for OptionBlockRef
{
    type Output = Option<crate::mining::BlockRef>;
    fn extract(
        r: <crate::generated::mining_capnp::mining::get_tip_results::Owned as capnp::traits::Owned>::Reader<'_>,
    ) -> Result<Option<crate::mining::BlockRef>, BitcoinCapnpError> {
        if !r.get_has_result() {
            Ok(None)
        } else {
            r.get_result()
                .map(|br| Some(br.into()))
                .map_err(BitcoinCapnpError::from)
        }
    }
}

/// BlockRefMarker: wait_tip_changed
impl ExtractResult<crate::generated::mining_capnp::mining::wait_tip_changed_results::Owned>
    for BlockRefMarker
{
    type Output = crate::mining::BlockRef;
    fn extract(
        r: <crate::generated::mining_capnp::mining::wait_tip_changed_results::Owned as capnp::traits::Owned>::Reader<'_>,
    ) -> Result<crate::mining::BlockRef, BitcoinCapnpError> {
        r.get_result()
            .map(|br| br.into())
            .map_err(BitcoinCapnpError::from)
    }
}

// Bytes: get_block_header, get_block
impl ExtractResult<crate::generated::mining_capnp::block_template::get_block_header_results::Owned>
    for Bytes
{
    type Output = Vec<u8>;
    fn extract(
        r: <crate::generated::mining_capnp::block_template::get_block_header_results::Owned as capnp::traits::Owned>::Reader<'_>,
    ) -> Result<Vec<u8>, BitcoinCapnpError> {
        r.get_result()
            .map(|d| d.to_vec())
            .map_err(BitcoinCapnpError::from)
    }
}
impl ExtractResult<crate::generated::mining_capnp::block_template::get_block_results::Owned>
    for Bytes
{
    type Output = Vec<u8>;
    fn extract(
        r: <crate::generated::mining_capnp::block_template::get_block_results::Owned as capnp::traits::Owned>::Reader<'_>,
    ) -> Result<Vec<u8>, BitcoinCapnpError> {
        r.get_result()
            .map(|d| d.to_vec())
            .map_err(BitcoinCapnpError::from)
    }
}

// Int64List: get_tx_fees, get_tx_sigops
impl ExtractResult<crate::generated::mining_capnp::block_template::get_tx_fees_results::Owned>
    for Int64List
{
    type Output = Vec<i64>;
    fn extract(
        r: <crate::generated::mining_capnp::block_template::get_tx_fees_results::Owned as capnp::traits::Owned>::Reader<'_>,
    ) -> Result<Vec<i64>, BitcoinCapnpError> {
        let result = r.get_result()?;
        Ok((0..result.len()).map(|i| result.get(i)).collect())
    }
}
impl ExtractResult<crate::generated::mining_capnp::block_template::get_tx_sigops_results::Owned>
    for Int64List
{
    type Output = Vec<i64>;
    fn extract(
        r: <crate::generated::mining_capnp::block_template::get_tx_sigops_results::Owned as capnp::traits::Owned>::Reader<'_>,
    ) -> Result<Vec<i64>, BitcoinCapnpError> {
        let result = r.get_result()?;
        Ok((0..result.len()).map(|i| result.get(i)).collect())
    }
}

// DataList: get_coinbase_merkle_path
impl
    ExtractResult<
        crate::generated::mining_capnp::block_template::get_coinbase_merkle_path_results::Owned,
    > for DataList
{
    type Output = Vec<Vec<u8>>;
    fn extract(
        r: <crate::generated::mining_capnp::block_template::get_coinbase_merkle_path_results::Owned as capnp::traits::Owned>::Reader<'_>,
    ) -> Result<Vec<Vec<u8>>, BitcoinCapnpError> {
        let result = r.get_result()?;
        let mut path = Vec::with_capacity(result.len() as usize);
        for i in 0..result.len() {
            path.push(result.get(i)?.to_vec());
        }
        Ok(path)
    }
}

// Unit: destroy
impl ExtractResult<crate::generated::echo_capnp::echo::destroy_results::Owned> for Unit {
    type Output = ();
    fn extract(
        _r: <crate::generated::echo_capnp::echo::destroy_results::Owned as capnp::traits::Owned>::Reader<'_>,
    ) -> Result<(), BitcoinCapnpError> {
        Ok(())
    }
}

// Text: echo
impl ExtractResult<crate::generated::echo_capnp::echo::echo_results::Owned> for Text {
    type Output = String;
    fn extract(
        r: <crate::generated::echo_capnp::echo::echo_results::Owned as capnp::traits::Owned>::Reader<'_>,
    ) -> Result<String, BitcoinCapnpError> {
        r.get_result()?
            .to_str()
            .map(|s| s.to_owned())
            .map_err(|e| capnp::Error::failed(format!("UTF-8 error: {}", e)).into())
    }
}

/// Reply helper: send `result` to `reply`; warn if the caller already
/// dropped its receiver (previously silently swallowed).
fn deliver<T>(
    reply: oneshot::Sender<Result<T, BitcoinCapnpError>>,
    result: Result<T, BitcoinCapnpError>,
) {
    if reply.send(result).is_err() {
        tracing::warn!("actor reply dropped: caller disconnected before reply");
    }
}

impl Actor {
    async fn run(mut self) {
        loop {
            let res = match self.cmd_rx.recv().await {
                Some(cmd) => self.handle(cmd).await,
                None => break,
            };

            if let Err(e) = res {
                error!("Actor command failed: {}", e);
            }
        }
    }

    /// Build + fill + send + (optionally cancellably) await + extract +
    /// deliver the reply. Cap'n Proto errors reach the caller as
    /// `Err(BitcoinCapnpError)` instead of being lost to the actor loop.
    ///
    /// `marker` is a zero-sized type witness (`Bool`, `Text`, etc.) that
    /// selects which `ExtractResult` impl to use — avoids turbofish at
    /// every call site.
    async fn make_req<P, R, E>(
        &self,
        _marker: E,
        mk: impl FnOnce() -> capnp::capability::Request<P, R>,
        fill: impl FnOnce(&mut P::Builder<'_>) -> capnp::Result<()>,
        cancel: Option<CancellationToken>,
        reply: oneshot::Sender<Result<E::Output, BitcoinCapnpError>>,
    ) where
        P: capnp::traits::Owned,
        for<'a> P::Builder<'a>: ContextBuilder,
        R: capnp::traits::Pipelined + capnp::traits::Owned + 'static + Unpin,
        <R as capnp::traits::Pipelined>::Pipeline: capnp::capability::FromTypelessPipeline,
        E: ExtractResult<R>,
    {
        let result = match send_request(&self.thread_ipc_client, mk, fill) {
            Ok(prom) => match await_response(prom, cancel).await {
                Ok(resp) => match resp.get().map_err(BitcoinCapnpError::from) {
                    Ok(reader) => E::extract(reader),
                    Err(e) => Err(e),
                },
                Err(e) => Err(e),
            },
            Err(e) => Err(e),
        };
        deliver(reply, result);
    }

    async fn handle(&mut self, cmd: Command) -> Result<(), BitcoinCapnpError> {
        match cmd {
            Command::MiningIsTestChain { reply } => {
                self.make_req(
                    Bool,
                    || self.mining_ipc_client.is_test_chain_request(),
                    |_| Ok(()),
                    None,
                    reply,
                )
                .await;
            }
            Command::MiningIsInitialBlockDownload { reply } => {
                self.make_req(
                    Bool,
                    || self.mining_ipc_client.is_initial_block_download_request(),
                    |_| Ok(()),
                    None,
                    reply,
                )
                .await;
            }
            Command::MiningGetTip { reply } => {
                self.make_req(
                    OptionBlockRef,
                    || self.mining_ipc_client.get_tip_request(),
                    |_| Ok(()),
                    None,
                    reply,
                )
                .await;
            }
            Command::MiningWaitTipChanged {
                current_tip,
                timeout,
                reply,
            } => {
                self.make_req(
                    BlockRefMarker,
                    || self.mining_ipc_client.wait_tip_changed_request(),
                    |b| {
                        b.set_current_tip(&current_tip);
                        b.set_timeout(timeout);
                        Ok(())
                    },
                    None,
                    reply,
                )
                .await;
            }
            Command::MiningCreateNewBlock { options, reply } => {
                let result = match send_request(
                    &self.thread_ipc_client,
                    || self.mining_ipc_client.create_new_block_request(),
                    |b| {
                        options.apply(&mut b.reborrow().get_options()?);
                        Ok(())
                    },
                ) {
                    Ok(prom) => match await_response(prom, None).await {
                        Ok(resp) => resp
                            .get()
                            .map_err(BitcoinCapnpError::from)
                            .and_then(|r| r.get_result().map_err(BitcoinCapnpError::from)),
                        Err(e) => Err(e),
                    },
                    Err(e) => Err(e),
                };
                if let Ok(template) = &result {
                    for state in self.monitors.values_mut() {
                        state.template_ipc_client = template.clone();
                    }
                }
                deliver(reply, result.map(|_| ()));
            }
            Command::MiningStartMonitoring {
                coinbase_output_max_additional_size,
                coinbase_output_max_additional_sigops,
                reply,
            } => {
                let coinbase_weight = (coinbase_output_max_additional_size * 4) as u64;
                let block_reserved_weight = coinbase_weight.max(2000);
                let options = BlockCreateOptions {
                    use_mempool: true,
                    block_reserved_weight,
                    coinbase_output_max_additional_sigops: coinbase_output_max_additional_sigops
                        as u64,
                };

                let template_ipc_client = match send_request(
                    &self.thread_ipc_client,
                    || self.mining_ipc_client.create_new_block_request(),
                    |b| {
                        options.apply(&mut b.reborrow().get_options()?);
                        Ok(())
                    },
                ) {
                    Ok(prom) => match await_response(prom, None).await {
                        Ok(resp) => {
                            match resp
                                .get()
                                .map_err(BitcoinCapnpError::from)
                                .and_then(|r| r.get_result().map_err(BitcoinCapnpError::from))
                            {
                                Ok(t) => t,
                                Err(e) => {
                                    deliver(reply, Err(e));
                                    return Ok(());
                                }
                            }
                        }
                        Err(e) => {
                            deliver(reply, Err(e));
                            return Ok(());
                        }
                    },
                    Err(e) => {
                        deliver(reply, Err(e));
                        return Ok(());
                    }
                };

                let (tip_change_tx, _) = broadcast::channel(16);
                let stop = CancellationToken::new();

                let id = self.next_monitor_id;
                self.next_monitor_id += 1;

                let mining = self.mining_ipc_client.clone();
                let thread = self.thread_ipc_client.clone();
                let tip_tx = tip_change_tx.clone();
                let stop_clone = stop.clone();

                tokio::task::spawn_local(async move {
                    run_tip_monitor(mining, thread, tip_tx, stop_clone).await;
                });

                self.monitors.insert(
                    id,
                    MonitorState {
                        template_ipc_client,
                        tip_change_tx: tip_change_tx.clone(),
                        stop,
                    },
                );

                deliver(reply, Ok((id, tip_change_tx)));
            }

            Command::EchoEcho {
                message,
                cancel,
                reply,
            } => {
                self.make_req(
                    Text,
                    || self.echo_ipc_client.echo_request(),
                    |b| {
                        b.set_echo(&message);
                        Ok(())
                    },
                    Some(cancel),
                    reply,
                )
                .await;
            }

            Command::MonitorFetchBlockTemplate { monitor_id, reply } => {
                match self.monitors.get(&monitor_id) {
                    Some(_) => {
                        todo!()
                    }
                    None => {
                        deliver(reply, Err(BitcoinCapnpError::MonitorNotFound(monitor_id)));
                    }
                }
            }
            Command::MonitorGetBlockHeader { monitor_id, reply } => {
                let client = match self.monitors.get(&monitor_id) {
                    Some(state) => state.template_ipc_client.clone(),
                    None => {
                        deliver(reply, Err(BitcoinCapnpError::MonitorNotFound(monitor_id)));
                        return Ok(());
                    }
                };
                self.make_req(
                    Bytes,
                    || client.get_block_header_request(),
                    |_| Ok(()),
                    None,
                    reply,
                )
                .await;
            }
            Command::MonitorGetBlock { monitor_id, reply } => {
                let client = match self.monitors.get(&monitor_id) {
                    Some(state) => state.template_ipc_client.clone(),
                    None => {
                        deliver(reply, Err(BitcoinCapnpError::MonitorNotFound(monitor_id)));
                        return Ok(());
                    }
                };
                self.make_req(
                    Bytes,
                    || client.get_block_request(),
                    |_| Ok(()),
                    None,
                    reply,
                )
                .await;
            }
            Command::MonitorGetTxFees { monitor_id, reply } => {
                let client = match self.monitors.get(&monitor_id) {
                    Some(state) => state.template_ipc_client.clone(),
                    None => {
                        deliver(reply, Err(BitcoinCapnpError::MonitorNotFound(monitor_id)));
                        return Ok(());
                    }
                };
                self.make_req(
                    Int64List,
                    || client.get_tx_fees_request(),
                    |_| Ok(()),
                    None,
                    reply,
                )
                .await;
            }
            Command::MonitorGetTxSigops { monitor_id, reply } => {
                let client = match self.monitors.get(&monitor_id) {
                    Some(state) => state.template_ipc_client.clone(),
                    None => {
                        deliver(reply, Err(BitcoinCapnpError::MonitorNotFound(monitor_id)));
                        return Ok(());
                    }
                };
                self.make_req(
                    Int64List,
                    || client.get_tx_sigops_request(),
                    |_| Ok(()),
                    None,
                    reply,
                )
                .await;
            }
            Command::MonitorGetCoinbaseTx { monitor_id, reply } => {
                match self.monitors.get(&monitor_id) {
                    Some(_) => {
                        todo!()
                    }
                    None => {
                        deliver(reply, Err(BitcoinCapnpError::MonitorNotFound(monitor_id)));
                    }
                }
            }
            Command::MonitorGetCoinbaseCommitment { monitor_id, reply } => {
                match self.monitors.get(&monitor_id) {
                    Some(_) => {
                        todo!()
                    }
                    None => {
                        deliver(reply, Err(BitcoinCapnpError::MonitorNotFound(monitor_id)));
                    }
                }
            }
            Command::MonitorGetWitnessCommitmentIndex { monitor_id, reply } => {
                match self.monitors.get(&monitor_id) {
                    Some(state) => {
                        todo!()
                        // let mut req = state
                        //     .template_ipc_client
                        //     .get_witness_commitment_index_request();
                        // req.get()
                        //     .get_context()?
                        //     .set_thread(self.thread_ipc_client.clone());
                        // let response = req.send().promise.await?;
                        // let _ = reply.send(Ok(response.get()?.get_result()));
                    }
                    None => {
                        deliver(reply, Err(BitcoinCapnpError::MonitorNotFound(monitor_id)));
                    }
                }
            }
            Command::MonitorGetCoinbaseMerklePath { monitor_id, reply } => {
                let client = match self.monitors.get(&monitor_id) {
                    Some(state) => state.template_ipc_client.clone(),
                    None => {
                        deliver(reply, Err(BitcoinCapnpError::MonitorNotFound(monitor_id)));
                        return Ok(());
                    }
                };
                self.make_req(
                    DataList,
                    || client.get_coinbase_merkle_path_request(),
                    |_| Ok(()),
                    None,
                    reply,
                )
                .await;
            }
            Command::MonitorSubmitSolution {
                monitor_id,
                version,
                timestamp,
                nonce,
                coinbase,
                reply,
            } => {
                let client = match self.monitors.get(&monitor_id) {
                    Some(state) => state.template_ipc_client.clone(),
                    None => {
                        deliver(reply, Err(BitcoinCapnpError::MonitorNotFound(monitor_id)));
                        return Ok(());
                    }
                };
                self.make_req(
                    Bool,
                    || client.submit_solution_request(),
                    |b| {
                        b.set_version(version);
                        b.set_timestamp(timestamp);
                        b.set_nonce(nonce);
                        b.set_coinbase(&coinbase);
                        Ok(())
                    },
                    None,
                    reply,
                )
                .await;
            }
            Command::MonitorWaitNext {
                monitor_id,
                options,
                reply,
            } => {
                let client = match self.monitors.get(&monitor_id) {
                    Some(state) => state.template_ipc_client.clone(),
                    None => {
                        deliver(reply, Err(BitcoinCapnpError::MonitorNotFound(monitor_id)));
                        return Ok(());
                    }
                };
                let new_template = match send_request(
                    &self.thread_ipc_client,
                    || client.wait_next_request(),
                    |b| {
                        if let Some(o) = &options {
                            let mut opts = b.reborrow().init_options();
                            o.apply(&mut opts);
                        }
                        Ok(())
                    },
                ) {
                    Ok(prom) => match await_response(prom, None).await {
                        Ok(resp) => {
                            match resp
                                .get()
                                .map_err(BitcoinCapnpError::from)
                                .and_then(|r| r.get_result().map_err(BitcoinCapnpError::from))
                            {
                                Ok(t) => t,
                                Err(e) => {
                                    deliver(reply, Err(e));
                                    return Ok(());
                                }
                            }
                        }
                        Err(e) => {
                            deliver(reply, Err(e));
                            return Ok(());
                        }
                    },
                    Err(e) => {
                        deliver(reply, Err(e));
                        return Ok(());
                    }
                };
                match self.monitors.get_mut(&monitor_id) {
                    Some(state) => {
                        state.template_ipc_client = new_template;
                        deliver(reply, Ok(()));
                    }
                    None => {
                        deliver(reply, Err(BitcoinCapnpError::MonitorNotFound(monitor_id)));
                    }
                }
            }
        }
        Ok(())
    }
}

async fn run_tip_monitor(
    mining_ipc_client: MiningIpcClient,
    thread_ipc_client: ThreadIpcClient,
    tip_change_tx: broadcast::Sender<TipChange>,
    stop: CancellationToken,
) {
    // Initial get_tip — send_request (sync) + await_response (no cancel needed)
    let prom = match send_request(
        &thread_ipc_client,
        || mining_ipc_client.get_tip_request(),
        |_| Ok(()),
    ) {
        Ok(p) => p,
        Err(e) => {
            error!("Failed to build get_tip request: {}", e);
            stop.cancel();
            return;
        }
    };
    let response = match await_response(prom, None).await {
        Ok(r) => r,
        Err(e) => {
            error!("Failed to get initial tip: {}", e);
            stop.cancel();
            return;
        }
    };
    let current_tip = match response.get() {
        Ok(result) => match result.get_result() {
            Ok(tip) => tip,
            Err(e) => {
                error!("Failed to extract tip from response: {}", e);
                stop.cancel();
                return;
            }
        },
        Err(e) => {
            error!("Failed to get tip response: {}", e);
            stop.cancel();
            return;
        }
    };

    let mut current_tip_height = current_tip.get_height() as u64;
    let current_tip_hash = match current_tip.get_hash() {
        Ok(hash) => hash.to_vec(),
        Err(e) => {
            error!("Failed to get tip hash: {}", e);
            stop.cancel();
            return;
        }
    };

    info!(current_tip_height, "Got current tip");

    loop {
        let prom = match send_request(
            &thread_ipc_client,
            || mining_ipc_client.wait_tip_changed_request(),
            |b| {
                b.set_current_tip(&current_tip_hash);
                b.set_timeout(10_000f64);
                Ok(())
            },
        ) {
            Ok(p) => p,
            Err(e) => {
                error!("Failed to build wait_tip_changed request: {}", e);
                stop.cancel();
                return;
            }
        };

        // Cancellation of the inner await is handled by await_response.
        // When the cancel token fires, the promise is dropped (sending Finish
        // to the server), and Err(BitcoinCapnpError::Cancelled) is returned.
        let response = match await_response(prom, Some(stop.clone())).await {
            Ok(r) => r,
            Err(BitcoinCapnpError::Cancelled) => {
                info!("Local cancellation token fired, exiting tip change monitoring loop");
                break;
            }
            Err(e) => {
                tracing::error!("Failed to get response: {}", e);
                continue;
            }
        };

        let reader = match response.get() {
            Ok(r) => r,
            Err(e) => {
                error!("Failed to get response: {}", e);
                continue;
            }
        };

        let new_tip = match reader.get_result() {
            Ok(new_tip) => new_tip,
            Err(e) => {
                error!("Failed to get new tip: {}", e);
                continue;
            }
        };

        let new_height = new_tip.get_height() as u64;
        let new_hash = match new_tip.get_hash() {
            Ok(hash) => hash.to_vec(),
            Err(e) => {
                error!("Failed to get new tip hash: {}", e);
                continue;
            }
        };

        current_tip_height = new_height;
        info!(current_tip_height, "Tip changed!");

        let tip = TipChange {
            height: new_height,
            hash: new_hash.clone(),
        };
        let _ = tip_change_tx.send(tip);
    }
}
