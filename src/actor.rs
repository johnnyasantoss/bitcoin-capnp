//! Internal actor thread that owns all Cap'n Proto state.
//!
//! The actor runs on a dedicated `std::thread` with a `tokio::runtime`
//! configured for the current thread and a `tokio::task::LocalSet`.
//! All `!Send` capnp clients live inside the actor; public handles
//! communicate via an `mpsc::UnboundedSender<Command>`.

use std::collections::HashMap;
use std::path::Path;

use tokio::sync::{broadcast, mpsc, oneshot};
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

use crate::client::IntoCapnp;
use crate::error::BitcoinIpcError;
use crate::gen::echo_capnp::echo::Client as EchoIpcClient;
use crate::gen::mining_capnp::block_template::Client as BlockTemplateIpcClient;
use crate::gen::mining_capnp::mining::Client as MiningIpcClient;
use crate::gen::proxy_capnp::thread::Client as ThreadIpcClient;
use crate::mining::{BlockCreateOptions, BlockRef, BlockWaitOptions, TipChange};

/// Sender half exposed to the public clients.
pub(crate) type ActorTx = mpsc::UnboundedSender<Command>;

/// Commands sent from public clients to the actor thread.
pub(crate) enum Command {
    MiningIsTestChain {
        reply: oneshot::Sender<Result<bool, BitcoinIpcError>>,
    },
    MiningIsInitialBlockDownload {
        reply: oneshot::Sender<Result<bool, BitcoinIpcError>>,
    },
    MiningGetTip {
        reply: oneshot::Sender<Result<Option<BlockRef>, BitcoinIpcError>>,
    },
    MiningWaitTipChanged {
        current_tip: Vec<u8>,
        timeout: f64,
        reply: oneshot::Sender<Result<BlockRef, BitcoinIpcError>>,
    },
    MiningCreateNewBlock {
        options: BlockCreateOptions,
        reply: oneshot::Sender<Result<(), BitcoinIpcError>>,
    },
    MiningStartMonitoring {
        coinbase_output_max_additional_size: u32,
        coinbase_output_max_additional_sigops: u16,
        reply: oneshot::Sender<Result<(u64, broadcast::Sender<TipChange>), BitcoinIpcError>>,
    },

    EchoEcho {
        message: String,
        cancel: CancellationToken,
        reply: oneshot::Sender<Result<String, BitcoinIpcError>>,
    },
    EchoDestroy {
        reply: oneshot::Sender<Result<(), BitcoinIpcError>>,
    },

    MonitorFetchBlockTemplate {
        monitor_id: u64,
        reply: oneshot::Sender<Result<(Vec<u8>, Vec<u8>), BitcoinIpcError>>,
    },
    MonitorGetBlockHeader {
        monitor_id: u64,
        reply: oneshot::Sender<Result<Vec<u8>, BitcoinIpcError>>,
    },
    MonitorGetBlock {
        monitor_id: u64,
        reply: oneshot::Sender<Result<Vec<u8>, BitcoinIpcError>>,
    },
    MonitorGetTxFees {
        monitor_id: u64,
        reply: oneshot::Sender<Result<Vec<i64>, BitcoinIpcError>>,
    },
    MonitorGetTxSigops {
        monitor_id: u64,
        reply: oneshot::Sender<Result<Vec<i64>, BitcoinIpcError>>,
    },
    MonitorGetCoinbaseTx {
        monitor_id: u64,
        reply: oneshot::Sender<Result<Vec<u8>, BitcoinIpcError>>,
    },
    MonitorGetCoinbaseCommitment {
        monitor_id: u64,
        reply: oneshot::Sender<Result<Vec<u8>, BitcoinIpcError>>,
    },
    MonitorGetWitnessCommitmentIndex {
        monitor_id: u64,
        reply: oneshot::Sender<Result<i32, BitcoinIpcError>>,
    },
    MonitorGetCoinbaseMerklePath {
        monitor_id: u64,
        reply: oneshot::Sender<Result<Vec<Vec<u8>>, BitcoinIpcError>>,
    },
    MonitorSubmitSolution {
        monitor_id: u64,
        version: u32,
        timestamp: u32,
        nonce: u32,
        coinbase: Vec<u8>,
        reply: oneshot::Sender<Result<bool, BitcoinIpcError>>,
    },
    MonitorWaitNext {
        monitor_id: u64,
        options: Option<BlockWaitOptions>,
        reply: oneshot::Sender<Result<(), BitcoinIpcError>>,
    },
}

#[allow(dead_code)]
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

    std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("actor runtime");
        rt.block_on(async {
            let local = tokio::task::LocalSet::new();
            local
                .run_until(async {
                    match init_actor(&path, cmd_rx).await {
                        Ok(actor) => actor.run().await,
                        Err(e) => {
                            error!("Actor initialization failed: {}", e);
                        }
                    }
                })
                .await;
        });
    });

    cmd_tx
}

async fn init_actor(
    path: &Path,
    cmd_rx: mpsc::UnboundedReceiver<Command>,
) -> Result<Actor, BitcoinIpcError> {
    let init_client = crate::libmp::connect(path).await?;
    let thread_client = crate::libmp::make_thread(&init_client).await?;

    let mut mining_request = init_client.make_mining_request();
    mining_request
        .get()
        .get_context()?
        .set_thread(thread_client.clone());
    let mining_response = mining_request.send().promise.await?;
    let mining_ipc_client: MiningIpcClient = mining_response.get()?.get_result()?;

    let mut echo_request = init_client.make_echo_request();
    echo_request
        .get()
        .get_context()?
        .set_thread(thread_client.clone());
    let echo_response = echo_request.send().promise.await?;
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

impl Actor {
    async fn run(mut self) {
        while let Some(cmd) = self.cmd_rx.recv().await {
            if let Err(e) = self.handle(cmd).await {
                error!("Actor command failed: {}", e);
            }
        }
    }

    async fn handle(&mut self, cmd: Command) -> Result<(), BitcoinIpcError> {
        match cmd {
            Command::MiningIsTestChain { reply } => {
                let mut req = self.mining_ipc_client.is_test_chain_request();
                req.get()
                    .get_context()?
                    .set_thread(self.thread_ipc_client.clone());
                let response = req.send().promise.await?;
                let _ = reply.send(Ok(response.get()?.get_result()));
            }
            Command::MiningIsInitialBlockDownload { reply } => {
                let mut req = self.mining_ipc_client.is_initial_block_download_request();
                req.get()
                    .get_context()?
                    .set_thread(self.thread_ipc_client.clone());
                let response = req.send().promise.await?;
                let _ = reply.send(Ok(response.get()?.get_result()));
            }
            Command::MiningGetTip { reply } => {
                let mut req = self.mining_ipc_client.get_tip_request();
                req.get()
                    .get_context()?
                    .set_thread(self.thread_ipc_client.clone());
                let response = req.send().promise.await?;
                let reader = response.get()?;
                let result = if !reader.get_has_result() {
                    None
                } else {
                    Some(reader.get_result()?.into())
                };
                let _ = reply.send(Ok(result));
            }
            Command::MiningWaitTipChanged {
                current_tip,
                timeout,
                reply,
            } => {
                let mut req = self.mining_ipc_client.wait_tip_changed_request();
                req.get()
                    .get_context()?
                    .set_thread(self.thread_ipc_client.clone());
                let mut builder = req.get();
                builder.set_current_tip(&current_tip);
                builder.set_timeout(timeout);
                let response = req.send().promise.await?;
                let _ = reply.send(Ok(response.get()?.get_result()?.into()));
            }
            Command::MiningCreateNewBlock { options, reply } => {
                let template = {
                    let mut req = self.mining_ipc_client.create_new_block_request();
                    options.apply(&mut req.get().get_options()?);
                    let response = req.send().promise.await?;
                    response.get()?.get_result()?
                };
                for state in self.monitors.values_mut() {
                    state.template_ipc_client = template.clone();
                }
                let _ = reply.send(Ok(()));
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

                let mut req = self.mining_ipc_client.create_new_block_request();
                options.apply(&mut req.get().get_options()?);
                let response = req.send().promise.await?;
                let template_ipc_client = response.get()?.get_result()?;

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

                let _ = reply.send(Ok((id, tip_change_tx)));
            }

            Command::EchoEcho {
                message,
                cancel,
                reply,
            } => {
                let mut req = self.echo_ipc_client.echo_request();
                req.get()
                    .get_context()?
                    .set_thread(self.thread_ipc_client.clone());
                req.get().set_echo(&message);

                let result = tokio::select! {
                    _ = cancel.cancelled() => Err(BitcoinIpcError::Cancelled),
                    response = req.send().promise => Ok(response?),
                };

                match result {
                    Ok(response) => {
                        let echoed = response
                            .get()?
                            .get_result()?
                            .to_str()
                            .map_err(|e| capnp::Error::failed(format!("UTF-8 error: {}", e)))?;
                        let _ = reply.send(Ok(echoed.to_owned()));
                    }
                    Err(e) => {
                        let _ = reply.send(Err(e));
                    }
                }
            }
            Command::EchoDestroy { reply } => {
                let mut req = self.echo_ipc_client.destroy_request();
                req.get()
                    .get_context()?
                    .set_thread(self.thread_ipc_client.clone());
                req.send().promise.await?;
                let _ = reply.send(Ok(()));
            }

            Command::MonitorFetchBlockTemplate { monitor_id, reply } => {
                match self.monitors.get(&monitor_id) {
                    Some(state) => {
                        let mut req = state.template_ipc_client.get_block_request();
                        req.get()
                            .get_context()?
                            .set_thread(self.thread_ipc_client.clone());
                        let block = req
                            .send()
                            .promise
                            .await
                            .and_then(|r| Ok(r.get()?.get_result()?.to_vec()))?;
                        let mut req = state.template_ipc_client.get_coinbase_tx_request();
                        req.get()
                            .get_context()?
                            .set_thread(self.thread_ipc_client.clone());
                        let coinbase = req
                            .send()
                            .promise
                            .await
                            .and_then(|r| Ok(r.get()?.get_result()?.to_vec()))?;
                        let _ = reply.send(Ok((block, coinbase)));
                    }
                    None => {
                        let _ = reply.send(Err(BitcoinIpcError::MonitorNotFound(monitor_id)));
                    }
                }
            }
            Command::MonitorGetBlockHeader { monitor_id, reply } => {
                match self.monitors.get(&monitor_id) {
                    Some(state) => {
                        let mut req = state.template_ipc_client.get_block_header_request();
                        req.get()
                            .get_context()?
                            .set_thread(self.thread_ipc_client.clone());
                        let data = req
                            .send()
                            .promise
                            .await
                            .and_then(|r| Ok(r.get()?.get_result()?.to_vec()))?;
                        let _ = reply.send(Ok(data));
                    }
                    None => {
                        let _ = reply.send(Err(BitcoinIpcError::MonitorNotFound(monitor_id)));
                    }
                }
            }
            Command::MonitorGetBlock { monitor_id, reply } => {
                match self.monitors.get(&monitor_id) {
                    Some(state) => {
                        let mut req = state.template_ipc_client.get_block_request();
                        req.get()
                            .get_context()?
                            .set_thread(self.thread_ipc_client.clone());
                        let data = req
                            .send()
                            .promise
                            .await
                            .and_then(|r| Ok(r.get()?.get_result()?.to_vec()))?;
                        let _ = reply.send(Ok(data));
                    }
                    None => {
                        let _ = reply.send(Err(BitcoinIpcError::MonitorNotFound(monitor_id)));
                    }
                }
            }
            Command::MonitorGetTxFees { monitor_id, reply } => {
                match self.monitors.get(&monitor_id) {
                    Some(state) => {
                        let mut req = state.template_ipc_client.get_tx_fees_request();
                        req.get()
                            .get_context()?
                            .set_thread(self.thread_ipc_client.clone());
                        let response = req.send().promise.await?;
                        let result = response.get()?.get_result()?;
                        let fees: Vec<i64> = (0..result.len()).map(|i| result.get(i)).collect();
                        let _ = reply.send(Ok(fees));
                    }
                    None => {
                        let _ = reply.send(Err(BitcoinIpcError::MonitorNotFound(monitor_id)));
                    }
                }
            }
            Command::MonitorGetTxSigops { monitor_id, reply } => {
                match self.monitors.get(&monitor_id) {
                    Some(state) => {
                        let mut req = state.template_ipc_client.get_tx_sigops_request();
                        req.get()
                            .get_context()?
                            .set_thread(self.thread_ipc_client.clone());
                        let response = req.send().promise.await?;
                        let result = response.get()?.get_result()?;
                        let sigops: Vec<i64> = (0..result.len()).map(|i| result.get(i)).collect();
                        let _ = reply.send(Ok(sigops));
                    }
                    None => {
                        let _ = reply.send(Err(BitcoinIpcError::MonitorNotFound(monitor_id)));
                    }
                }
            }
            Command::MonitorGetCoinbaseTx { monitor_id, reply } => {
                match self.monitors.get(&monitor_id) {
                    Some(state) => {
                        let mut req = state.template_ipc_client.get_coinbase_tx_request();
                        req.get()
                            .get_context()?
                            .set_thread(self.thread_ipc_client.clone());
                        let data = req
                            .send()
                            .promise
                            .await
                            .and_then(|r| Ok(r.get()?.get_result()?.to_vec()))?;
                        let _ = reply.send(Ok(data));
                    }
                    None => {
                        let _ = reply.send(Err(BitcoinIpcError::MonitorNotFound(monitor_id)));
                    }
                }
            }
            Command::MonitorGetCoinbaseCommitment { monitor_id, reply } => {
                match self.monitors.get(&monitor_id) {
                    Some(state) => {
                        let mut req = state.template_ipc_client.get_coinbase_commitment_request();
                        req.get()
                            .get_context()?
                            .set_thread(self.thread_ipc_client.clone());
                        let data = req
                            .send()
                            .promise
                            .await
                            .and_then(|r| Ok(r.get()?.get_result()?.to_vec()))?;
                        let _ = reply.send(Ok(data));
                    }
                    None => {
                        let _ = reply.send(Err(BitcoinIpcError::MonitorNotFound(monitor_id)));
                    }
                }
            }
            Command::MonitorGetWitnessCommitmentIndex { monitor_id, reply } => {
                match self.monitors.get(&monitor_id) {
                    Some(state) => {
                        let mut req = state
                            .template_ipc_client
                            .get_witness_commitment_index_request();
                        req.get()
                            .get_context()?
                            .set_thread(self.thread_ipc_client.clone());
                        let response = req.send().promise.await?;
                        let _ = reply.send(Ok(response.get()?.get_result()));
                    }
                    None => {
                        let _ = reply.send(Err(BitcoinIpcError::MonitorNotFound(monitor_id)));
                    }
                }
            }
            Command::MonitorGetCoinbaseMerklePath { monitor_id, reply } => {
                match self.monitors.get(&monitor_id) {
                    Some(state) => {
                        let mut req = state.template_ipc_client.get_coinbase_merkle_path_request();
                        req.get()
                            .get_context()?
                            .set_thread(self.thread_ipc_client.clone());
                        let response = req.send().promise.await?;
                        let result = response.get()?.get_result()?;
                        let mut path = Vec::new();
                        for i in 0..result.len() {
                            path.push(result.get(i)?.to_vec());
                        }
                        let _ = reply.send(Ok(path));
                    }
                    None => {
                        let _ = reply.send(Err(BitcoinIpcError::MonitorNotFound(monitor_id)));
                    }
                }
            }
            Command::MonitorSubmitSolution {
                monitor_id,
                version,
                timestamp,
                nonce,
                coinbase,
                reply,
            } => match self.monitors.get(&monitor_id) {
                Some(state) => {
                    let mut req = state.template_ipc_client.submit_solution_request();
                    req.get()
                        .get_context()?
                        .set_thread(self.thread_ipc_client.clone());
                    let mut builder = req.get();
                    builder.set_version(version);
                    builder.set_timestamp(timestamp);
                    builder.set_nonce(nonce);
                    builder.set_coinbase(&coinbase);
                    let response = req.send().promise.await?;
                    let _ = reply.send(Ok(response.get()?.get_result()));
                }
                None => {
                    let _ = reply.send(Err(BitcoinIpcError::MonitorNotFound(monitor_id)));
                }
            },
            Command::MonitorWaitNext {
                monitor_id,
                options,
                reply,
            } => {
                let template = match self.monitors.get(&monitor_id) {
                    Some(state) => {
                        let mut req = state.template_ipc_client.wait_next_request();
                        req.get()
                            .get_context()?
                            .set_thread(self.thread_ipc_client.clone());
                        if let Some(o) = options {
                            let mut opts = req.get().init_options();
                            o.apply(&mut opts);
                        }
                        let response = req.send().promise.await?;
                        Some(response.get()?.get_result()?)
                    }
                    None => None,
                };
                match template {
                    Some(t) => {
                        if let Some(state) = self.monitors.get_mut(&monitor_id) {
                            state.template_ipc_client = t;
                            let _ = reply.send(Ok(()));
                        } else {
                            let _ = reply.send(Err(BitcoinIpcError::MonitorNotFound(monitor_id)));
                        }
                    }
                    None => {
                        let _ = reply.send(Err(BitcoinIpcError::MonitorNotFound(monitor_id)));
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
    let mut req = mining_ipc_client.get_tip_request();
    if let Err(e) = req
        .get()
        .get_context()
        .map(|mut c| c.set_thread(thread_ipc_client.clone()))
    {
        error!("Failed to set thread context for get_tip: {}", e);
        stop.cancel();
        return;
    }
    let response = match req.send().promise.await {
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
        let token = stop.clone();
        let mut req = mining_ipc_client.wait_tip_changed_request();
        if let Err(e) = req
            .get()
            .get_context()
            .map(|mut c| c.set_thread(thread_ipc_client.clone()))
        {
            error!("Failed to set thread context for wait_tip_changed: {}", e);
            stop.cancel();
            return;
        }
        req.get().set_current_tip(&current_tip_hash);
        // time in ms
        req.get().set_timeout(10_000f64);

        let response = tokio::select! {
            _ = token.cancelled() => {
                info!("Local cancellation token fired, exiting tip change monitoring loop");
                break;
            }
            response = req.send().promise => match response {
                Ok(r) => r,
                Err(e) => {
                    tracing::error!("Failed to get response: {}", e);
                    continue;
                }
            },
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
