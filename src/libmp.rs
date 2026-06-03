//! Internal plumbing for Bitcoin Core's `capnp/proxy.capnp` threading model.
//! Do not modify — upstream changes belong in Bitcoin Core repository.
//!
//! Provides connection setup and thread creation utilities.
//! `pub(crate)` — not part of public API.

use crate::error::BitcoinIpcError;
use capnp_rpc::{rpc_twoparty_capnp, twoparty, RpcSystem};
use std::path::Path;
use tokio::net::UnixStream;
use tokio_util::compat::*;

/// Connect to a Bitcoin Core node via UNIX socket, bootstrap the RPC system,
/// and return the bootstrap `Init` client.
///
/// Spawns the `RpcSystem` as a local task. The returned client
/// holds internal references that keep the RPC connection alive.
pub(crate) async fn connect(
    socket_path: &Path,
) -> Result<crate::gen::init_capnp::init::Client, BitcoinIpcError> {
    let stream = UnixStream::connect(socket_path).await?;
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

    Ok(bootstrap_client)
}

/// Create a worker thread via the Init interface's `construct` method.
///
/// Returns a `Thread` client handle that mining/echo clients use
/// to route method calls to the correct server thread.
pub(crate) async fn make_thread(
    init_client: &crate::gen::init_capnp::init::Client,
) -> Result<crate::gen::proxy_capnp::thread::Client, BitcoinIpcError> {
    use crate::gen::proxy_capnp::thread_map::Client as ThreadMapClient;

    let construct_response = init_client.construct_request().send().promise.await?;
    let thread_map: ThreadMapClient = construct_response.get()?.get_thread_map()?;
    let thread_request = thread_map.make_thread_request();
    let thread_response = thread_request.send().promise.await?;
    let thread_client = thread_response.get()?.get_result()?;

    tracing::info!("IPC execution thread client successfully created.");

    Ok(thread_client)
}
