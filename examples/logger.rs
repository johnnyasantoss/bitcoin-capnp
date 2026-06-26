use std::path::Path;

use bitcoin_capnp::BitcoinCapnp;
use tokio_util::sync::CancellationToken;
use tracing::info;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let args: Vec<String> = std::env::args().collect();
    if args.len() != 2 {
        eprintln!("Usage: {} <bitcoin_unix_socket_path>", args[0]);
        std::process::exit(1);
    }

    let path = Path::new(&args[1]);
    let cancel = CancellationToken::new();

    let cancel_clone = cancel.clone();
    tokio::spawn(async move {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to listen for Ctrl+C signal");
        info!("Ctrl+C received");
        cancel.cancel();
    });

    let ipc = BitcoinCapnp::new(path);

    let monitor = ipc
        .mining
        .start_monitoring(1, 1)
        .await
        .expect("failed to start tip monitoring");
    let mut tip_rx = monitor.subscribe_tip_changes();

    tokio::spawn(async move {
        while let Ok(tip) = tip_rx.recv().await {
            info!("Tip changed — height: {}, hash: {:?}", tip.height, tip.hash);
        }
    });

    cancel_clone.cancelled().await;
}
