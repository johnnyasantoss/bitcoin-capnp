use std::path::Path;

use bitcoin_ipc::BitcoinCoreIpc;
use tokio_util::sync::CancellationToken;
use tracing::info;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let args: Vec<String> = std::env::args().collect();
    if args.len() != 2 {
        eprintln!("Usage: {} <bitcoin_core_unix_socket_path>", args[0]);
        std::process::exit(1);
    }

    let path = Path::new(&args[1]);
    let cancel = CancellationToken::new();
    let local_set = tokio::task::LocalSet::new();

    let cancel_clone = cancel.clone();
    tokio::spawn(async move {
        tokio::signal::ctrl_c().await.unwrap();
        info!("Ctrl+C received");
        cancel.cancel();
    });

    local_set
        .run_until(async move {
            let ipc = BitcoinCoreIpc::new(path, cancel_clone.clone(), 1, 1)
                .await
                .unwrap();

            let mut tip_rx = ipc.subscribe_tip_changes();
            tokio::task::spawn_local(async move {
                while let Ok(tip) = tip_rx.recv().await {
                    info!(
                        "Tip changed — height: {}, hash: {:?}",
                        tip.height, tip.hash
                    );
                }
            });

            ipc.run().await;
        })
        .await;
}
