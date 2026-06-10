use std::env::args;
use std::path::Path;
use std::process::exit;
use std::time::Duration;

use bitcoin_ipc::BitcoinIpc;
use tokio::signal::ctrl_c;
use tokio::spawn;
use tokio::time::MissedTickBehavior;
use tokio_util::sync::CancellationToken;
use tracing::info;
use tracing_subscriber::fmt::init;

#[tokio::main]
async fn main() {
    init();

    let args: Vec<String> = args().collect();
    if args.len() != 2 {
        eprintln!("Usage: {} <bitcoin_unix_socket_path>", args[0]);
        eprintln!();
        eprintln!("Example: {} /tmp/bitcoin-node.sock", args[0]);
        exit(1);
    }

    let path = Path::new(&args[1]);
    let cancel = CancellationToken::new();

    let cancel_clone = cancel.clone();
    spawn(async move {
        ctrl_c().await.expect("failed to listen for Ctrl+C signal");
        info!("Ctrl+C — shutting down");
        cancel.cancel();
    });

    echo_ping(path, cancel_clone).await;
}

async fn echo_ping(path: &Path, cancel: CancellationToken) {
    let ipc = BitcoinIpc::new(path);

    let mut sent = 0u64;
    let mut lost = 0u64;

    let mut interval = tokio::time::interval(Duration::from_secs(1));
    interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            _ = cancel.cancelled() => break,
            _ = interval.tick() => {
                sent += 1;
                let msg = format!("ping #{}", sent);
                match ipc.echo.echo(&msg, cancel.clone()).await {
                    Ok(reply) => info!("{} — reply: {}", msg, reply),
                    Err(e) => {
                        lost += 1;
                        tracing::error!("{} — error: {}", msg, e);
                    }
                }
            }
        }
    }

    info!("--- {} pings sent, {} lost", sent, lost);
    ipc.echo.destroy().await.ok();
}
