use std::{path::Path, time::Duration};

use bitcoin_capnp::{BitcoinCapnp, BitcoinCapnpError};
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
        cancel_clone.cancel();
    });

    let ipc = BitcoinCapnp::new(path);

    loop {
        let height = ipc.chain.get_height(cancel.clone()).await;

        match height {
            Ok(height) => {
                println!("Here's the height we got from ipc: {:?}", height);
            }
            Err(err) => match err {
                BitcoinCapnpError::Cancelled => return,
                _ => {
                    eprintln!("Error getting height: {}", err);
                    return;
                }
            },
        };

        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}
