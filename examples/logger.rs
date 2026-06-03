use std::path::Path;

use sv2_bitcoin_core::Sv2BitcoinCore;
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

    let core = Sv2BitcoinCore::new(path, cancel.clone(), 1, 1)
        .await
        .expect("Failed to connect to Bitcoin Core IPC");

    info!("Connected to Bitcoin Core IPC");

    let template_id = core
        .fetch_template_data()
        .await
        .expect("Failed to fetch block template");

    let templates = core.template_data.read().await;
    if let Some(template) = templates.get(&template_id) {
        info!("Template ID: {}", template.template_id);
        info!(
            "Block version: {}",
            template.block.header.version.to_consensus()
        );
        info!("Transaction count: {}", template.block.txdata.len());
        info!("nBits: {:#x}", template.get_nbits());
        info!("nTime: {}", template.get_ntime());
        info!("Coinbase tx version: {}", template.get_coinbase_tx_version());
        info!("Merkle path length: {}", template.get_merkle_path().len());
    }
}
