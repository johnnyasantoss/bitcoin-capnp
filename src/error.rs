use bitcoin::consensus;

/// Errors that can occur when communicating with Bitcoin Core via IPC.
#[derive(Debug, thiserror::Error)]
pub enum BitcoinIpcError {
    /// A Cap'n Proto serialization or RPC error.
    #[error("Cap'n Proto error: {0}")]
    CapnpError(#[from] capnp::Error),
    /// An I/O error on the UNIX socket connection.
    #[error("I/O error: {0}")]
    IoError(#[from] std::io::Error),
    /// The block header returned by Bitcoin Core failed to deserialize.
    #[error("Invalid block header: {0}")]
    InvalidHeader(#[from] consensus::encode::Error),
    /// The block header had an unexpected length.
    #[error("Invalid block header length")]
    InvalidHeaderLength,
    /// The block template returned by Bitcoin Core had an invalid structure.
    #[error("Invalid block structure")]
    InvalidBlockStructure,
    /// The operation was cancelled by the caller.
    #[error("Operation cancelled")]
    Cancelled,
}
