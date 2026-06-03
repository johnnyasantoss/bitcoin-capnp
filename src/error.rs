use bitcoin::consensus;

#[derive(Debug, thiserror::Error)]
pub enum BitcoinIpcError {
    #[error("Cap'n Proto error: {0}")]
    CapnpError(#[from] capnp::Error),
    #[error("I/O error: {0}")]
    IoError(#[from] std::io::Error),
    #[error("Invalid block header: {0}")]
    InvalidHeader(#[from] consensus::encode::Error),
    #[error("Invalid block header length")]
    InvalidHeaderLength,
    #[error("Invalid block structure")]
    InvalidBlockStructure,
}
