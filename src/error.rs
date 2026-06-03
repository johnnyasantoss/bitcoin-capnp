use bitcoin::consensus;

#[derive(Debug)]
pub enum IpcBitcoinCoreError {
    CapnpError(capnp::Error),
    IoError(std::io::Error),
    InvalidTemplateHeader(consensus::encode::Error),
    InvalidTemplateHeaderLength,
    InvalidBlockStructure,
    TemplateNotFound,
}

impl From<capnp::Error> for IpcBitcoinCoreError {
    fn from(error: capnp::Error) -> Self {
        IpcBitcoinCoreError::CapnpError(error)
    }
}

impl From<std::io::Error> for IpcBitcoinCoreError {
    fn from(error: std::io::Error) -> Self {
        IpcBitcoinCoreError::IoError(error)
    }
}

impl From<consensus::encode::Error> for IpcBitcoinCoreError {
    fn from(error: consensus::encode::Error) -> Self {
        IpcBitcoinCoreError::InvalidTemplateHeader(error)
    }
}
