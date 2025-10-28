use std::io;

#[derive(thiserror::Error, Debug)]
pub enum StreamError {
    #[error("io: {0}")]
    Io(#[from] io::Error),

    /// Error returned by the user-provided codec implementation.
    #[error("codec: {0}")]
    Codec(#[source] Box<dyn std::error::Error + Send + Sync>),

    /// On-disk invariants violated (bad magic, crc mismatch, etc.)
    #[error("corrupt: {0}")]
    Corrupt(&'static str),

    /// Operation on a closed writer/reader
    #[error("closed")]
    Closed,

    /// Attempted to start a new transaction while another is active.
    #[error("transaction in progress")]
    TransactionInProgress,
}

impl From<&'static str> for StreamError {
    fn from(s: &'static str) -> Self {
        StreamError::Corrupt(s)
    }
}
