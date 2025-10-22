//! Pluggable record (de)serialization, streaming-friendly.

use std::error::Error;
use std::io::{Read, Write};

/// A simple codec interface the library uses to turn `T` into bytes and back.
/// Implement this yourself, or enable one of the provided serde codecs.
pub trait Codec: Send + Sync + 'static + Clone {
    type Value: Send + Sync + 'static;
    type Error: Error + Send + Sync + 'static;

    /// Encode `value` **into the provided writer**. (Record length-prefixing is handled elsewhere.)
    fn encode_into(&self, value: &Self::Value, w: &mut impl Write) -> Result<(), Self::Error>;

    /// Decode a single value **from the provided reader** (exactly one record's payload).
    fn decode_from(&self, r: &mut impl Read) -> Result<Self::Value, Self::Error>;
}

#[cfg(feature = "serde-bincode")]
pub mod serde_bincode;

#[cfg(feature = "serde-bincode")]
pub use serde_bincode::SerdeBincode;
