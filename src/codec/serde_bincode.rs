//! Serde + bincode-based codec (compact, fast) using `Serialize`/`Deserialize`.

use super::Codec;
use std::io::{Read, Write};
use std::marker::PhantomData;

#[derive(Copy, Debug, Default)]
pub struct SerdeBincode<T>(PhantomData<T>);

impl<T> SerdeBincode<T> {
    pub fn new() -> Self {
        Self(PhantomData)
    }
}

impl<T> Clone for SerdeBincode<T> {
    fn clone(&self) -> Self {
        Self::new()
    }
}

#[derive(thiserror::Error, Debug)]
pub enum BincodeError {
    #[error("decode error: {0}")]
    Decode(#[from] bincode::error::DecodeError),
    #[error("encode error: {0}")]
    Encode(#[from] bincode::error::EncodeError),
}

impl<T> Codec for SerdeBincode<T>
where
    T: serde::Serialize + serde::de::DeserializeOwned + Send + Sync + 'static,
{
    type Value = T;
    type Error = BincodeError;

    fn encode_into(&self, value: &Self::Value, w: &mut impl Write) -> Result<(), Self::Error> {
        bincode::serde::encode_into_std_write(value, w, bincode::config::standard())?;
        Ok(())
    }

    fn decode_from(&self, r: &mut impl Read) -> Result<Self::Value, Self::Error> {
        let val = bincode::serde::decode_from_std_read(r, bincode::config::standard())?;
        Ok(val)
    }
}
