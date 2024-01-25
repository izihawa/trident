use percent_encoding::{AsciiSet, CONTROLS};
use tokio_util::bytes;

pub const FRAGMENT: &AsciiSet = &CONTROLS
    .add(b' ')
    .add(b'"')
    .add(b'<')
    .add(b'>')
    .add(b'`')
    .add(b'/');

#[inline]
pub fn key_to_bytes(key: &str) -> bytes::Bytes {
    bytes::Bytes::copy_from_slice(key.as_bytes())
}

#[inline]
pub fn bytes_to_key(b: &[u8]) -> &[u8] {
    b
}
