use percent_encoding::{AsciiSet, CONTROLS};

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
