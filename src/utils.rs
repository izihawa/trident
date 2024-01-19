use percent_encoding::{AsciiSet, CONTROLS};
use tokio_util::bytes;

pub const FRAGMENT: &AsciiSet = &CONTROLS
    .add(b' ')
    .add(b'"')
    .add(b'<')
    .add(b'>')
    .add(b'`')
    .add(b'/');

pub fn key_to_bytes(key: &str) -> bytes::Bytes {
    let mut v = Vec::from(key.as_bytes());
    v.push(b'\0');
    bytes::Bytes::from(v)
}

pub fn bytes_to_key(b: &[u8]) -> &[u8] {
    b.strip_suffix(b"\0").unwrap_or(b)
}
