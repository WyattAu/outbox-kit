//! The deterministic headers codec shared by the durable stores.
//!
//! `headers` is a [`BTreeMap`] (deterministic iteration order), so a
//! length-prefixed byte encoding is stable: the same map encodes to
//! identical bytes on every writer, on every backend. No serde dependency
//! in the storage layer.

use std::collections::BTreeMap;

use crate::error::StoreError;

/// Encode headers: `u32 LE count`, then per entry `u32 LE key-length`,
/// key bytes, `u32 LE value-length`, value bytes.
pub(crate) fn encode_headers(headers: &BTreeMap<String, String>) -> Result<Vec<u8>, StoreError> {
    let count =
        u32::try_from(headers.len()).map_err(|_| StoreError::Backend("too many headers".into()))?;
    let mut out = Vec::new();
    out.extend_from_slice(&count.to_le_bytes());
    for (key, value) in headers {
        for blob in [key.as_bytes(), value.as_bytes()] {
            let len = u32::try_from(blob.len())
                .map_err(|_| StoreError::Backend("header entry too long".into()))?;
            out.extend_from_slice(&len.to_le_bytes());
            out.extend_from_slice(blob);
        }
    }
    Ok(out)
}

/// Read a `u32` length prefix, advancing the cursor.
fn read_u32(cursor: &mut &[u8]) -> Result<u32, StoreError> {
    let (bytes, rest) = cursor
        .split_at_checked(4)
        .ok_or_else(|| StoreError::Backend("corrupt headers blob".into()))?;
    let mut buffer = [0_u8; 4];
    buffer.copy_from_slice(bytes);
    *cursor = rest;
    Ok(u32::from_le_bytes(buffer))
}

/// Read one length-prefixed blob, advancing the cursor.
fn read_blob<'a>(cursor: &mut &'a [u8]) -> Result<&'a [u8], StoreError> {
    let len = read_u32(cursor)? as usize;
    let (bytes, rest) = cursor
        .split_at_checked(len)
        .ok_or_else(|| StoreError::Backend("corrupt headers blob".into()))?;
    *cursor = rest;
    Ok(bytes)
}

/// Decode the headers codec; keys and values must be UTF-8.
pub(crate) fn decode_headers(mut bytes: &[u8]) -> Result<BTreeMap<String, String>, StoreError> {
    let count = read_u32(&mut bytes)?;
    let mut headers = BTreeMap::new();
    for _ in 0..count {
        let key = read_blob(&mut bytes)?;
        let value = read_blob(&mut bytes)?;
        let key = String::from_utf8(key.to_vec())
            .map_err(|_| StoreError::Backend("corrupt header key".into()))?;
        let value = String::from_utf8(value.to_vec())
            .map_err(|_| StoreError::Backend("corrupt header value".into()))?;
        headers.insert(key, value);
    }
    Ok(headers)
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]
    use super::*;

    #[test]
    fn round_trip_preserves_the_map() {
        let mut headers = BTreeMap::new();
        headers.insert("trace".to_owned(), "t-1".to_owned());
        headers.insert("content-type".to_owned(), "application/json".to_owned());
        let decoded = decode_headers(&encode_headers(&headers).unwrap()).unwrap();
        assert_eq!(decoded, headers);
    }

    #[test]
    fn empty_map_encodes_to_a_count_prefix() {
        let bytes = encode_headers(&BTreeMap::new()).unwrap();
        assert_eq!(bytes, 0_u32.to_le_bytes());
        assert!(decode_headers(&bytes).unwrap().is_empty());
    }

    #[test]
    fn encoding_is_deterministic() {
        let mut headers = BTreeMap::new();
        headers.insert("b".to_owned(), "2".to_owned());
        headers.insert("a".to_owned(), "1".to_owned());
        assert_eq!(
            encode_headers(&headers).unwrap(),
            encode_headers(&headers).unwrap()
        );
    }

    #[test]
    fn codec_rejects_corruption() {
        // Truncated prefix.
        assert!(decode_headers(&[0, 0]).is_err());
        // Count claims 1 entry, buffer is empty.
        assert!(decode_headers(&1_u32.to_le_bytes()).is_err());
        // Key length overruns the buffer.
        let mut bytes = 1_u32.to_le_bytes().to_vec();
        bytes.extend_from_slice(&u32::MAX.to_le_bytes());
        assert!(decode_headers(&bytes).is_err());
        // Non-UTF-8 key.
        let mut bytes = 1_u32.to_le_bytes().to_vec();
        bytes.extend_from_slice(&4_u32.to_le_bytes());
        bytes.extend_from_slice(&[0xFF, 0xFE, 0xFD, 0xFC]);
        bytes.extend_from_slice(&1_u32.to_le_bytes());
        bytes.extend_from_slice(b"v");
        assert!(decode_headers(&bytes).is_err());
    }

    #[cfg(feature = "sqlite")]
    #[tokio::test]
    async fn round_trip_through_the_store_contract() {
        // The codec is what the durable stores persist; exercise it via a
        // real store to prove envelope fidelity end to end.
        use crate::event::OutboxEvent;
        use crate::OutboxStore;
        let store = crate::SqliteStore::open_in_memory().unwrap();
        let mut event = OutboxEvent::new("orders", b"payload").unwrap();
        event.headers.insert("k".to_owned(), "v".to_owned());
        store.append(&event).await.unwrap();
        let due = store.fetch_due(10, u64::MAX).await.unwrap();
        assert_eq!(due.first().unwrap().headers, event.headers);
    }
}
