//! Payload codecs: encryption applied inside the application before payloads
//! become records.
//!
//! A codec sees workflow inputs and results, step results, signals, update
//! arguments and results, child inputs and heartbeat progress. Record
//! headers (IDs, names, positions), error messages and search attributes
//! stay readable so the engine can route and index them. The log,
//! checkpoints, the run store, replicas and a Fusee server hold only what
//! the codec produced.
//!
//! [`aes_gcm()`] encrypts with AES-256-GCM. Every payload carries the ID of the
//! key that encrypted it, so keys can rotate: the first key encrypts, every
//! listed key decrypts. Payloads without the codec's marker (written before
//! the codec was turned on) are passed through unchanged.

use aes_gcm::aead::{Aead, AeadCore, KeyInit, OsRng};
use aes_gcm::{Aes256Gcm, Key, Nonce};
use std::sync::Arc;

/// Transforms payloads on their way into and out of the journal.
pub trait Codec: Send + Sync + std::fmt::Debug {
    fn encode(&self, plain: &[u8]) -> Result<Vec<u8>, String>;
    fn decode(&self, data: &[u8]) -> Result<Vec<u8>, String>;
}

/// How an encrypted payload begins.
pub(crate) const MARK: &[u8; 4] = b"FEN1";

/// AES-256-GCM with key IDs.
pub struct AesGcmCodec {
    keys: Vec<(String, Aes256Gcm)>,
}

impl std::fmt::Debug for AesGcmCodec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let ids: Vec<&str> = self.keys.iter().map(|k| k.0.as_str()).collect();
        f.debug_struct("AesGcmCodec").field("keys", &ids).finish()
    }
}

/// An AES-256-GCM codec. `keys` are `(key ID, 32-byte key)`; the first one
/// encrypts new payloads.
pub fn aes_gcm<I: Into<String>>(keys: impl IntoIterator<Item = (I, [u8; 32])>) -> Arc<dyn Codec> {
    Arc::new(AesGcmCodec::new(keys))
}

impl AesGcmCodec {
    pub fn new<I: Into<String>>(keys: impl IntoIterator<Item = (I, [u8; 32])>) -> Self {
        let keys = keys
            .into_iter()
            .map(|(id, k)| (id.into(), Aes256Gcm::new(Key::<Aes256Gcm>::from_slice(&k))))
            .collect();
        Self { keys }
    }
}

impl Codec for AesGcmCodec {
    fn encode(&self, plain: &[u8]) -> Result<Vec<u8>, String> {
        let (id, cipher) = self
            .keys
            .first()
            .ok_or_else(|| "the codec has no keys".to_string())?;
        if id.len() > 255 {
            return Err("key IDs are at most 255 bytes".into());
        }
        let nonce = Aes256Gcm::generate_nonce(&mut OsRng);
        let sealed = cipher
            .encrypt(&nonce, plain)
            .map_err(|_| "encryption failed".to_string())?;
        let mut out = Vec::with_capacity(4 + 1 + id.len() + 12 + sealed.len());
        out.extend_from_slice(MARK);
        out.push(id.len() as u8);
        out.extend_from_slice(id.as_bytes());
        out.extend_from_slice(&nonce);
        out.extend_from_slice(&sealed);
        Ok(out)
    }

    fn decode(&self, data: &[u8]) -> Result<Vec<u8>, String> {
        let Some(rest) = data.strip_prefix(MARK) else {
            return Ok(data.to_vec());
        };
        let (&n, rest) = rest
            .split_first()
            .ok_or_else(|| "encrypted payload is truncated".to_string())?;
        let n = n as usize;
        if rest.len() < n + 12 {
            return Err("encrypted payload is truncated".into());
        }
        let (id, rest) = rest.split_at(n);
        let id = std::str::from_utf8(id).map_err(|_| "bad key ID".to_string())?;
        let (nonce, sealed) = rest.split_at(12);
        let cipher = self
            .keys
            .iter()
            .find(|k| k.0 == id)
            .map(|k| &k.1)
            .ok_or_else(|| {
                format!("payload was encrypted with key '{id}', which is not configured")
            })?;
        cipher
            .decrypt(Nonce::from_slice(nonce), sealed)
            .map_err(|_| format!("payload does not decrypt with key '{id}'"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip_and_rotation() {
        let old = aes_gcm([("k1", [7u8; 32])]);
        let sealed = old.encode(b"{\"card\":\"4242\"}").unwrap();
        assert!(sealed.starts_with(b"FEN1\x02k1"));
        assert!(!sealed.windows(4).any(|w| w == b"4242"));
        let rotated = aes_gcm([("k2", [9u8; 32]), ("k1", [7u8; 32])]);
        assert_eq!(rotated.decode(&sealed).unwrap(), b"{\"card\":\"4242\"}");
        assert!(rotated.encode(b"x").unwrap().starts_with(b"FEN1\x02k2"));
        assert!(aes_gcm([("k2", [9u8; 32])]).decode(&sealed).is_err());
        assert_eq!(old.decode(b"[1,2]").unwrap(), b"[1,2]");
        let mut tampered = sealed.clone();
        let last = tampered.len() - 1;
        tampered[last] ^= 1;
        assert!(old.decode(&tampered).is_err());
    }
}
