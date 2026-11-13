//! The token exchange: an HMAC-SHA256 challenge and response in both
//! directions, in [`fusee_core::frame`] frames, before any protocol frame.
//!
//! ```text
//! initiator → HELLO      version u8 = 1, purpose u8, sender u8, nonce [32]
//! responder → CHALLENGE  version u8 = 1, nonce [32]
//! initiator → PROOF      HMAC(token, "fusee auth v1 initiator" ‖ T)
//! responder → ACCEPT     HMAC(token, "fusee auth v1 responder" ‖ T)
//!          or REFUSED    UTF-8 reason, then the responder closes
//! T = hello body ‖ challenge body ‖ binding length u8 ‖ binding
//! ```
//!
//! `binding` is 32 bytes of TLS exporter output (RFC 8446 section 7.5,
//! label `EXPORTER-fusee-auth-v1`) under TLS and empty otherwise, so a proof
//! made inside one TLS session is worthless in another. Both nonces are
//! fresh, so a recorded proof never verifies again. The responder answers
//! only after the initiator's proof verifies, so a side without the token
//! never sees a MAC computed with it. The initiator sends no protocol frame
//! before it has verified ACCEPT. MACs are compared in constant time.

use super::{Purpose, Token};
use crate::proto;
use fusee_core::frame;
use fusee_core::wire::{Reader, put_bytes, put_u64};
use ring::rand::SecureRandom;
use std::io::{self, Read, Write};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

pub(crate) const HELLO: u8 = 0xF0;
pub(crate) const CHALLENGE: u8 = 0xF1;
pub(crate) const PROOF: u8 = 0xF2;
pub(crate) const ACCEPT: u8 = 0xF3;
pub(crate) const REFUSED: u8 = 0xF4;

const VERSION: u8 = 1;
const NONCE: usize = 32;
const HELLO_LEN: usize = 3 + NONCE;
const CHALLENGE_LEN: usize = 1 + NONCE;
const MAX_BODY: usize = 1024;
const INITIATOR: &[u8] = b"fusee auth v1 initiator";
const RESPONDER: &[u8] = b"fusee auth v1 responder";
pub(super) const EXPORTER_LABEL: &[u8] = b"EXPORTER-fusee-auth-v1";

fn denied(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::PermissionDenied, message.into())
}

fn nonce() -> io::Result<[u8; NONCE]> {
    let mut n = [0u8; NONCE];
    ring::rand::SystemRandom::new()
        .fill(&mut n)
        .map_err(|_| io::Error::other("no system randomness for the authentication nonce"))?;
    Ok(n)
}

fn transcript(label: &[u8], hello: &[u8], challenge: &[u8], binding: &[u8]) -> Vec<u8> {
    let mut t = Vec::with_capacity(label.len() + hello.len() + challenge.len() + 1 + binding.len());
    t.extend_from_slice(label);
    t.extend_from_slice(hello);
    t.extend_from_slice(challenge);
    t.push(binding.len() as u8);
    t.extend_from_slice(binding);
    t
}

fn sign(token: &Token, label: &[u8], hello: &[u8], challenge: &[u8], binding: &[u8]) -> Vec<u8> {
    ring::hmac::sign(token.key(), &transcript(label, hello, challenge, binding))
        .as_ref()
        .to_vec()
}

fn verify(
    token: &Token,
    label: &[u8],
    hello: &[u8],
    challenge: &[u8],
    binding: &[u8],
    tag: &[u8],
) -> bool {
    ring::hmac::verify(
        token.key(),
        &transcript(label, hello, challenge, binding),
        tag,
    )
    .is_ok()
}

fn new_hello(purpose: Purpose, sender: u8) -> io::Result<Vec<u8>> {
    let mut hello = vec![VERSION, purpose as u8, sender];
    hello.extend_from_slice(&nonce()?);
    Ok(hello)
}

/// What the initiator does with each answer, shared by both drivers.
fn check_challenge(kind: u8, body: &[u8]) -> io::Result<()> {
    match kind {
        CHALLENGE if body.len() == CHALLENGE_LEN && body[0] == VERSION => Ok(()),
        CHALLENGE => Err(denied(
            "the listener uses an unsupported authentication version",
        )),
        _ => Err(answer_error(kind, body)),
    }
}

fn check_accept(
    kind: u8,
    body: &[u8],
    token: &Token,
    hello: &[u8],
    challenge: &[u8],
    binding: &[u8],
) -> io::Result<()> {
    if kind != ACCEPT {
        return Err(answer_error(kind, body));
    }
    if !verify(token, RESPONDER, hello, challenge, binding, body) {
        return Err(denied(
            "the listener could not prove it holds the token; refusing to talk to it",
        ));
    }
    Ok(())
}

fn answer_error(kind: u8, body: &[u8]) -> io::Error {
    let reason = match kind {
        REFUSED => String::from_utf8_lossy(body).into_owned(),
        // A server without a token answers the hello with a protocol error.
        proto::resp::ERR => {
            let mut r = Reader::new(body);
            let _ = r.u64();
            r.str().unwrap_or("refused").to_owned()
        }
        _ => format!("unexpected frame {kind} during authentication"),
    };
    denied(format!("authentication refused: {reason}"))
}

fn closed(e: io::Error) -> io::Error {
    if e.kind() == io::ErrorKind::UnexpectedEof {
        denied(
            "the other side closed the connection during authentication; check that both use the same token",
        )
    } else {
        e
    }
}

fn parse_header(header: &[u8; frame::HEADER]) -> io::Result<(u8, usize, u32)> {
    if &header[..4] != frame::MAGIC {
        // 0x15 and 0x16 start a TLS alert and a TLS handshake record.
        if matches!(header[0], 0x15 | 0x16) && header[1] == 3 {
            return Err(denied(
                "the other side uses TLS and this side does not: give both the same TLS settings (a worker or client turns TLS on with ca=PATH)",
            ));
        }
        return Err(denied("expected an authentication frame"));
    }
    let len = u32::from_le_bytes([header[5], header[6], header[7], header[8]]) as usize;
    if len > MAX_BODY {
        return Err(denied("oversized authentication frame"));
    }
    let crc = u32::from_le_bytes([header[9], header[10], header[11], header[12]]);
    Ok((header[4], len, crc))
}

fn check_crc(kind: u8, body: &[u8], crc: u32) -> io::Result<()> {
    if crc32c::crc32c_append(crc32c::crc32c(&[kind]), body) != crc {
        return Err(denied("authentication frame checksum mismatch"));
    }
    Ok(())
}

fn encode(kind: u8, body: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(frame::HEADER + body.len());
    frame::encode(kind, body, &mut out);
    out
}

// ---------------------------------------------------------------- blocking

fn read_blocking<S: Read>(s: &mut S) -> io::Result<(u8, Vec<u8>)> {
    let mut header = [0u8; frame::HEADER];
    s.read_exact(&mut header).map_err(closed)?;
    let (kind, len, crc) = parse_header(&header)?;
    let mut body = vec![0u8; len];
    s.read_exact(&mut body).map_err(closed)?;
    check_crc(kind, &body, crc)?;
    Ok((kind, body))
}

fn write_blocking<S: Write>(s: &mut S, kind: u8, body: &[u8]) -> io::Result<()> {
    s.write_all(&encode(kind, body))?;
    s.flush()
}

pub(super) fn initiate_blocking<S: Read + Write>(
    s: &mut S,
    token: &Token,
    purpose: Purpose,
    sender: u8,
    binding: &[u8],
) -> io::Result<()> {
    let hello = new_hello(purpose, sender)?;
    write_blocking(s, HELLO, &hello)?;
    let (kind, challenge) = read_blocking(s)?;
    check_challenge(kind, &challenge)?;
    let proof = sign(token, INITIATOR, &hello, &challenge, binding);
    write_blocking(s, PROOF, &proof)?;
    let (kind, accept) = read_blocking(s)?;
    check_accept(kind, &accept, token, &hello, &challenge, binding)
}

// ---------------------------------------------------------------- async

async fn read_async<S: AsyncRead + Unpin>(s: &mut S) -> io::Result<(u8, Vec<u8>)> {
    let mut header = [0u8; frame::HEADER];
    s.read_exact(&mut header).await.map_err(closed)?;
    let (kind, len, crc) = parse_header(&header)?;
    let mut body = vec![0u8; len];
    s.read_exact(&mut body).await.map_err(closed)?;
    check_crc(kind, &body, crc)?;
    Ok((kind, body))
}

async fn write_async<S: AsyncWrite + Unpin>(s: &mut S, kind: u8, body: &[u8]) -> io::Result<()> {
    s.write_all(&encode(kind, body)).await?;
    s.flush().await
}

pub(super) async fn initiate<S: AsyncRead + AsyncWrite + Unpin>(
    s: &mut S,
    token: &Token,
    purpose: Purpose,
    sender: u8,
    binding: &[u8],
) -> io::Result<()> {
    let hello = new_hello(purpose, sender)?;
    write_async(s, HELLO, &hello).await?;
    let (kind, challenge) = read_async(s).await?;
    check_challenge(kind, &challenge)?;
    let proof = sign(token, INITIATOR, &hello, &challenge, binding);
    write_async(s, PROOF, &proof).await?;
    let (kind, accept) = read_async(s).await?;
    check_accept(kind, &accept, token, &hello, &challenge, binding)
}

/// Sends `kind` with `body`, then reads and discards what the initiator
/// still sends for a moment: closing a socket with unread input resets the
/// connection, and the reset can destroy the answer written just before it.
async fn answer_and_linger<S: AsyncRead + AsyncWrite + Unpin>(s: &mut S, kind: u8, body: &[u8]) {
    if write_async(s, kind, body).await.is_ok() {
        super::stream::linger(s).await;
    }
}

async fn refuse<S: AsyncRead + AsyncWrite + Unpin>(s: &mut S, reason: &str) -> io::Error {
    answer_and_linger(s, REFUSED, reason.as_bytes()).await;
    denied(reason.to_owned())
}

/// Verifies the initiator and proves the token back. Returns the sender ID
/// the initiator declared in its (now authenticated) hello.
#[cfg(test)]
pub(super) async fn respond<S: AsyncRead + AsyncWrite + Unpin>(
    s: &mut S,
    token: &Token,
    purpose: Purpose,
    binding: &[u8],
) -> io::Result<u8> {
    respond_any(s, &[token], purpose, binding)
        .await
        .map(|(sender, _)| sender)
}

/// [`respond`] for a listener with several tokens: the proof is checked
/// against each in turn (each in constant time), and the one it verifies
/// with answers. Returns the sender and that token's index.
pub(super) async fn respond_any<S: AsyncRead + AsyncWrite + Unpin>(
    s: &mut S,
    tokens: &[&Token],
    purpose: Purpose,
    binding: &[u8],
) -> io::Result<(u8, usize)> {
    let (kind, hello) = read_async(s).await?;
    if kind != HELLO {
        if purpose == Purpose::Server {
            // A worker or client without a token sent its protocol hello:
            // tell it why, in a frame it already understands.
            let mut b = Vec::new();
            put_u64(&mut b, 0);
            put_bytes(
                &mut b,
                b"this server requires token authentication: add token-file=PATH to the address",
            );
            answer_and_linger(s, proto::resp::ERR, &b).await;
        }
        return Err(denied("the connecting side did not authenticate"));
    }
    if hello.len() != HELLO_LEN || hello[0] != VERSION {
        return Err(refuse(s, "unsupported authentication version").await);
    }
    if hello[1] != purpose as u8 {
        return Err(refuse(s, "this listener serves another protocol").await);
    }
    let mut challenge = vec![VERSION];
    challenge.extend_from_slice(&nonce()?);
    write_async(s, CHALLENGE, &challenge).await?;
    let (kind, proof) = read_async(s).await?;
    let found = (kind == PROOF)
        .then(|| {
            tokens
                .iter()
                .position(|t| verify(t, INITIATOR, &hello, &challenge, binding, &proof))
        })
        .flatten();
    let Some(i) = found else {
        return Err(refuse(s, "wrong token").await);
    };
    let accept = sign(tokens[i], RESPONDER, &hello, &challenge, binding);
    write_async(s, ACCEPT, &accept).await?;
    Ok((hello[2], i))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::pin::Pin;
    use std::sync::{Arc, Mutex};
    use std::task::{Context, Poll};
    use tokio::io::ReadBuf;

    /// Records every byte its stream reads and writes.
    struct Recorder<S> {
        inner: S,
        written: Arc<Mutex<Vec<u8>>>,
        read: Arc<Mutex<Vec<u8>>>,
    }

    impl<S: AsyncRead + Unpin> AsyncRead for Recorder<S> {
        fn poll_read(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            let before = buf.filled().len();
            let result = Pin::new(&mut self.inner).poll_read(cx, buf);
            let fresh = buf.filled()[before..].to_vec();
            self.read.lock().unwrap().extend_from_slice(&fresh);
            result
        }
    }

    impl<S: AsyncWrite + Unpin> AsyncWrite for Recorder<S> {
        fn poll_write(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<io::Result<usize>> {
            let result = Pin::new(&mut self.inner).poll_write(cx, buf);
            if let Poll::Ready(Ok(n)) = &result {
                self.written.lock().unwrap().extend_from_slice(&buf[..*n]);
            }
            result
        }
        fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Pin::new(&mut self.inner).poll_flush(cx)
        }
        fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Pin::new(&mut self.inner).poll_shutdown(cx)
        }
    }

    fn contains(haystack: &[u8], needle: &[u8]) -> bool {
        haystack.windows(needle.len()).any(|w| w == needle)
    }

    #[tokio::test]
    async fn the_secret_never_crosses_the_wire_and_a_recorded_proof_never_verifies_again() {
        let secret = *b"never-on-the-wire-0123456789abcd";
        let token = Token::new(&secret).unwrap();
        let written = Arc::new(Mutex::new(Vec::new()));
        let read = Arc::new(Mutex::new(Vec::new()));
        let (a, mut b) = tokio::io::duplex(4096);
        let mut recorded = Recorder {
            inner: a,
            written: written.clone(),
            read: read.clone(),
        };
        let initiator = {
            let token = token.clone();
            tokio::spawn(
                async move { initiate(&mut recorded, &token, Purpose::Replica, 1, &[]).await },
            )
        };
        assert_eq!(
            respond(&mut b, &token, Purpose::Replica, &[])
                .await
                .unwrap(),
            1
        );
        initiator.await.unwrap().unwrap();
        let written = written.lock().unwrap().clone();
        let read = read.lock().unwrap().clone();
        assert_eq!(written.len(), 2 * frame::HEADER + HELLO_LEN + 32);
        assert_eq!(read.len(), 2 * frame::HEADER + CHALLENGE_LEN + 32);
        assert!(!contains(&written, &secret) && !contains(&read, &secret));

        // Replaying the recorded hello and proof to a new exchange fails:
        // the responder's fresh nonce makes the old proof worthless.
        let (mut attacker, mut b) = tokio::io::duplex(4096);
        let responder = {
            let token = token.clone();
            tokio::spawn(async move { respond(&mut b, &token, Purpose::Replica, &[]).await })
        };
        attacker.write_all(&written).await.unwrap();
        let (kind, challenge) = read_async(&mut attacker).await.unwrap();
        assert_eq!(kind, CHALLENGE);
        assert_ne!(
            challenge[..],
            read[frame::HEADER..frame::HEADER + CHALLENGE_LEN]
        );
        let (kind, reason) = read_async(&mut attacker).await.unwrap();
        assert_eq!((kind, &reason[..]), (REFUSED, &b"wrong token"[..]));
        drop(attacker);
        let e = responder.await.unwrap().unwrap_err();
        assert_eq!(e.kind(), io::ErrorKind::PermissionDenied);

        // A listener without the token cannot pass for one: the initiator
        // refuses whatever it answers.
        let (mut a, mut b) = tokio::io::duplex(4096);
        let initiator = {
            let token = token.clone();
            tokio::spawn(async move { initiate(&mut a, &token, Purpose::Replica, 1, &[]).await })
        };
        let (kind, _) = read_async(&mut b).await.unwrap();
        assert_eq!(kind, HELLO);
        let mut challenge = vec![VERSION];
        challenge.extend_from_slice(&[9u8; NONCE]);
        write_async(&mut b, CHALLENGE, &challenge).await.unwrap();
        let (kind, _) = read_async(&mut b).await.unwrap();
        assert_eq!(kind, PROOF);
        write_async(&mut b, ACCEPT, &[0u8; 32]).await.unwrap();
        let e = initiator.await.unwrap().unwrap_err();
        assert!(e.to_string().contains("could not prove"), "{e}");
    }
}
