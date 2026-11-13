use super::*;
use rcgen::{
    BasicConstraints, CertificateParams, DnType, ExtendedKeyUsagePurpose, IsCa, Issuer, KeyPair,
    KeyUsagePurpose,
};
use std::io::{Read, Write};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// A throwaway CA and certificates it signs, generated per test.
pub(crate) struct Pki {
    ca: String,
    issuer: Issuer<'static, KeyPair>,
}

impl Pki {
    pub(crate) fn new() -> Self {
        let mut params = CertificateParams::new(Vec::<String>::new()).unwrap();
        params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        params
            .distinguished_name
            .push(DnType::CommonName, "fusee test CA");
        params.key_usages = vec![
            KeyUsagePurpose::KeyCertSign,
            KeyUsagePurpose::CrlSign,
            KeyUsagePurpose::DigitalSignature,
        ];
        let key = KeyPair::generate().unwrap();
        let ca = params.self_signed(&key).unwrap().pem();
        Self {
            ca,
            issuer: Issuer::new(params, key),
        }
    }

    /// `(certificate PEM, key PEM)` for these names, for servers and clients.
    pub(crate) fn leaf(&self, names: &[&str]) -> (String, String) {
        let mut params =
            CertificateParams::new(names.iter().map(|n| n.to_string()).collect::<Vec<_>>())
                .unwrap();
        params.distinguished_name.push(DnType::CommonName, names[0]);
        params.use_authority_key_identifier_extension = true;
        params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
        params.extended_key_usages = vec![
            ExtendedKeyUsagePurpose::ServerAuth,
            ExtendedKeyUsagePurpose::ClientAuth,
        ];
        let key = KeyPair::generate().unwrap();
        let cert = params.signed_by(&key, &self.issuer).unwrap();
        (cert.pem(), key.serialize_pem())
    }

    fn tls(&self, names: &[&str], with_ca: bool) -> Tls {
        let (cert, key) = self.leaf(names);
        Tls::from_pem(
            Some(cert.as_bytes()),
            Some(key.as_bytes()),
            with_ca.then_some(self.ca.as_bytes()),
        )
        .unwrap()
    }

    fn verifier(&self) -> Tls {
        Tls::from_pem(None, None, Some(self.ca.as_bytes())).unwrap()
    }
}

fn token(byte: u8) -> Token {
    Token::new(&[byte; 32]).unwrap()
}

#[test]
fn options_turn_tls_on_with_a_ca_and_refuse_anything_unknown() {
    assert!(Security::from_options("").unwrap().is_open());
    let e = Security::from_options("tokenfile=/x").unwrap_err();
    assert!(e.to_string().contains("unknown connection option"), "{e}");
    let e = Security::from_options("cert=/a&key=/b").unwrap_err();
    assert!(e.to_string().contains("ca=PATH"), "{e}");
    let e = Security::from_options("name=db1").unwrap_err();
    assert!(e.to_string().contains("needs ca="), "{e}");
    let e = Security::from_options("ca=").unwrap_err();
    assert!(e.to_string().contains("needs a value"), "{e}");
    assert!(Token::new(b"too short").is_err());
    assert_eq!(host_of("[::1]:7600"), "::1");
    assert_eq!(host_of("db1.internal:7600"), "db1.internal");
    let dir = std::env::temp_dir().join(format!("fusee-options-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let pki = Pki::new();
    std::fs::write(dir.join("ca.pem"), &pki.ca).unwrap();
    std::fs::write(dir.join("token"), "0123456789abcdef0123456789abcdef\n").unwrap();
    let s = Security::from_options(&format!(
        "ca={}&token-file={}&name=db1",
        dir.join("ca.pem").display(),
        dir.join("token").display()
    ))
    .unwrap();
    let tls = s.tls.as_ref().unwrap();
    assert_eq!(tls.server_name.as_deref(), Some("db1"));
    assert!(s.token.is_some() && !tls.has_identity());
    assert_eq!(format!("{:?}", s.token.unwrap()), "Token(redacted)");
    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test]
async fn the_token_exchange_admits_only_the_same_token() {
    for (theirs, ok) in [(1u8, true), (2u8, false)] {
        let (mut a, mut b) = tokio::io::duplex(4096);
        let initiator = tokio::spawn(async move {
            auth::initiate(&mut a, &token(theirs), Purpose::Replica, 2, b"binding").await
        });
        let responded = auth::respond(&mut b, &token(1), Purpose::Replica, b"binding").await;
        let initiated = initiator.await.unwrap();
        if ok {
            assert_eq!(responded.unwrap(), 2);
            initiated.unwrap();
        } else {
            let e = responded.unwrap_err();
            assert_eq!(e.kind(), io::ErrorKind::PermissionDenied);
            let e = initiated.unwrap_err();
            assert!(e.to_string().contains("wrong token"), "{e}");
        }
    }
    // A proof made for another session (binding) or protocol never verifies.
    let (mut a, mut b) = tokio::io::duplex(4096);
    let initiator = tokio::spawn(async move {
        auth::initiate(&mut a, &token(1), Purpose::Replica, 2, b"session A").await
    });
    assert!(
        auth::respond(&mut b, &token(1), Purpose::Replica, b"session B")
            .await
            .is_err()
    );
    assert!(initiator.await.unwrap().is_err());
    let (mut a, mut b) = tokio::io::duplex(4096);
    let initiator = tokio::spawn(async move {
        auth::initiate(&mut a, &token(1), Purpose::Server, NO_SENDER, &[]).await
    });
    assert!(
        auth::respond(&mut b, &token(1), Purpose::Replica, &[])
            .await
            .is_err()
    );
    let e = initiator.await.unwrap().unwrap_err();
    assert!(e.to_string().contains("another protocol"), "{e}");
}

/// Accepts one TLS connection with `acceptor`.
async fn accept_one(acceptor: Acceptor) -> (u16, tokio::task::JoinHandle<io::Result<TlsStream>>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let task = tokio::spawn(async move {
        let (sock, _) = listener.accept().await?;
        match acceptor.accept_tcp(sock).await? {
            (Secured::Tls(s), _) => Ok(s),
            (Secured::Plain(_), _) => Err(io::Error::other("expected TLS")),
        }
    });
    (port, task)
}

fn pattern(i: usize) -> u8 {
    (i.wrapping_mul(31) ^ (i >> 7)) as u8
}

/// A blocking worker-style connection (reader thread, writer thread) against
/// the server's asynchronous TLS stream, both sides writing several MiB at
/// once, with key updates requested from both sides mid-stream.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn split_tls_streams_carry_bulk_traffic_both_ways_through_key_updates() {
    let pki = Pki::new();
    let server = Security {
        tls: Some(pki.tls(&["127.0.0.1"], false)),
        token: Some(token(7)),
    };
    let client = Security {
        tls: Some(pki.verifier()),
        token: Some(token(7)),
    };
    let (port, accepted) = accept_one(Acceptor::new(&server, Purpose::Server).unwrap()).await;
    const TOTAL: usize = 24 << 20;
    let connector = Connector::new(&client, Purpose::Server, NO_SENDER).unwrap();
    let client = tokio::task::spawn_blocking(move || {
        let tcp = std::net::TcpStream::connect(("127.0.0.1", port)).unwrap();
        let conn = connector
            .secure(Raw::Tcp(tcp), &format!("127.0.0.1:{port}"))
            .unwrap();
        let shared = conn.shared_tls().unwrap();
        let (mut reader, mut writer, _raw) = conn.split();
        let sending = std::thread::spawn(move || {
            let data: Vec<u8> = (0..TOTAL).map(pattern).collect();
            let mut at = 0;
            let mut size = 1usize;
            while at < TOTAL {
                let n = size.min(TOTAL - at);
                writer.write_all(&data[at..at + n]).unwrap();
                at += n;
                size = (size * 7 + 13) % (300 << 10) + 1;
                if at > TOTAL / 3 && at - n <= TOTAL / 3 {
                    // Ask the server to update its keys too.
                    shared.lock().unwrap().refresh_traffic_keys().unwrap();
                }
            }
        });
        let mut got = vec![0u8; TOTAL];
        reader.read_exact(&mut got).unwrap();
        sending.join().unwrap();
        got
    });
    let stream = accepted.await.unwrap().unwrap();
    let (mut read, write) = stream.into_split();
    let echo = tokio::spawn(async move {
        let mut write = write;
        let mut buf = vec![0u8; 50_000];
        let mut seen = 0usize;
        let mut updated = false;
        while seen < TOTAL {
            let n = read.read(&mut buf).await.unwrap();
            assert!(n > 0, "the client closed early");
            for (k, b) in buf[..n].iter().enumerate() {
                assert_eq!(*b, pattern(seen + k), "byte {} differs", seen + k);
            }
            seen += n;
            if !updated && seen > TOTAL / 2 {
                updated = true;
                write.refresh_traffic_keys();
            }
            write.write_all(&buf[..n]).await.unwrap();
        }
        write.flush().await.unwrap();
        (read, write)
    });
    let got = client.await.unwrap();
    assert!(got.iter().enumerate().all(|(i, b)| *b == pattern(i)));
    drop(echo.await.unwrap());
}

/// Records `try_write` accepted reach the peer although nobody writes or
/// waits for writability again; dropping the halves then closes the socket.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn accepted_records_drain_without_another_write_and_close_with_the_halves() {
    let pki = Pki::new();
    let server = Security {
        tls: Some(pki.tls(&["localhost"], false)),
        token: None,
    };
    let client = Security {
        tls: Some(pki.verifier()),
        token: None,
    };
    let (port, accepted) = accept_one(Acceptor::new(&server, Purpose::Server).unwrap()).await;
    let sock = tokio::net::TcpStream::connect(("127.0.0.1", port))
        .await
        .unwrap();
    let connector = Connector::new(&client, Purpose::Server, NO_SENDER).unwrap();
    let Secured::Tls(stream) = connector
        .secure_tcp(sock, &format!("localhost:{port}"))
        .await
        .unwrap()
    else {
        panic!("expected TLS");
    };
    let mut peer = accepted.await.unwrap().unwrap();
    let (read, write) = stream.into_split();
    let chunk = vec![0x5a_u8; 64 << 10];
    let mut accepted_bytes = 0usize;
    loop {
        match TryWrite::try_write(&write, &chunk) {
            Ok(n) => accepted_bytes += n,
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => break,
            Err(e) => panic!("{e}"),
        }
    }
    assert!(accepted_bytes >= stream_backlog(), "{accepted_bytes}");
    let mut buf = vec![0u8; 1 << 16];
    let mut got = 0usize;
    while got < accepted_bytes {
        tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        let n = peer.read(&mut buf).await.unwrap();
        assert!(n > 0);
        assert!(buf[..n].iter().all(|b| *b == 0x5a));
        got += n;
    }
    assert_eq!(got, accepted_bytes);
    drop((read, write));
    let n = tokio::time::timeout(std::time::Duration::from_secs(5), peer.read(&mut buf))
        .await
        .expect("the socket closes with its halves")
        .unwrap();
    assert_eq!(n, 0);
}

fn stream_backlog() -> usize {
    256 << 10
}

use crate::server::TryWrite;
