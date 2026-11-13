//! TLS and token authentication on real sockets: workers and clients against
//! a server, and replicas against each other and against an attacker.
use fusee::{Ctx, Engine, Error, FlushMethod, LocalOptions};
use fusee_core::frame;
use fusee_core::vsr::{Msg, Timing, encode_msg};
use fusee_server::replicated::{ReplicaConfig, ReplicaNode};
use fusee_server::security::{Scope, Security, Tls, Token};
use fusee_server::{Client, Server, ServerConfig};
use rcgen::{
    BasicConstraints, CertificateParams, DnType, ExtendedKeyUsagePurpose, IsCa, Issuer, KeyPair,
    KeyUsagePurpose,
};
use serde_json::json;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

fn tmp(name: &str) -> PathBuf {
    let d = std::env::temp_dir().join(format!("fusee-security-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&d);
    std::fs::create_dir_all(&d).unwrap();
    d
}

/// A CA generated for one test, writing PEM files into `dir`.
struct Pki {
    dir: PathBuf,
    issuer: Issuer<'static, KeyPair>,
}

impl Pki {
    fn new(dir: &Path, name: &str) -> Self {
        let mut params = CertificateParams::new(Vec::<String>::new()).unwrap();
        params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        params.distinguished_name.push(DnType::CommonName, name);
        params.key_usages = vec![
            KeyUsagePurpose::KeyCertSign,
            KeyUsagePurpose::CrlSign,
            KeyUsagePurpose::DigitalSignature,
        ];
        let key = KeyPair::generate().unwrap();
        let cert = params.self_signed(&key).unwrap();
        std::fs::write(dir.join(format!("{name}.pem")), cert.pem()).unwrap();
        Self {
            dir: dir.join(name),
            issuer: Issuer::new(params, key),
        }
    }

    fn ca(&self) -> PathBuf {
        self.dir.with_extension("pem")
    }

    /// Writes `<ca>-<who>.pem` and `<ca>-<who>.key`; returns their paths.
    fn leaf(&self, who: &str, names: &[&str]) -> (PathBuf, PathBuf) {
        let mut params =
            CertificateParams::new(names.iter().map(|n| n.to_string()).collect::<Vec<_>>())
                .unwrap();
        params.distinguished_name.push(DnType::CommonName, who);
        params.use_authority_key_identifier_extension = true;
        params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
        params.extended_key_usages = vec![
            ExtendedKeyUsagePurpose::ServerAuth,
            ExtendedKeyUsagePurpose::ClientAuth,
        ];
        let key = KeyPair::generate().unwrap();
        let cert = params.signed_by(&key, &self.issuer).unwrap();
        let base = self.dir.to_string_lossy().into_owned();
        let (c, k) = (
            PathBuf::from(format!("{base}-{who}.pem")),
            PathBuf::from(format!("{base}-{who}.key")),
        );
        std::fs::write(&c, cert.pem()).unwrap();
        write_private(&k, key.serialize_pem().as_bytes());
        (c, k)
    }
}

fn write_private(path: &Path, bytes: &[u8]) {
    use std::os::unix::fs::OpenOptionsExt;
    let mut f = std::fs::OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .mode(0o600)
        .open(path)
        .unwrap();
    f.write_all(bytes).unwrap();
}

fn token_file(dir: &Path, name: &str, secret: &str) -> PathBuf {
    let path = dir.join(name);
    write_private(&path, format!("{secret}\n").as_bytes());
    path
}

fn local() -> LocalOptions {
    LocalOptions {
        flush: FlushMethod::Fsync,
        ..Default::default()
    }
}

async fn start_server(dir: &Path, security: Security) -> (String, Arc<Server>) {
    let server = Server::new(ServerConfig {
        dir: dir.join("data"),
        shards: 2,
        local: local(),
        security,
    })
    .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    tokio::spawn(fusee_server::server::serve_tcp(server.clone(), listener));
    (addr, server)
}

fn register(engine: &Engine) -> fusee::WorkflowRef<u32, u32> {
    engine.register("double", |ctx: Ctx, n: u32| async move {
        let go: Option<u32> = ctx.wait_for("go").await?;
        ctx.step(
            "x",
            || async move { Ok::<_, Error>(n * 2 + go.unwrap_or(0)) },
        )
        .await
    })
}

async fn worker(address: String) -> std::io::Result<(fusee::Config, fusee_server::remote::Remote)> {
    tokio::task::spawn_blocking(move || fusee_server::remote_worker(&address))
        .await
        .unwrap()
}

/// The error a worker that should be refused gets.
async fn worker_refused(address: String) -> std::io::Error {
    match worker(address).await {
        Ok(_) => panic!("the server admitted a worker it should refuse"),
        Err(e) => e,
    }
}

async fn client(address: String) -> std::io::Result<Client> {
    tokio::task::spawn_blocking(move || Client::connect(&address))
        .await
        .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tls_and_a_token_carry_workers_and_clients_end_to_end() {
    let dir = tmp("round-trip");
    let pki = Pki::new(&dir, "ca");
    let (cert, key) = pki.leaf("server", &["127.0.0.1", "localhost"]);
    let token = token_file(&dir, "token", "a9c1d93b7f0e4a6c8d2b5e7f1a3c5e7f");
    let security = Security {
        tls: Some(Tls::from_pem_files(Some(&cert), Some(&key), None).unwrap()),
        token: Some(Token::from_file(&token).unwrap()),
    };
    let (addr, server) = start_server(&dir, security).await;
    let socket = dir.join("fusee.sock");
    let unix = tokio::net::UnixListener::bind(&socket).unwrap();
    tokio::spawn(fusee_server::server::serve_unix(server, unix));
    let options = format!("ca={}&token-file={}", pki.ca().display(), token.display());

    let (cfg, remote) = worker(format!("{addr}?{options}")).await.unwrap();
    let engine = Engine::open(cfg).await.unwrap();
    let wf = register(&engine);
    tokio::spawn(fusee_server::serve_calls(remote, engine.clone()));
    engine.start(&wf, "job-1", &20).await.unwrap();

    // A client over TLS, by host name, signals the run.
    let tls = client(format!(
        "localhost:{}?{options}",
        addr.rsplit_once(':').unwrap().1
    ))
    .await
    .unwrap();
    tokio::task::spawn_blocking(move || tls.signal("job-1", "go", &json!(2)))
        .await
        .unwrap()
        .unwrap();
    // A client on the Unix socket proves the same token without TLS.
    let local = client(format!(
        "unix:{}?token-file={}",
        socket.display(),
        token.display()
    ))
    .await
    .unwrap();
    let result = tokio::task::spawn_blocking(move || local.result("job-1"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(result, json!(42));
    engine.shutdown().await.unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_wrong_or_missing_token_is_refused() {
    let dir = tmp("tokens");
    let right = token_file(&dir, "right", "0f1e2d3c4b5a69788796a5b4c3d2e1f0");
    let wrong = token_file(&dir, "wrong", "ffffffffffffffffffffffffffffffff");
    let security = Security {
        tls: None,
        token: Some(Token::from_file(&right).unwrap()),
    };
    let (addr, _server) = start_server(&dir, security).await;

    let e = worker_refused(format!("{addr}?token-file={}", wrong.display())).await;
    assert_eq!(e.kind(), std::io::ErrorKind::PermissionDenied, "{e}");
    assert!(e.to_string().contains("wrong token"), "{e}");
    let e = worker_refused(addr.clone()).await;
    assert!(
        e.to_string().contains("requires token authentication"),
        "{e}"
    );
    let e = client(format!("{addr}?token-file={}", wrong.display()))
        .await
        .unwrap_err();
    assert_eq!(e.kind(), std::io::ErrorKind::PermissionDenied, "{e}");
    // A client without a token connects, and its first call says why the
    // server hung up.
    let bare = client(addr.clone()).await.unwrap();
    let e = tokio::task::spawn_blocking(move || bare.result("job"))
        .await
        .unwrap()
        .unwrap_err();
    assert!(
        e.to_string().contains("requires token authentication"),
        "{e}"
    );

    // A server without a token says so to a worker that brings one.
    let open_dir = dir.join("open");
    std::fs::create_dir_all(&open_dir).unwrap();
    let (open, _open_server) = start_server(&open_dir, Security::default()).await;
    let e = worker_refused(format!("{open}?token-file={}", right.display())).await;
    assert!(
        e.to_string().contains("does not use token authentication"),
        "{e}"
    );

    // The refusals left the server serving: the right token gets the lease.
    let (cfg, _remote) = worker(format!("{addr}?token-file={}", right.display()))
        .await
        .unwrap();
    let engine = Engine::open(cfg).await.unwrap();
    let wf = register(&engine);
    let run = engine.start(&wf, "job", &1).await.unwrap();
    engine.run::<u32>("job").signal("go", &0).await.unwrap();
    assert_eq!(run.result().await.unwrap(), 2);
    engine.shutdown().await.unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mutual_tls_refuses_a_worker_without_a_trusted_certificate() {
    let dir = tmp("mtls");
    let pki = Pki::new(&dir, "ca");
    let rogue = Pki::new(&dir, "rogue");
    let (cert, key) = pki.leaf("server", &["127.0.0.1"]);
    let (worker_cert, worker_key) = pki.leaf("worker", &["worker"]);
    let (rogue_cert, rogue_key) = rogue.leaf("worker", &["worker"]);
    let security = Security {
        tls: Some(Tls::from_pem_files(Some(&cert), Some(&key), Some(&pki.ca())).unwrap()),
        token: None,
    };
    let (addr, _server) = start_server(&dir, security).await;
    let ca = pki.ca().display().to_string();

    let e = worker_refused(format!("{addr}?ca={ca}")).await;
    assert_eq!(e.kind(), std::io::ErrorKind::PermissionDenied, "{e}");
    let e = worker_refused(format!(
        "{addr}?ca={ca}&cert={}&key={}",
        rogue_cert.display(),
        rogue_key.display()
    ))
    .await;
    assert_eq!(e.kind(), std::io::ErrorKind::PermissionDenied, "{e}");
    // A worker that does not trust the server's CA refuses it, too.
    let e = worker_refused(format!(
        "{addr}?ca={}&cert={}&key={}",
        rogue.ca().display(),
        worker_cert.display(),
        worker_key.display()
    ))
    .await;
    assert_eq!(e.kind(), std::io::ErrorKind::PermissionDenied, "{e}");

    let (cfg, _remote) = worker(format!(
        "{addr}?ca={ca}&cert={}&key={}",
        worker_cert.display(),
        worker_key.display()
    ))
    .await
    .unwrap();
    let engine = Engine::open(cfg).await.unwrap();
    let wf = register(&engine);
    let run = engine.start(&wf, "job", &5).await.unwrap();
    engine.run::<u32>("job").signal("go", &1).await.unwrap();
    assert_eq!(run.result().await.unwrap(), 11);
    engine.shutdown().await.unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------- replicas

fn free_ports(n: usize) -> Vec<String> {
    let ls: Vec<std::net::TcpListener> = (0..n)
        .map(|_| std::net::TcpListener::bind("127.0.0.1:0").unwrap())
        .collect();
    ls.iter()
        .map(|l| l.local_addr().unwrap().to_string())
        .collect()
}

async fn group(
    root: &Path,
    peers: &[String],
    security: impl Fn(u8) -> Security,
) -> Vec<ReplicaNode> {
    let mut nodes = Vec::new();
    for id in 0..peers.len() as u8 {
        nodes.push(
            ReplicaNode::start(ReplicaConfig {
                id,
                peers: peers.to_vec(),
                join: false,
                dir: root.join(format!("r{id}")),
                local: local(),
                timing: Timing {
                    heartbeat_ms: 25,
                    election_ms: 1000,
                },
                security: security(id),
            })
            .await
            .unwrap(),
        );
    }
    nodes
}

/// The only ready primary, once it has stayed so for 300 ms.
async fn stable_primary(nodes: &[ReplicaNode]) -> usize {
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut stable: Option<((usize, u64), Instant)> = None;
    loop {
        let mut ready = nodes.iter().enumerate().filter_map(|(i, n)| {
            let s = n.status();
            s.ready.then_some((i, s.view))
        });
        match (ready.next(), ready.next()) {
            (Some(leader), None) => match stable {
                Some((prior, since)) if prior == leader => {
                    if since.elapsed() > Duration::from_millis(300) {
                        return leader.0;
                    }
                }
                _ => stable = Some((leader, Instant::now())),
            },
            _ => stable = None,
        }
        assert!(Instant::now() < deadline, "no stable primary");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

async fn commit_a_workflow(node: &ReplicaNode, id: &str) {
    let engine = Engine::open(node.engine_config()).await.unwrap();
    let wf = register(&engine);
    let run = engine.start(&wf, id, &20).await.unwrap();
    engine.run::<u32>(id).signal("go", &2).await.unwrap();
    assert_eq!(run.result().await.unwrap(), 42);
    engine.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_replica_group_with_mutual_tls_and_a_token_elects_and_commits() {
    let root = tmp("replica-tls");
    let pki = Pki::new(&root, "ca");
    let token = token_file(&root, "token", "5d41402abc4b2a76b9719d911017c592");
    let peers = free_ports(3);
    let leaves: Vec<_> = (0..3)
        .map(|i| pki.leaf(&format!("member-{i}"), &["127.0.0.1"]))
        .collect();
    let nodes = group(&root, &peers, |id| Security {
        tls: Some(
            Tls::from_pem_files(
                Some(&leaves[id as usize].0),
                Some(&leaves[id as usize].1),
                Some(&pki.ca()),
            )
            .unwrap(),
        ),
        token: Some(Token::from_file(&token).unwrap()),
    })
    .await;
    let p = stable_primary(&nodes).await;
    commit_a_workflow(&nodes[p], "tls-job").await;
    let committed = nodes[p].status().commit;
    // Backups learn the commit over the same authenticated streams.
    let deadline = Instant::now() + Duration::from_secs(10);
    while nodes.iter().any(|n| n.status().commit < committed) {
        assert!(Instant::now() < deadline, "backups did not catch up");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    for n in &nodes {
        let (_, sent, received) = n.observer().snapshot();
        assert!(sent > 0 && received > 0);
    }
    drop(nodes);
    let _ = std::fs::remove_dir_all(&root);
}

/// Forged `StartViewChange` frames for a much later view, claiming every
/// sender ID, on a fresh stream to `addr`. Returns the stream.
fn attack(addr: &str, view: u64, n: u8) -> std::net::TcpStream {
    let mut s = std::net::TcpStream::connect(addr).unwrap();
    // Before the member can reset the stream: macOS refuses socket options
    // on a reset socket.
    s.set_read_timeout(Some(Duration::from_secs(15))).unwrap();
    let mut body = Vec::new();
    encode_msg(&Msg::StartViewChange { view }, &mut body);
    let mut out = Vec::new();
    for from in 0..n {
        frame::encode(from, &body, &mut out);
    }
    let _ = s.write_all(&out);
    s
}

/// The replica closed the stream (EOF or reset). Returns what it sent first:
/// nothing for a stream that skipped the token exchange, a refusal for one
/// that failed it.
fn closed_by_peer(mut s: std::net::TcpStream) -> Option<Vec<u8>> {
    // Fails on a socket the member already reset, which then reads as closed.
    let _ = s.set_read_timeout(Some(Duration::from_secs(15)));
    let mut got = Vec::new();
    let mut buf = [0u8; 256];
    loop {
        match s.read(&mut buf) {
            Ok(0) => return Some(got),
            Err(e)
                if matches!(
                    e.kind(),
                    std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                ) =>
            {
                return None;
            }
            Err(_) => return Some(got),
            Ok(n) => got.extend_from_slice(&buf[..n]),
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn frames_from_an_unauthenticated_stream_change_nothing() {
    // Control: without a token, one forged frame moves a member's view.
    let root = tmp("attack-open");
    let peers = free_ports(3);
    let nodes = group(&root, &peers, |_| Security::default()).await;
    let p = stable_primary(&nodes).await;
    let view = nodes[p].status().view;
    // The primary: a backup bound by its read-lease promise defers a
    // request to change views, the primary joins it at once.
    let target = p;
    let _s = attack(&peers[target], view + 10, 3);
    let deadline = Instant::now() + Duration::from_secs(10);
    while nodes[target].status().view <= view {
        assert!(
            Instant::now() < deadline,
            "the forged frame should move an open member's view"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    drop(nodes);
    let _ = std::fs::remove_dir_all(&root);

    // With a token, the same frames are never applied, on any member.
    let root = tmp("attack-token");
    let token = token_file(&root, "token", "c4ca4238a0b923820dcc509a6f75849b");
    let peers = free_ports(3);
    let nodes = group(&root, &peers, |_| Security {
        tls: None,
        token: Some(Token::from_file(&token).unwrap()),
    })
    .await;
    let p = stable_primary(&nodes).await;
    let before: Vec<_> = nodes.iter().map(|n| n.status()).collect();
    let view = before[p].view;
    let mut streams = Vec::new();
    for addr in &peers {
        streams.push(attack(addr, view + 10, 3));
    }
    // A stream that tries the token exchange with the wrong secret, then
    // sends frames anyway.
    let mut liar = std::net::TcpStream::connect(&peers[(p + 1) % 3]).unwrap();
    liar.set_read_timeout(Some(Duration::from_secs(15)))
        .unwrap();
    let mut hello = vec![1u8, 2, 0];
    hello.extend_from_slice(&[7u8; 32]);
    let mut out = Vec::new();
    frame::encode(0xF0, &hello, &mut out);
    liar.write_all(&out).unwrap();
    let mut challenge = [0u8; 13 + 33];
    liar.read_exact(&mut challenge).unwrap();
    let mut out = Vec::new();
    frame::encode(0xF2, &[0u8; 32], &mut out);
    let mut body = Vec::new();
    encode_msg(&Msg::StartViewChange { view: view + 20 }, &mut body);
    frame::encode(0, &body, &mut out);
    let _ = liar.write_all(&out);
    streams.push(liar);

    let liar = streams.pop().unwrap();
    for s in streams {
        let answer = closed_by_peer(s).expect("the member must drop the stream");
        assert!(
            answer.is_empty(),
            "no protocol reply to an unauthenticated stream"
        );
    }
    let refusal = closed_by_peer(liar).expect("the member must drop the stream");
    assert!(
        String::from_utf8_lossy(&refusal).contains("wrong token"),
        "{refusal:?}"
    );
    tokio::time::sleep(Duration::from_millis(500)).await;
    for (node, before) in nodes.iter().zip(&before) {
        let now = node.status();
        assert_eq!(
            now.view, before.view,
            "an unauthenticated frame moved a view"
        );
    }
    assert!(nodes[p].status().ready);
    // The group still commits over its own authenticated streams.
    commit_a_workflow(&nodes[p], "after-attack").await;
    assert_eq!(nodes[p].status().view, view);
    drop(nodes);
    let _ = std::fs::remove_dir_all(&root);
}

fn pem_certs(path: &Path) -> Vec<rustls::pki_types::CertificateDer<'static>> {
    use rustls::pki_types::pem::PemObject;
    rustls::pki_types::CertificateDer::pem_slice_iter(&std::fs::read(path).unwrap())
        .collect::<Result<_, _>>()
        .unwrap()
}

/// An attacker's TLS client: trusts `ca`, presents `cert`.
fn tls_client(ca: &Path, cert: &Path, key: &Path) -> Arc<rustls::ClientConfig> {
    use rustls::pki_types::pem::PemObject;
    let mut roots = rustls::RootCertStore::empty();
    for c in pem_certs(ca) {
        roots.add(c).unwrap();
    }
    let key =
        rustls::pki_types::PrivateKeyDer::from_pem_slice(&std::fs::read(key).unwrap()).unwrap();
    Arc::new(
        rustls::ClientConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_protocol_versions(&[&rustls::version::TLS13])
        .unwrap()
        .with_root_certificates(roots)
        .with_client_auth_cert(pem_certs(cert), key)
        .unwrap(),
    )
}

type TlsAttack = rustls::StreamOwned<rustls::ClientConnection, std::net::TcpStream>;

/// [`attack`] inside a TLS session made with `config`.
fn tls_attack(addr: &str, config: Arc<rustls::ClientConfig>, view: u64, n: u8) -> TlsAttack {
    let tcp = std::net::TcpStream::connect(addr).unwrap();
    tcp.set_read_timeout(Some(Duration::from_secs(15))).unwrap();
    let name = rustls::pki_types::ServerName::try_from("127.0.0.1").unwrap();
    let mut tls =
        rustls::StreamOwned::new(rustls::ClientConnection::new(config, name).unwrap(), tcp);
    let mut body = Vec::new();
    encode_msg(&Msg::StartViewChange { view }, &mut body);
    let mut out = Vec::new();
    for from in 0..n {
        frame::encode(from, &body, &mut out);
    }
    let _ = tls.write_all(&out).and_then(|()| tls.flush());
    tls
}

/// The member ended the TLS session without sending a byte of plaintext.
fn tls_dropped(mut s: TlsAttack) {
    let mut buf = [0u8; 256];
    match s.read(&mut buf) {
        Ok(0) => {}
        Ok(n) => panic!("a member answered an unauthorised stream with {n} bytes"),
        Err(e) => assert!(
            !matches!(
                e.kind(),
                std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
            ),
            "the member kept the stream open"
        ),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mutual_tls_replicas_admit_only_member_certificates_and_serve_secured_workers() {
    let root = tmp("replica-mtls");
    let pki = Pki::new(&root, "ca");
    let rogue = Pki::new(&root, "rogue");
    let token = token_file(&root, "token", "1b6453892473a467d07372d45eb05abc");
    let peers = free_ports(3);
    let leaves: Vec<_> = (0..3)
        .map(|i| pki.leaf(&format!("member-{i}"), &["127.0.0.1"]))
        .collect();
    let member = |id: usize, ca: bool| {
        Tls::from_pem_files(
            Some(&leaves[id].0),
            Some(&leaves[id].1),
            ca.then(|| pki.ca()).as_deref(),
        )
        .unwrap()
    };
    let nodes = group(&root, &peers, |id| Security {
        tls: Some(member(id as usize, true)),
        token: None,
    })
    .await;
    // Each member also serves workers: TLS with its certificate, a token.
    let mut endpoints = Vec::new();
    for (id, node) in nodes.iter().enumerate() {
        let server = Server::replicated(node);
        server
            .set_security(&Security {
                tls: Some(member(id, false)),
                token: Some(Token::from_file(&token).unwrap()),
            })
            .unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        endpoints.push(listener.local_addr().unwrap().to_string());
        tokio::spawn(fusee_server::server::serve_tcp(server, listener));
    }
    let p = stable_primary(&nodes).await;
    let before: Vec<_> = nodes.iter().map(|n| n.status().view).collect();
    let view = before[p];

    // The group's CA signed this certificate, but for a worker's name, not
    // a member's host; the other one comes from a CA the group does not trust.
    let (worker_cert, worker_key) = pki.leaf("worker", &["worker.internal"]);
    let (rogue_cert, rogue_key) = rogue.leaf("member-x", &["127.0.0.1"]);
    let not_a_member = tls_client(&pki.ca(), &worker_cert, &worker_key);
    let untrusted = tls_client(&pki.ca(), &rogue_cert, &rogue_key);
    let mut tls_streams = Vec::new();
    let mut plain_streams = Vec::new();
    for addr in &peers {
        tls_streams.push(tls_attack(addr, not_a_member.clone(), view + 10, 3));
        tls_streams.push(tls_attack(addr, untrusted.clone(), view + 10, 3));
        plain_streams.push(attack(addr, view + 10, 3));
    }
    let dropped = tokio::task::spawn_blocking(move || {
        for s in tls_streams {
            tls_dropped(s);
        }
        for s in plain_streams {
            let answer = closed_by_peer(s).expect("the member must drop the stream");
            assert!(
                answer.len() < 64,
                "a protocol reply to a plain stream: {answer:?}"
            );
        }
    });
    dropped.await.unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;
    for (node, before) in nodes.iter().zip(&before) {
        assert_eq!(
            node.status().view,
            *before,
            "an unauthorised frame moved a view"
        );
    }
    assert!(nodes[p].status().ready);

    // A worker finds the primary through every member's TLS listener and
    // runs a workflow the group commits.
    let options = format!("ca={}&token-file={}", pki.ca().display(), token.display());
    let address = format!("replicas://{}?{options}", endpoints.join(","));
    let refused = worker_refused(format!(
        "replicas://{}?ca={}",
        endpoints.join(","),
        pki.ca().display()
    ))
    .await;
    assert!(
        refused
            .to_string()
            .contains("requires token authentication"),
        "{refused}"
    );
    let (cfg, _remote) = worker(address).await.unwrap();
    let engine = Engine::open(cfg).await.unwrap();
    let wf = register(&engine);
    let run = engine.start(&wf, "mtls-job", &4).await.unwrap();
    engine
        .run::<u32>("mtls-job")
        .signal("go", &1)
        .await
        .unwrap();
    assert_eq!(run.result().await.unwrap(), 9);
    engine.shutdown().await.unwrap();
    assert_eq!(nodes[p].status().view, view);
    drop(nodes);
    let _ = std::fs::remove_dir_all(&root);
}

/// Per-shard leases and the shared log under TLS and the token: two
/// workers each hold one shard (one over TLS, one on the Unix socket with
/// the token; the second may hold two but finds one free), clients reach
/// both, and when one leaves the other is granted its shard and finishes
/// its run.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn workers_sharing_shards_over_tls_and_a_token_hand_shards_over() {
    let dir = tmp("shared-shards");
    let pki = Pki::new(&dir, "ca");
    let (cert, key) = pki.leaf("server", &["127.0.0.1", "localhost"]);
    let token = token_file(&dir, "token", "5be1d93b7f0e4a6c8d2b5e7f1a3c5e70");
    let security = Security {
        tls: Some(Tls::from_pem_files(Some(&cert), Some(&key), None).unwrap()),
        token: Some(Token::from_file(&token).unwrap()),
    };
    let (addr, server) = start_server(&dir, security).await;
    assert!(server.shared_log());
    let socket = dir.join("fusee.sock");
    let unix = tokio::net::UnixListener::bind(&socket).unwrap();
    tokio::spawn(fusee_server::server::serve_unix(server, unix));
    let tls = format!(
        "{addr}?ca={}&max-shards=1&token-file={}",
        pki.ca().display(),
        token.display()
    );
    let on_unix = format!(
        "unix:{}?max-shards=2&token-file={}",
        socket.display(),
        token.display()
    );

    let (cfg, remote_a) = worker(tls.clone()).await.unwrap();
    let engine_a = Engine::open(cfg).await.unwrap();
    register(&engine_a);
    let calls_a = tokio::spawn(fusee_server::serve_calls(
        remote_a.clone(),
        engine_a.clone(),
    ));
    let (cfg, remote_b) = worker(on_unix).await.unwrap();
    let engine_b = Engine::open(cfg).await.unwrap();
    register(&engine_b);
    let calls_b = tokio::spawn(fusee_server::serve_calls(
        remote_b.clone(),
        engine_b.clone(),
    ));
    let (held_a, held_b) = (remote_a.held_shards(), remote_b.held_shards());
    assert_eq!((held_a.len(), held_b.len()), (1, 1));
    assert_ne!(held_a, held_b);

    // One run on each shard, started through a TLS client.
    let mut ids = [None, None];
    for k in 0.. {
        let id = format!("s-{k}");
        let s = fusee_core::shard::shard_of(&id, 2) as usize;
        ids[s].get_or_insert(id);
        if ids.iter().all(Option::is_some) {
            break;
        }
    }
    let ids = ids.map(Option::unwrap);
    let c = client(tls.clone()).await.unwrap();
    for (n, id) in ids.iter().enumerate() {
        let (c, id) = (c.clone(), id.clone());
        tokio::task::spawn_blocking(move || c.start("double", &id, &json!(n as u32 + 1)))
            .await
            .unwrap()
            .unwrap();
    }
    // A leaves; B is granted A's shard and finishes both runs.
    calls_a.abort();
    let _ = calls_a.await;
    engine_a.shutdown().await.unwrap();
    drop((engine_a, remote_a));
    let deadline = Instant::now() + Duration::from_secs(20);
    while remote_b.held_shards().len() < 2 {
        assert!(Instant::now() < deadline, "B was never granted A's shard");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    for (n, id) in ids.iter().enumerate() {
        let (c, id) = (c.clone(), id.clone());
        let result = tokio::task::spawn_blocking(move || {
            for _ in 0..200 {
                if c.signal(&id, "go", &json!(5)).is_ok() {
                    return c.result(&id);
                }
                std::thread::sleep(Duration::from_millis(20));
            }
            c.result(&id)
        })
        .await
        .unwrap()
        .unwrap();
        assert_eq!(result, json!((n as u32 + 1) * 2 + 5));
    }
    calls_b.abort();
    engine_b.shutdown().await.unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}

/// A client's token makes calls and holds no shard; a reader's makes only
/// calls that read. The scope is decided at the connection's first frame.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn client_and_read_tokens_reach_only_their_scope() {
    let dir = tmp("scopes");
    let worker_token = token_file(&dir, "worker", "a9c1d93b7f0e4a6c8d2b5e7f1a3c5e7f");
    let client_token = token_file(&dir, "client", "c1d2e3f4a5b6c7d8e9f0a1b2c3d4e5f6");
    let read_token = token_file(&dir, "read", "0f1e2d3c4b5a69788796a5b4c3d2e1f0");
    let security = Security {
        tls: None,
        token: Some(Token::from_file(&worker_token).unwrap()),
    };
    let (addr, server) = start_server(&dir, security.clone()).await;
    server
        .set_security_scoped(
            &security,
            vec![
                (Token::from_file(&client_token).unwrap(), Scope::Client),
                (Token::from_file(&read_token).unwrap(), Scope::Read),
            ],
        )
        .unwrap();
    let at = |t: &Path| format!("{addr}?token-file={}", t.display());

    // A worker with a client's or a reader's token is refused its shards.
    for t in [&client_token, &read_token] {
        let e = worker_refused(at(t)).await;
        assert!(e.to_string().contains("cannot hold shards"), "{e}");
    }
    let (cfg, remote) = worker(at(&worker_token)).await.unwrap();
    let engine = Engine::open(cfg).await.unwrap();
    register(&engine);
    tokio::spawn(fusee_server::serve_calls(remote, engine.clone()));

    // The client's token starts and signals, naming who asked (audit): the
    // name crosses the server to the worker's record.
    let c = client(at(&client_token)).await.unwrap();
    let journal = tokio::task::spawn_blocking(move || {
        c.start("double", "s-1", &json!(20))?;
        c.call(
            "signal",
            &json!({"id": "s-1", "name": "go", "value": 2, "by": "cli:ana"}),
        )?;
        c.call("describe", &json!({"id": "s-1"}))
    })
    .await
    .unwrap()
    .unwrap()["journal"]
        .clone();
    let signal = journal
        .as_array()
        .unwrap()
        .iter()
        .find(|r| r["kind"] == "signal")
        .unwrap();
    assert_eq!(signal["by"], "cli:ana", "{journal}");
    // The reader's token reads, and is refused a change.
    let r = client(at(&read_token)).await.unwrap();
    let (result, refused) =
        tokio::task::spawn_blocking(move || (r.result("s-1"), r.start("double", "s-2", &json!(1))))
            .await
            .unwrap();
    assert_eq!(result.unwrap(), json!(42));
    let e = refused.unwrap_err();
    assert!(e.to_string().contains("only reads"), "{e}");
    // A token the server does not know is refused outright.
    let stranger = token_file(&dir, "stranger", "ffffffffffffffffffffffffffffffff");
    assert!(client(at(&stranger)).await.is_err());
    engine.shutdown().await.unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}

/// The HTTP API of `fusee server --http` reaches its own server inside the
/// process, so the worker listener's TLS and token do not stand in its way,
/// and on a replica group it works once its member is the ready primary,
/// though that member closed every connection until then. A backup's API
/// says that calls go to the primary.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_servers_own_http_api_reaches_it_over_tls_and_on_a_replica_group() {
    use fusee::http::Calls;
    let root = tmp("local-calls");
    let pki = Pki::new(&root, "ca");
    let token = token_file(&root, "token", "c2f3a1b4d5e6f708192a3b4c5d6e7f80");
    let peers = free_ports(3);
    let leaves: Vec<_> = (0..3)
        .map(|i| pki.leaf(&format!("member-{i}"), &["127.0.0.1"]))
        .collect();
    let tls = |id: usize, ca: bool| {
        Tls::from_pem_files(
            Some(&leaves[id].0),
            Some(&leaves[id].1),
            ca.then(|| pki.ca()).as_deref(),
        )
        .unwrap()
    };
    let nodes = group(&root, &peers, |id| Security {
        tls: Some(tls(id as usize, true)),
        token: None,
    })
    .await;
    // Each member serves workers over TLS with a token, and has its API's
    // calls from the start, before any member is ready.
    let mut endpoints = Vec::new();
    let mut apis = Vec::new();
    for (id, node) in nodes.iter().enumerate() {
        let server = Server::replicated(node);
        server
            .set_security(&Security {
                tls: Some(tls(id, false)),
                token: Some(Token::from_file(&token).unwrap()),
            })
            .unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        endpoints.push(listener.local_addr().unwrap().to_string());
        apis.push(fusee_server::client::LocalCalls::new(&server));
        tokio::spawn(fusee_server::server::serve_tcp(server, listener));
    }
    let p = stable_primary(&nodes).await;
    let options = format!("ca={}&token-file={}", pki.ca().display(), token.display());
    let (cfg, remote) = worker(format!("replicas://{}?{options}", endpoints.join(",")))
        .await
        .unwrap();
    let engine = Engine::open(cfg).await.unwrap();
    register(&engine);
    tokio::spawn(fusee_server::serve_calls(remote, engine.clone()));

    let api = &apis[p];
    api.call(
        "start",
        json!({"type": "double", "id": "http-job", "input": 20}),
    )
    .await
    .unwrap();
    api.call(
        "signal",
        json!({"id": "http-job", "name": "go", "value": 2}),
    )
    .await
    .unwrap();
    assert_eq!(
        api.call("result", json!({"id": "http-job"})).await.unwrap(),
        json!(42)
    );
    let listed = api
        .call("list", json!({"where": {}, "limit": 10}))
        .await
        .unwrap();
    assert_eq!(listed[0]["id"], "http-job", "{listed}");
    let backup = (p + 1) % 3;
    let e = apis[backup].call("list", json!({})).await.unwrap_err();
    assert!(
        matches!(&e, Error::Connection(m) if m.contains("backup")),
        "{e}"
    );
    engine.shutdown().await.unwrap();
    drop(nodes);
    let _ = std::fs::remove_dir_all(&root);
}
