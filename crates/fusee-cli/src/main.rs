//! `fusee` command line.
//!
//! - `fusee disk-check <dir> [seconds]`: flush latency of the disk under `dir`.
//! - `fusee inspect <dir>`: segments, checkpoints and records per shard.
//! - `fusee export <dir> <workflow-id> [-o file]` and
//!   `fusee export --sample N <dir> [-o dir]`: journals for replay testing.
//! - `fusee show <file.fjnl>`: an exported journal, readable.
//! - `fusee upgrade <dir>`: converts a closed log-format-2 directory to the
//!   shared log (format 3).
//! - `fusee reshard <dir> <shards>`: changes a closed local directory's
//!   shard count.
//! - `fusee restore <target> <dir> [--at <time>] [--timeline <id>]`:
//!   rebuilds a data directory from a continuous backup.
//! - `fusee backups <target> [--verify] [--json]`: a backup's timelines and
//!   restore points.
//! - `fusee backup <dir> <target>`: backs up a closed directory once.
//! - `fusee server ...`: server mode (see `fusee server --help`).
//! - `fusee replicas --dir <dir> status|set id=host:port,...`: a replica
//!   server's role and membership, and membership changes.

use std::path::{Path, PathBuf};
use std::time::Duration;

/// The files the HTTP API's options come from, read again on `SIGHUP`.
struct HttpFiles {
    token: Option<String>,
    client_token: Option<String>,
    read_token: Option<String>,
    namespaces: Option<String>,
    codec_url: Option<String>,
    identity_header: Option<String>,
    insecure: bool,
}

impl HttpFiles {
    fn options(&self) -> Result<fusee::http::HttpOptions, String> {
        let read = |f: &Option<String>| -> Result<Option<String>, String> {
            f.as_ref()
                .map(|p| {
                    std::fs::read_to_string(p)
                        .map(|t| t.trim().to_owned())
                        .map_err(|e| format!("{p}: {e}"))
                })
                .transpose()
        };
        Ok(fusee::http::HttpOptions {
            token: read(&self.token)?,
            client_token: read(&self.client_token)?,
            read_token: read(&self.read_token)?,
            namespaces: match &self.namespaces {
                Some(p) => read_namespaces(p)?,
                None => Vec::new(),
            },
            codec_url: self.codec_url.clone(),
            identity_header: self.identity_header.clone(),
            insecure: self.insecure,
            ..Default::default()
        })
    }
}

/// `--namespaces-file`: lines of `<namespace> <token>`; blank lines and
/// `#` comments are skipped.
fn read_namespaces(path: &str) -> Result<Vec<(String, String)>, String> {
    let text = std::fs::read_to_string(path).map_err(|e| format!("{path}: {e}"))?;
    let mut out = Vec::new();
    for (n, line) in text.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        match line.split_whitespace().collect::<Vec<_>>().as_slice() {
            [ns, token] if !ns.contains('/') => out.push((token.to_string(), ns.to_string())),
            _ => {
                return Err(format!(
                    "{path}:{}: expected `<namespace> <token>` (a namespace has no '/')",
                    n + 1
                ));
            }
        }
    }
    Ok(out)
}

const SERVER_HELP: &str = "\
fusee server --dir <dir> [--listen host:port] [--unix /path.sock] [--shards N]
             [--client-token-file FILE] [--read-token-file FILE]
             [--http host:port [--namespaces-file FILE] [--codec-url URL]
              [--identity-header NAME [--http-insecure]]]
             [--replicas host:port,... --replica-id N [--join]]
             [--heartbeat-ms N] [--election-ms N]
             [--flush fullfsync|fsync|fdatasync|dsync]
             [--tls-cert FILE --tls-key FILE] [--tls-ca FILE] [--token-file FILE]
             [--replica-tls-ca FILE] [--replica-token-file FILE]
             [--invoke URL [--invoke-secret-file FILE] [--invoke-types a,b]
              [--invoke-concurrency N] [--invoke-budget-ms N] [--invoke-shards N]
              [--invoke-ca FILE] [--invoke-cert FILE --invoke-key FILE]]...
             [--invoke-config FILE]

  --http                 Serves the HTTP API, the web UI and /metrics here.
                         Off loopback it needs --token-file (the same token
                         as Bearer).
  --client-token-file    A client's token: it makes calls (start, signal,
                         query, ...) over the server protocol and the HTTP
                         API, and cannot hold shards. Needs --token-file,
                         the workers' token.
  --read-token-file      A token that only reads (dashboards, on-call): GET
                         requests on the HTTP API, read calls on the server
                         protocol.
  --namespaces-file      HTTP tokens that reach one namespace each: lines of
                         `<namespace> <token>`. A namespace is a prefix of
                         the workflow ID: `acme`'s token reaches `acme/...`
                         runs only.
  --codec-url            The operator's codec endpoint (fusee codec-server):
                         the UI decodes encrypted payloads through it.
  --identity-header      Behind a proxy that signs people in: the header in
                         which it names the user (X-Forwarded-User). Calls
                         need it or a token; signals, cancellations,
                         terminations and resets record it (audit).
  --http-insecure        Serve the HTTP API off loopback without a token:
                         only behind such a proxy.
  --replicas             The members' replication addresses: host:port,...
                         (member IDs 0, 1, 2, ... in order) or
                         id=host:port,... Once the membership has changed
                         (fusee replicas set), the directory's saved
                         membership wins and this only says where to look.
  --join                 This replica is a new member of a running group:
                         start it with an empty --dir, then add it with
                         fusee replicas set on any member.
  --heartbeat-ms         Primary heartbeat interval (default 5).
  --election-ms          A backup that hears nothing from its primary this
                         long, plus up to as long again at random, starts
                         an election (default 40). At least four heartbeats.

  --tls-cert, --tls-key  PEM certificate chain and private key. Workers and
                         clients connect with TLS 1.3; with --replicas the
                         replica protocol uses them too, in both directions.
  --tls-ca               Workers and clients must present a certificate this
                         CA signed (mutual TLS). Also the replicas' CA unless
                         --replica-tls-ca is given.
  --token-file           Workers and clients must prove this shared secret
                         (HMAC challenge, never sent). Applies to --unix too.
  --replica-tls-ca       CA that signs every member's certificate: replicas
                         use mutual TLS with each other. Each certificate
                         must name the host of its member's --replicas
                         entry and allow server and client authentication.
  --replica-token-file   Secret replicas prove to each other (default: the
                         --token-file secret).

  --invoke               An HTTP endpoint that runs workers on demand
                         (functions that scale to zero): the server POSTs to
                         it when a shard with no worker has work due. Repeat
                         for more endpoints; the --invoke-* options after
                         one apply to it. See docs/server-mode.md.
  --invoke-secret-file   Signs each request (HMAC-SHA256, fusee-signature).
  --invoke-types         Workflow types this endpoint has code for (default
                         every type).
  --invoke-concurrency   Invocations in flight at once (default 16).
  --invoke-budget-ms     How long one invocation may take (default 60000).
  --invoke-shards        Shards per invocation at most (default no limit).
  --invoke-ca            CA of an https endpoint (default the system bundle).
  --invoke-cert, --invoke-key
                         Client certificate the server presents (mutual TLS).
  --invoke-config        A JSON file with an endpoints list; each entry has
                         url and optionally secretFile, types, concurrency,
                         budgetMs, shards, graceMs, ca, cert and key.

A listener on a non-loopback address without TLS or a token prints a warning.";

/// `--invoke URL` and the `--invoke-*` options after it, then
/// `--invoke-config FILE`: the server's invocation endpoints.
fn invoke_endpoints(args: &[String]) -> std::io::Result<Vec<fusee_server::invoke::Endpoint>> {
    let bad = |m: String| std::io::Error::new(std::io::ErrorKind::InvalidInput, m);
    let mut list: Vec<serde_json::Map<String, serde_json::Value>> = Vec::new();
    let mut config = Vec::new();
    let mut i = 1;
    while i < args.len() {
        let flag = args[i].as_str();
        if !flag.starts_with("--invoke") {
            i += 1;
            continue;
        }
        let v = args
            .get(i + 1)
            .cloned()
            .ok_or_else(|| bad(format!("{flag} needs a value")))?;
        i += 2;
        if flag == "--invoke" {
            let mut e = serde_json::Map::new();
            e.insert("url".into(), v.into());
            list.push(e);
            continue;
        }
        if flag == "--invoke-config" {
            config.push(v);
            continue;
        }
        let key = match flag {
            "--invoke-secret-file" => "secretFile",
            "--invoke-types" => "types",
            "--invoke-concurrency" => "concurrency",
            "--invoke-budget-ms" => "budgetMs",
            "--invoke-shards" => "shards",
            "--invoke-ca" => "ca",
            "--invoke-cert" => "cert",
            "--invoke-key" => "key",
            _ => return Err(bad(format!("unknown option {flag}"))),
        };
        let Some(e) = list.last_mut() else {
            return Err(bad(format!(
                "{flag} applies to the --invoke before it; give --invoke URL first"
            )));
        };
        let value = match key {
            "types" => serde_json::Value::from(v.split(',').map(str::to_owned).collect::<Vec<_>>()),
            "concurrency" | "budgetMs" | "shards" => serde_json::Value::from(
                v.parse::<u64>()
                    .map_err(|_| bad(format!("{flag} takes a whole number, not '{v}'")))?,
            ),
            _ => v.into(),
        };
        if e.insert(key.into(), value).is_some() {
            return Err(bad(format!("{flag} is given twice for one --invoke")));
        }
    }
    let mut endpoints = list
        .into_iter()
        .map(|e| fusee_server::invoke::Endpoint::from_json(&serde_json::Value::Object(e)))
        .collect::<std::io::Result<Vec<_>>>()?;
    for path in config {
        endpoints.extend(fusee_server::invoke::Endpoint::from_config_file(path)?);
    }
    Ok(endpoints)
}

/// TLS and token settings for the worker listener and the replica protocol.
#[derive(Clone)]
struct SecurityFlags {
    cert: Option<String>,
    key: Option<String>,
    ca: Option<String>,
    token: Option<String>,
    replica_ca: Option<String>,
    replica_token: Option<String>,
}

impl SecurityFlags {
    fn workers(&self) -> std::io::Result<fusee_server::security::Security> {
        let tls = match (&self.cert, &self.key) {
            (None, None) if self.ca.is_some() => {
                return Err(std::io::Error::other(
                    "--tls-ca requires --tls-cert and --tls-key",
                ));
            }
            (None, None) => None,
            (Some(cert), Some(key)) => Some(fusee_server::security::Tls::from_pem_files(
                Some(Path::new(cert)),
                Some(Path::new(key)),
                self.ca.as_deref().map(Path::new),
            )?),
            _ => {
                return Err(std::io::Error::other(
                    "--tls-cert and --tls-key go together",
                ));
            }
        };
        Ok(fusee_server::security::Security {
            tls,
            token: self
                .token
                .as_deref()
                .map(fusee_server::security::Token::from_file)
                .transpose()?,
        })
    }

    /// The tokens of less reach than the worker's: `--client-token-file`
    /// and `--read-token-file`.
    fn scoped(
        &self,
        client: Option<&str>,
        read: Option<&str>,
    ) -> std::io::Result<Vec<(fusee_server::security::Token, fusee_server::security::Scope)>> {
        use fusee_server::security::{Scope, Token};
        let mut out = Vec::new();
        for (file, scope) in [(client, Scope::Client), (read, Scope::Read)] {
            if let Some(f) = file {
                let t = Token::from_file(f)
                    .map_err(|e| std::io::Error::new(e.kind(), format!("{f}: {e}")))?;
                out.push((t, scope));
            }
        }
        Ok(out)
    }

    fn replicas(&self) -> std::io::Result<fusee_server::security::Security> {
        let ca = self.replica_ca.as_ref().or(self.ca.as_ref());
        let tls = match (&self.cert, &self.key, ca) {
            (None, None, Some(_)) if self.replica_ca.is_some() => {
                return Err(std::io::Error::other(
                    "--replica-tls-ca requires --tls-cert and --tls-key",
                ));
            }
            (None, None, _) => None,
            (Some(cert), Some(key), Some(ca)) => Some(fusee_server::security::Tls::from_pem_files(
                Some(Path::new(cert)),
                Some(Path::new(key)),
                Some(Path::new(ca)),
            )?),
            (Some(_), Some(_), None) => {
                return Err(std::io::Error::other(
                    "with --replicas, --tls-cert also secures the replica protocol, which needs --replica-tls-ca (or --tls-ca) to verify the other members",
                ));
            }
            _ => {
                return Err(std::io::Error::other(
                    "--tls-cert and --tls-key go together",
                ));
            }
        };
        Ok(fusee_server::security::Security {
            tls,
            token: self
                .replica_token
                .as_ref()
                .or(self.token.as_ref())
                .map(fusee_server::security::Token::from_file)
                .transpose()?,
        })
    }
}

/// The local admin socket of a replica server: `<dir>/vsr/admin.sock`,
/// guarded by the directory's permissions. One request line per connection
/// (`status`, or `set id=host:port,...`), answered with text lines.
fn admin_socket(dir: &str) -> PathBuf {
    Path::new(dir).join("vsr").join("admin.sock")
}

fn serve_replica_admin(
    dir: &str,
    node: std::sync::Arc<fusee_server::replicated::ReplicaNode>,
) -> std::io::Result<()> {
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt};
    let path = admin_socket(dir);
    let _ = std::fs::remove_file(&path);
    let listener = tokio::net::UnixListener::bind(&path)?;
    tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            let node = node.clone();
            tokio::spawn(async move {
                let (read, mut write) = stream.into_split();
                let mut line = String::new();
                if tokio::io::BufReader::new(read)
                    .read_line(&mut line)
                    .await
                    .is_err()
                {
                    return;
                }
                let reply = match line.trim().split_once(' ').unwrap_or((line.trim(), "")) {
                    ("status", _) => replica_status(&node),
                    ("set", list) => match fusee_server::replicated::parse_members(list) {
                        Ok(addresses) => {
                            let members = addresses
                                .into_iter()
                                .enumerate()
                                .filter(|(_, a)| !a.is_empty())
                                .map(|(id, a)| (id as u8, a))
                                .collect();
                            match node.change_members(members, Duration::from_secs(30)).await {
                                Ok(epoch) => {
                                    format!(
                                        "membership changed: epoch {epoch}\n{}",
                                        replica_status(&node)
                                    )
                                }
                                Err(e) => format!("error: {e}\n"),
                            }
                        }
                        Err(e) => format!("error: {e}\n"),
                    },
                    _ => "error: expected status or set id=host:port,...\n".into(),
                };
                let _ = write.write_all(reply.as_bytes()).await;
            });
        }
    });
    Ok(())
}

fn replica_status(node: &fusee_server::replicated::ReplicaNode) -> String {
    let s = node.status();
    let (epoch, members) = node.membership();
    let role = if s.retired {
        "retired (removed from the group; stop it once the new members have caught up)"
    } else if s.joining {
        "joining (waiting to be added with fusee replicas set)"
    } else if s.ready {
        "primary"
    } else if s.primary {
        "primary, catching up"
    } else {
        "backup"
    };
    let mut out = format!(
        "role: {role}\nview: {}\nepoch: {epoch}\ncommit: {} (op {}, checkpoint {})\nmembers:\n",
        s.view, s.commit, s.op, s.checkpoint
    );
    for (id, addr) in members {
        out.push_str(&format!("  {id}={addr}\n"));
    }
    out
}

/// `fusee replicas --dir <dir> status|set <list>`: asks the replica server
/// running on `dir` through its admin socket.
async fn replicas_command(args: &[String]) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut dir = None;
    let mut rest = Vec::new();
    let mut i = 0;
    while i < args.len() {
        if args[i] == "--dir" {
            dir = args.get(i + 1).cloned();
            i += 2;
        } else {
            rest.push(args[i].clone());
            i += 1;
        }
    }
    let (Some(dir), Some(command)) = (dir, rest.first()) else {
        usage()
    };
    let request = match command.as_str() {
        "status" => "status\n".to_string(),
        "set" => match rest.get(1) {
            Some(list) => format!("set {list}\n"),
            None => usage(),
        },
        _ => usage(),
    };
    let path = admin_socket(&dir);
    let mut stream = match tokio::net::UnixStream::connect(&path).await {
        Ok(s) => s,
        Err(e) => {
            eprintln!(
                "replicas: cannot reach the replica server of {dir} at {}: {e}",
                path.display()
            );
            std::process::exit(1);
        }
    };
    let mut reply = String::new();
    if let Err(e) = async {
        stream.write_all(request.as_bytes()).await?;
        stream.read_to_string(&mut reply).await
    }
    .await
    {
        eprintln!("replicas: {e}");
        std::process::exit(1);
    }
    print!("{reply}");
    if reply.starts_with("error") {
        std::process::exit(1);
    }
}

fn describe(security: &fusee_server::security::Security) -> &'static str {
    match (security.tls.is_some(), security.token.is_some()) {
        (true, true) => " (TLS, token)",
        (true, false) => " (TLS)",
        (false, true) => " (token)",
        (false, false) => "",
    }
}

const SELF_PARSED_USAGE: &str = "usage:\n  fusee disk-check <dir> [seconds-per-probe]\n  fusee inspect <dir>\n  fusee upgrade <dir>\n  fusee reshard <dir> <shards>\n  fusee restore <target> <dir> [--at <time>] [--timeline <id>]\n  fusee backups <target> [--verify] [--json]\n  fusee backup <dir> <target> [--retention <days>]\n  fusee export <dir> <workflow-id> [-o file.fjnl]\n  fusee export --sample N <dir> [-o out-dir]\n  fusee show <file.fjnl>\n  fusee server --dir <dir> [--listen 127.0.0.1:7600] [--unix <path>] [--shards N] [--replicas host:port,... --replica-id N [--join]] [timing and security options, see fusee server --help]\n  fusee replicas --dir <dir> status\n  fusee replicas --dir <dir> set id=host:port,...\n  fusee version";

fn usage() -> ! {
    eprintln!("{SELF_PARSED_USAGE}");
    std::process::exit(2)
}

/// A `fusee server` option it cannot start with.
fn server_error(message: &str) -> ! {
    eprintln!("server: {message}");
    std::process::exit(2)
}

/// Log format 3: the shared log's segments, the chain through them, and
/// each shard's records in it.
fn inspect_wal(dir: &std::path::Path) -> std::io::Result<()> {
    use fusee_core::wal::{End, Progress, WalHeader, WalScanner};
    let mut segs: Vec<(u64, PathBuf)> = std::fs::read_dir(dir)?
        .filter_map(|e| e.ok())
        .filter_map(|e| {
            let n = e.file_name().to_str()?.strip_suffix(".wal")?.parse().ok()?;
            Some((n, e.path()))
        })
        .collect();
    segs.sort();
    println!(
        "{}  (shared log, format {})",
        dir.display(),
        fusee_core::wal::WAL_FORMAT
    );
    let mut chain: Option<(u64, u64)> = None;
    let mut per_shard: std::collections::BTreeMap<u32, (u64, u64)> = Default::default();
    // References to offloaded records in the chain: (batch, shard, LSN, ref).
    let mut refs: Vec<(u64, u32, u64, fusee_core::wal::OffloadRef)> = Vec::new();
    for (n, path) in segs {
        let bytes = std::fs::read(&path)?;
        let name = format!("{n:020}.wal");
        let header = match WalHeader::decode(&bytes) {
            Ok(Some(h)) if h.segment == n => h,
            Ok(None) => {
                println!("  {name}  spare ({} bytes)", bytes.len());
                continue;
            }
            Ok(Some(h)) => {
                println!(
                    "  {name}  header names segment {}: not part of the log",
                    h.segment
                );
                continue;
            }
            Err(e) => {
                println!("  {name}  {e}");
                continue;
            }
        };
        if let Some((seq, checksum)) = chain
            && (header.base_seq != seq + 1 || header.base_prev != checksum)
        {
            println!(
                "  {name}  starts at batch {} but the chain is at {seq}: not part of the log",
                header.base_seq
            );
            continue;
        }
        let Some(mut scanner) = WalScanner::start(&bytes, true).ok().flatten() else {
            continue;
        };
        let mut records = 0u64;
        let (mut offloaded, mut offloaded_bytes) = (0u64, 0u64);
        let Progress::Done(s) = scanner.advance(&bytes, true, bytes.len(), &mut |b| {
            for sec in b.sections() {
                records += u64::from(sec.count);
                let e = per_shard
                    .entry(sec.shard)
                    .or_insert((sec.first_lsn, sec.first_lsn));
                e.1 = sec.first_lsn + u64::from(sec.count) - 1;
                let _ = fusee_core::wal::for_each_offload(sec.records, |i, r| {
                    offloaded += 1;
                    offloaded_bytes += r.len;
                    refs.push((b.header.seq, sec.shard, sec.first_lsn + u64::from(i), r));
                });
            }
        }) else {
            continue;
        };
        let tail = match s.tail {
            End::Clean => "clean".to_string(),
            End::Torn => "torn write after it (never acknowledged)".to_string(),
            End::Full => "full".to_string(),
            End::Damaged { at, later_seq } => format!(
                "DAMAGED at offset {at}: batch {later_seq} after it says acknowledged batches were lost"
            ),
        };
        let offload = if offloaded > 0 {
            format!("  offloaded {offloaded} ({offloaded_bytes} bytes in the blob area)")
        } else {
            String::new()
        };
        println!(
            "  {name}  batches {}..={} ({})  records {records}{offload}  {} of {} bytes used  {tail}",
            header.base_seq,
            s.last_seq,
            s.batches,
            s.end,
            bytes.len()
        );
        chain = Some((s.last_seq, s.last_checksum));
    }
    for (shard, (first, last)) in per_shard {
        println!("  shard {shard}: records {first}..={last} in the log");
    }
    inspect_blobs(&dir.join("blobs"), &refs)
}

/// The shared log's blob area: each file, the chain's references into it,
/// and whether every referenced record reads back with its checksum.
fn inspect_blobs(
    dir: &std::path::Path,
    refs: &[(u64, u32, u64, fusee_core::wal::OffloadRef)],
) -> std::io::Result<()> {
    let mut files: Vec<(u64, PathBuf)> = match std::fs::read_dir(dir) {
        Ok(d) => d
            .filter_map(|e| e.ok())
            .filter_map(|e| {
                let n = e
                    .file_name()
                    .to_str()?
                    .strip_suffix(".blob")?
                    .parse()
                    .ok()?;
                Some((n, e.path()))
            })
            .collect(),
        Err(_) if refs.is_empty() => return Ok(()),
        Err(e) => {
            println!(
                "{}  missing ({e}), but the log references {} offloaded records",
                dir.display(),
                refs.len()
            );
            Vec::new()
        }
    };
    files.sort();
    if !files.is_empty() {
        println!("{}  (blob area)", dir.display());
    }
    let mut damaged = Vec::new();
    for (n, path) in &files {
        let bytes = std::fs::read(path)?;
        let mine: Vec<_> = refs.iter().filter(|r| r.3.file == *n).collect();
        let used: u64 = mine.iter().map(|r| r.3.len).sum();
        for (seq, shard, lsn, r) in &mine {
            let end = (r.offset + r.len) as usize;
            if bytes
                .get(r.offset as usize..end)
                .is_none_or(|b| !r.matches(b))
            {
                damaged.push(format!(
                    "batch {seq} shard {shard} record {lsn} (bytes {}..{end} of {n:020}.blob)",
                    r.offset
                ));
            }
        }
        let batches = match (mine.first(), mine.last()) {
            (Some(a), Some(b)) => format!("  from batches {}..={}", a.0, b.0),
            _ => "  referenced by no batch of the chain (deleted at the next open)".into(),
        };
        println!(
            "  {n:020}.blob  {} bytes  {} records ({used} bytes){batches}",
            bytes.len(),
            mine.len()
        );
    }
    for r in refs {
        if !files.iter().any(|(n, _)| *n == r.3.file) {
            damaged.push(format!(
                "batch {} shard {} record {} (blob file {} is missing)",
                r.0, r.1, r.2, r.3.file
            ));
        }
    }
    if damaged.is_empty() {
        if !refs.is_empty() {
            println!("  every one of {} offloaded records verified", refs.len());
        }
    } else {
        println!(
            "  DAMAGED: {} offloaded records do not read back:",
            damaged.len()
        );
        for d in damaged.iter().take(20) {
            println!("    {d}");
        }
    }
    Ok(())
}

fn inspect(dir: PathBuf) -> std::io::Result<()> {
    let meta = std::fs::read_to_string(dir.join("fusee.meta")).unwrap_or_default();
    println!("{}", meta.trim());
    if dir.join("wal").is_dir() {
        inspect_wal(&dir.join("wal"))?;
    }
    let mut shards: Vec<_> = std::fs::read_dir(&dir)?
        .filter_map(|e| e.ok())
        .filter(|e| e.file_name().to_string_lossy().starts_with("shard-"))
        .map(|e| e.path())
        .collect();
    shards.sort();
    for s in shards {
        println!("{}", s.display());
        let mut segs: Vec<_> = std::fs::read_dir(s.join("log"))
            .map(|d| d.filter_map(|e| e.ok()).map(|e| e.path()).collect())
            .unwrap_or_default();
        segs.sort();
        for seg in segs {
            let bytes = std::fs::read(&seg)?;
            match fusee_core::segment::scan_segment(&bytes, |_, _| {}) {
                Ok(Some(sc)) => println!(
                    "  {}  format {}  records {}..={} ({})  end {}  tail {:?}",
                    seg.file_name()
                        .map(|n| n.to_string_lossy().into_owned())
                        .unwrap_or_default(),
                    sc.header.format,
                    sc.header.base_lsn,
                    sc.last_lsn,
                    sc.records,
                    sc.end,
                    sc.tail
                ),
                Ok(None) => println!(
                    "  {}  (prepared, not yet used)",
                    seg.file_name()
                        .map(|n| n.to_string_lossy().into_owned())
                        .unwrap_or_default()
                ),
                Err(e) => println!("  {}  error: {e}", seg.display()),
            }
        }
        for e in std::fs::read_dir(&s)?.filter_map(|e| e.ok()) {
            let name = e.file_name().to_string_lossy().into_owned();
            if name.ends_with(".fck") {
                let bytes = std::fs::read(e.path())?;
                match fusee_core::store::Checkpoint::decode(&bytes) {
                    Ok(c) => println!(
                        "  {name}  through record {}  loaded runs {}  run store gen {} len {}{}",
                        c.lsn,
                        c.runs.len(),
                        c.store_generation,
                        c.store_len,
                        if c.log_position > 0 {
                            format!("  shared log from batch {}", c.log_position)
                        } else {
                            String::new()
                        }
                    ),
                    Err(err) => println!("  {name}  invalid: {err}"),
                }
            }
        }
    }
    Ok(())
}

const BACKUP_HELP: &str = "\
Targets: a directory path (or file:///path), or s3://bucket/prefix with
?region=..&endpoint=https://host&path_style=1 as needed; S3 credentials come
from AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY and AWS_SESSION_TOKEN, the
region from AWS_REGION, a CA bundle from SSL_CERT_FILE.
--at: RFC 3339 (2026-11-09T12:00:00Z), Unix milliseconds, or a time before
now (-90s, -15m, -2h, -1d). The newest restore point at or before it is
restored; without --at, the newest one.";

fn flag(args: &[String], name: &str) -> Option<String> {
    let i = args.iter().position(|a| a == name)?;
    Some(args.get(i + 1).cloned().unwrap_or_else(|| usage()))
}

/// Positional arguments: everything but `--flag value` pairs and the
/// switches named.
fn positional(args: &[String], switches: &[&str]) -> Vec<String> {
    let mut out = Vec::new();
    let mut i = 1;
    while i < args.len() {
        if switches.contains(&args[i].as_str()) {
            i += 1;
        } else if args[i].starts_with("--") {
            i += 2;
        } else {
            out.push(args[i].clone());
            i += 1;
        }
    }
    out
}

fn restore_cmd(args: &[String]) -> std::io::Result<()> {
    let pos = positional(args, &[]);
    let (Some(target), Some(dir)) = (pos.first(), pos.get(1)) else {
        eprintln!(
            "usage: fusee restore <target> <dir> [--at <time>] [--timeline <id>]\n\n{BACKUP_HELP}"
        );
        std::process::exit(2)
    };
    let at_ms = flag(args, "--at")
        .map(|t| fusee::backup::parse_time(&t, fusee::local::unix_ms()))
        .transpose()
        .map_err(std::io::Error::other)?;
    let target = fusee::backup::open_target(target)?;
    let r = fusee::backup::restore(
        &*target,
        Path::new(dir),
        &fusee::backup::RestoreOptions {
            at_ms,
            timeline: flag(args, "--timeline"),
        },
    )?;
    println!(
        "{dir}: restored timeline {} at {} (log batch {}): {} shards, {} checkpoints, {} log segments, {} bytes",
        r.timeline,
        fusee::backup::format_time(r.time_ms),
        r.seq,
        r.shards,
        r.checkpoints,
        r.segments,
        r.bytes
    );
    Ok(())
}

fn backups_cmd(args: &[String]) -> std::io::Result<()> {
    let pos = positional(args, &["--verify", "--json"]);
    let Some(target) = pos.first() else {
        eprintln!("usage: fusee backups <target> [--verify] [--json]\n\n{BACKUP_HELP}");
        std::process::exit(2)
    };
    let target = fusee::backup::open_target(target)?;
    let Some((root, state)) = fusee::backup::load_state(&*target)? else {
        println!("{}: no backup", target.describe());
        return Ok(());
    };
    let verified = if args.iter().any(|a| a == "--verify") {
        Some(fusee::backup::verify(&*target)?)
    } else {
        None
    };
    let mut timelines = Vec::new();
    for t in &state.timelines {
        let ok: Vec<_> = t.restorable(state.shards).collect();
        let log: u64 = t
            .segments
            .iter()
            .flat_map(|s| &s.chunks)
            .map(|c| c.end - c.offset)
            .sum();
        let store: u64 = t.stores.iter().map(|c| c.end - c.offset).sum();
        let cps: u64 = t.checkpoints.iter().map(|c| c.bytes).sum();
        timelines.push(serde_json::json!({
            "id": t.id,
            "created": fusee::backup::format_time(t.created_ms),
            "restore_points": t.points.len(),
            "restorable": ok.len(),
            "oldest": ok.first().map(|p| fusee::backup::format_time(p.0)),
            "newest": ok.last().map(|p| fusee::backup::format_time(p.0)),
            "newest_batch": ok.last().map(|p| p.1),
            "segments": t.segments.len(),
            "checkpoints": t.checkpoints.len(),
            "log_bytes": log,
            "store_bytes": store,
            "checkpoint_bytes": cps,
            "archives": t.archives.len(),
        }));
    }
    if args.iter().any(|a| a == "--json") {
        let mut v = serde_json::json!({
            "target": target.describe(),
            "shards": state.shards,
            "manifest_version": root.version,
            "updated": fusee::backup::format_time(root.updated_ms),
            "timelines": timelines,
        });
        if let Some(c) = &verified {
            v["verify"] = serde_json::json!({
                "objects": c.objects, "bytes": c.bytes,
                "missing": c.missing, "damaged": c.damaged,
            });
        }
        println!("{v}");
    } else {
        println!(
            "{}: {} shards, manifest version {}, updated {}",
            target.describe(),
            state.shards,
            root.version,
            fusee::backup::format_time(root.updated_ms)
        );
        for t in &timelines {
            println!(
                "  timeline {} (created {}): {} restorable of {} restore points, {} .. {}",
                t["id"].as_str().unwrap_or(""),
                t["created"].as_str().unwrap_or(""),
                t["restorable"],
                t["restore_points"],
                t["oldest"].as_str().unwrap_or("-"),
                t["newest"].as_str().unwrap_or("-"),
            );
            println!(
                "    {} log segments ({} bytes), {} checkpoints ({} bytes), run stores {} bytes, {} archives",
                t["segments"],
                t["log_bytes"],
                t["checkpoints"],
                t["checkpoint_bytes"],
                t["store_bytes"],
                t["archives"]
            );
        }
        if let Some(c) = &verified {
            println!(
                "  verified {} objects, {} bytes: {} missing, {} damaged",
                c.objects,
                c.bytes,
                c.missing.len(),
                c.damaged.len()
            );
            for k in c.missing.iter().chain(&c.damaged) {
                println!("    {k}");
            }
        }
    }
    if verified.is_some_and(|c| !c.missing.is_empty() || !c.damaged.is_empty()) {
        std::process::exit(1);
    }
    Ok(())
}

async fn backup_cmd(args: &[String]) -> std::io::Result<()> {
    let pos = positional(args, &[]);
    let (Some(dir), Some(target)) = (pos.first(), pos.get(1)) else {
        eprintln!("usage: fusee backup <dir> <target> [--retention <days>]\n\n{BACKUP_HELP}");
        std::process::exit(2)
    };
    if fusee::engine::read_meta(Path::new(dir))
        .map_err(|e| std::io::Error::other(e.to_string()))?
        .is_none()
    {
        return Err(std::io::Error::other(format!(
            "{dir} is not a Fusee data directory"
        )));
    }
    let mut options = fusee::backup::BackupOptions::default();
    if let Some(days) = flag(args, "--retention") {
        let d: f64 = days
            .parse()
            .map_err(|_| std::io::Error::other("--retention takes days"))?;
        options.retention = Duration::from_secs_f64(d * 86_400.0);
    }
    let target = fusee::backup::open_target(target)?;
    let io = |e: fusee::Error| std::io::Error::other(e.to_string());
    let config = cli::tool_config(Path::new(dir)).backup(target.clone(), options);
    let engine = fusee::Engine::open(config).await.map_err(io)?;
    let point = engine.backup().expect("opened with a backup").sync().await;
    engine.shutdown().await.map_err(io)?;
    let point = point?;
    println!(
        "{dir}: backed up to {} through log batch {} (restore point {}, timeline {})",
        target.describe(),
        point.seq,
        fusee::backup::format_time(point.time_ms),
        point.timeline
    );
    Ok(())
}

mod cli;
mod project;

/// The storage and server commands, which parse their own flags.
const SELF_PARSED: &[&str] = &[
    "disk-check",
    "inspect",
    "upgrade",
    "reshard",
    "restore",
    "backups",
    "backup",
    "export",
    "show",
    "server",
    "replicas",
    "version",
];

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args
        .first()
        .is_some_and(|a| SELF_PARSED.contains(&a.as_str()))
    {
        // `fusee <command> --help`: its usage lines (the server has its own).
        if args.get(1).is_some_and(|a| a == "--help" || a == "-h") && args[0] != "server" {
            let prefix = format!("fusee {} ", args[0]);
            for line in SELF_PARSED_USAGE.lines().filter(|l| {
                l.trim_start().starts_with(&prefix) || l.trim() == format!("fusee {}", args[0])
            }) {
                println!("{}", line.trim_start());
            }
            return;
        }
        return self_parsed(args).await;
    }
    let parsed = <cli::Cli as clap::Parser>::parse();
    if let Err(e) = cli::run(parsed).await {
        eprintln!("error: {e}");
        eprintln!("  ({}: {})", e.code(), e.docs());
        std::process::exit(1);
    }
}

async fn self_parsed(args: Vec<String>) {
    match args.first().map(String::as_str) {
        Some("disk-check") => {
            let Some(dir) = args.get(1) else { usage() };
            let secs: f64 = args.get(2).and_then(|s| s.parse().ok()).unwrap_or(2.0);
            match fusee::disk::disk_check(&PathBuf::from(dir), Duration::from_secs_f64(secs)) {
                Ok(report) => {
                    if args.iter().any(|a| a == "--json") {
                        let probes: Vec<_> = report
                            .probes
                            .iter()
                            .map(|p| {
                                serde_json::json!({
                                    "method": p.method, "bytes": p.bytes, "samples": p.samples,
                                    "p50_us": p.p50_us, "p99_us": p.p99_us, "p999_us": p.p999_us,
                                    "max_us": p.max_us, "mean_us": p.mean_us
                                })
                            })
                            .collect();
                        println!(
                            "{}",
                            serde_json::json!({"dir": report.dir, "platform": report.platform, "probes": probes, "notes": report.notes})
                        );
                    } else {
                        print!("{report}");
                    }
                }
                Err(e) => {
                    eprintln!("disk-check failed: {e}");
                    std::process::exit(1);
                }
            }
        }
        Some("inspect") => {
            let Some(dir) = args.get(1) else { usage() };
            if let Err(e) = inspect(PathBuf::from(dir)) {
                eprintln!("inspect failed: {e}");
                std::process::exit(1);
            }
        }
        Some("upgrade") => {
            let Some(dir) = args.get(1) else { usage() };
            let local = fusee::LocalOptions::default();
            match fusee::upgrade::upgrade_to_shared_log(std::path::Path::new(dir), &local) {
                Ok(fusee::upgrade::Upgraded::AlreadyCurrent) => {
                    println!("{dir} already uses the shared log (log format 3)");
                }
                Ok(fusee::upgrade::Upgraded::Converted { records }) => {
                    let total: u64 = records.iter().sum();
                    println!(
                        "{dir}: converted to the shared log; {total} records copied from {} shards",
                        records.len()
                    );
                }
                Err(e) => {
                    eprintln!("upgrade failed: {e}");
                    std::process::exit(1);
                }
            }
        }
        Some("reshard") => {
            let (Some(dir), Some(shards)) =
                (args.get(1), args.get(2).and_then(|s| s.parse::<u32>().ok()))
            else {
                usage()
            };
            let opts = fusee::reshard::ReshardOptions::default();
            match fusee::reshard::reshard(Path::new(dir), shards, &opts) {
                Ok(fusee::reshard::Resharded::AlreadyCurrent { shards }) => {
                    println!("{dir} has {shards} shards");
                }
                Ok(fusee::reshard::Resharded::Done(r)) => {
                    println!(
                        "{dir}: from {} to {} shards; moved {} active, {} suspended and {} \
                         finished runs, {} unacknowledged messages and {} archived journals",
                        r.from, r.to, r.active, r.suspended, r.finished, r.messages, r.archived
                    );
                    if r.renumbered > 0 {
                        println!(
                            "{} runs got new run numbers ({} unfinished)",
                            r.renumbered,
                            r.renumbered_unfinished.len()
                        );
                    }
                    for u in &r.renumbered_unfinished {
                        println!(
                            "  {}: run {} is now run {}",
                            u.workflow_id, u.old_run, u.new_run
                        );
                        for (old, new) in &u.tokens {
                            println!("    completion token {old} is now {new}");
                        }
                    }
                }
                Err(e) => {
                    eprintln!("reshard failed: {e}");
                    std::process::exit(1);
                }
            }
        }
        Some("export") => {
            let mut sample = None;
            let mut out = None;
            let mut rest = Vec::new();
            let mut i = 1;
            while i < args.len() {
                match args[i].as_str() {
                    "--sample" => {
                        sample = args.get(i + 1).and_then(|s| s.parse::<usize>().ok());
                        i += 1;
                    }
                    "-o" | "--out" => {
                        out = args.get(i + 1).cloned();
                        i += 1;
                    }
                    other => rest.push(other.to_owned()),
                }
                i += 1;
            }
            let Some(dir) = rest.first() else { usage() };
            let id = rest.get(1).map(String::as_str);
            if id.is_none() && sample.is_none() {
                usage()
            }
            let dir = PathBuf::from(dir);
            match fusee::export::export_from_dir(&dir, id, sample.unwrap_or(1)) {
                Ok(journals) => {
                    let single = id.is_some();
                    for j in &journals {
                        let path = match (&out, single) {
                            (Some(o), true) => PathBuf::from(o),
                            (Some(o), false) => {
                                let _ = std::fs::create_dir_all(o);
                                PathBuf::from(o).join(fusee::export::file_name(&j.workflow_id))
                            }
                            (None, _) => PathBuf::from(fusee::export::file_name(&j.workflow_id)),
                        };
                        if let Err(e) = j.write(&path) {
                            eprintln!("export: {}: {e}", path.display());
                            std::process::exit(1);
                        }
                        let note = if j.is_complete() {
                            ""
                        } else {
                            " (start and result only: the engine did not keep history)"
                        };
                        println!(
                            "{}  {} records  {}{note}",
                            path.display(),
                            j.records.len(),
                            j.workflow_id
                        );
                    }
                    if journals.is_empty() {
                        eprintln!("export: no run with its whole journal was found");
                        std::process::exit(1);
                    }
                }
                Err(e) => {
                    eprintln!("export failed: {e}");
                    std::process::exit(1);
                }
            }
        }
        Some("show") => {
            let Some(file) = args.get(1) else { usage() };
            match fusee::export::ExportedJournal::read(&PathBuf::from(file)) {
                Ok(j) => {
                    println!(
                        "{} ({} v{}, {})",
                        j.workflow_id, j.workflow_type, j.version, j.status
                    );
                    for r in &j.records {
                        println!("  {}", fusee::journal::normalize_record(r));
                    }
                }
                Err(e) => {
                    eprintln!("show: {e}");
                    std::process::exit(1);
                }
            }
        }
        Some("server") => {
            let mut dir = None;
            let mut listen = "127.0.0.1:7600".to_string();
            let mut unix = None;
            let mut shards = None;
            let mut peers = None;
            let mut replica_id = None;
            let mut join = false;
            let mut timing = fusee_core::vsr::Timing::default();
            let mut flush = None;
            let mut http: Option<String> = None;
            let mut read_token_file: Option<String> = None;
            let mut namespaces_file: Option<String> = None;
            let mut client_token_file: Option<String> = None;
            let mut codec_url: Option<String> = None;
            let mut identity_header: Option<String> = None;
            let mut http_insecure = false;
            let mut flags = SecurityFlags {
                cert: None,
                key: None,
                ca: None,
                token: None,
                replica_ca: None,
                replica_token: None,
            };
            // Every option is known: a misspelt one (--token-flie) must not
            // start a server without what it asked for.
            let mut i = 1;
            while i < args.len() {
                let flag = args[i].as_str();
                match flag {
                    "--http-insecure" => {
                        http_insecure = true;
                        i += 1;
                        continue;
                    }
                    "--join" => {
                        join = true;
                        i += 1;
                        continue;
                    }
                    "--help" | "-h" => {
                        eprintln!("{SERVER_HELP}");
                        return;
                    }
                    f if !f.starts_with("--") => server_error(&format!(
                        "unexpected argument '{f}' (see fusee server --help)"
                    )),
                    _ => {}
                }
                let v = match args.get(i + 1) {
                    Some(v) if !v.starts_with("--") => v.clone(),
                    _ => server_error(&format!("{flag} needs a value")),
                };
                let number = |what: &str| -> u64 {
                    v.parse().unwrap_or_else(|_| {
                        server_error(&format!("{flag} takes {what}, not '{v}'"))
                    })
                };
                match flag {
                    "--dir" => dir = Some(v.clone()),
                    "--http" => http = Some(v.clone()),
                    "--read-token-file" => read_token_file = Some(v.clone()),
                    "--namespaces-file" => namespaces_file = Some(v.clone()),
                    "--client-token-file" => client_token_file = Some(v.clone()),
                    "--codec-url" => codec_url = Some(v.clone()),
                    "--identity-header" => identity_header = Some(v.clone()),
                    "--listen" => listen = v.clone(),
                    "--unix" => unix = Some(v.clone()),
                    "--shards" => {
                        shards = Some(
                            u32::try_from(number("a number of shards"))
                                .unwrap_or_else(|_| server_error("--shards is too large")),
                        )
                    }
                    "--replicas" => {
                        peers = Some(
                            fusee_server::replicated::parse_members(&v)
                                .unwrap_or_else(|e| server_error(&format!("--replicas: {e}"))),
                        )
                    }
                    "--heartbeat-ms" => timing.heartbeat_ms = number("milliseconds"),
                    "--election-ms" => timing.election_ms = number("milliseconds"),
                    "--replica-id" => {
                        replica_id = Some(
                            u8::try_from(number("a member ID"))
                                .unwrap_or_else(|_| server_error("--replica-id is at most 255")),
                        )
                    }
                    "--flush" => {
                        flush = Some(match v.as_str() {
                            "fsync" => fusee::FlushMethod::Fsync,
                            "fullfsync" => fusee::FlushMethod::FullFsync,
                            "fdatasync" => fusee::FlushMethod::Fdatasync,
                            "dsync" => fusee::FlushMethod::Dsync,
                            other => server_error(&format!(
                                "--flush is fullfsync, fsync, fdatasync or dsync, not '{other}'"
                            )),
                        })
                    }
                    "--tls-cert" => flags.cert = Some(v.clone()),
                    "--tls-key" => flags.key = Some(v.clone()),
                    "--tls-ca" => flags.ca = Some(v.clone()),
                    "--token-file" => flags.token = Some(v.clone()),
                    "--replica-tls-ca" => flags.replica_ca = Some(v.clone()),
                    "--replica-token-file" => flags.replica_token = Some(v.clone()),
                    // Endpoints and their options: read by `invoke_endpoints`,
                    // which refuses the ones it does not know.
                    f if f.starts_with("--invoke") => {}
                    other => server_error(&format!(
                        "unknown option '{other}' (see fusee server --help)"
                    )),
                }
                i += 2;
            }
            let Some(dir) = dir else { usage() };
            let invokers = invoke_endpoints(&args).unwrap_or_else(|e| {
                eprintln!("server: {e}");
                std::process::exit(2)
            });
            let mut local = fusee::LocalOptions::default();
            if let Some(f) = flush {
                local.flush = f;
            }
            let shards = shards.unwrap_or(if peers.is_some() { 1 } else { 4 });
            let workers = flags.workers().unwrap_or_else(|e| {
                eprintln!("server: {e}");
                std::process::exit(1)
            });
            let mut node = None;
            let server = match (peers,replica_id) {
                (None,None) => fusee_server::Server::new(fusee_server::ServerConfig {
                    dir: PathBuf::from(&dir), shards, local, security: workers.clone(),
                }),
                (Some(peers),Some(id)) if shards == 1 => match flags.replicas() {
                    Ok(security) => {
                        if !security.is_open() {
                            eprintln!("fusee server: replica protocol{}", describe(&security));
                        }
                        let cfg=fusee_server::replicated::ReplicaConfig {id,peers,join,dir:PathBuf::from(&dir),local,timing:timing.clone(),security};
                        fusee_server::replicated::ReplicaNode::start(cfg).await.and_then(|replica|{
                            let server=fusee_server::Server::replicated(&replica);
                            server.set_security(&workers)?;
                            let replica = std::sync::Arc::new(replica);
                            serve_replica_admin(&dir, replica.clone())?;
                            node=Some(replica);
                            Ok(server)
                        })
                    }
                    Err(e) => Err(e),
                },
                _ => Err(std::io::Error::other("replication requires --replicas and --replica-id, and exactly one shard per group")),
            }.unwrap_or_else(|e|{eprintln!("server: {e}");std::process::exit(1)});
            let scoped = flags
                .scoped(client_token_file.as_deref(), read_token_file.as_deref())
                .and_then(|scoped| server.set_security_scoped(&workers, scoped));
            if let Err(e) = scoped {
                eprintln!("server: {e}");
                std::process::exit(1);
            }
            let tcp = match tokio::net::TcpListener::bind(&listen).await {
                Ok(l) => l,
                Err(e) => {
                    eprintln!("server: cannot listen on {listen}: {e}");
                    std::process::exit(1);
                }
            };
            eprintln!(
                "fusee server: {shards} shards in {dir}, listening on {listen}{}",
                describe(&workers)
            );
            if let Some(path) = unix {
                let _ = std::fs::remove_file(&path);
                match tokio::net::UnixListener::bind(&path) {
                    Ok(l) => {
                        let token = if workers.token.is_some() {
                            " (token)"
                        } else {
                            ""
                        };
                        eprintln!("fusee server: also listening on unix:{path}{token}");
                        tokio::spawn(fusee_server::server::serve_unix(server.clone(), l));
                    }
                    Err(e) => eprintln!("server: cannot listen on {path}: {e}"),
                }
            }
            let mut http_reload: Option<(
                HttpFiles,
                tokio::sync::mpsc::UnboundedSender<fusee::http::HttpOptions>,
            )> = None;
            if let Some(addr) = &http {
                // The HTTP API relays to lease holders through the server
                // itself, as a client's calls do, over a connection inside
                // this process (no TLS or token to pass) that is opened
                // again whenever the server closes it.
                let calls = fusee_server::client::LocalCalls::new(&server);
                let files = HttpFiles {
                    token: flags.token.clone(),
                    client_token: client_token_file.clone(),
                    read_token: read_token_file.clone(),
                    namespaces: namespaces_file.clone(),
                    codec_url: codec_url.clone(),
                    identity_header: identity_header.clone(),
                    insecure: http_insecure,
                };
                let opts = files.options().unwrap_or_else(|e| {
                    eprintln!("server: --http: {e}");
                    std::process::exit(2)
                });
                let (tx, mut reloads) =
                    tokio::sync::mpsc::unbounded_channel::<fusee::http::HttpOptions>();
                http_reload = Some((files, tx));
                let addr = addr.clone();
                tokio::spawn(async move {
                    match fusee::http::serve(calls, &addr, opts).await {
                        Ok(s) => {
                            eprintln!("fusee server: HTTP API and UI at {}", s.url());
                            while let Some(o) = reloads.recv().await {
                                if let Err(e) = s.reload(o) {
                                    eprintln!("fusee server: HTTP tokens not reloaded: {e}");
                                }
                            }
                            std::future::pending::<()>().await;
                        }
                        Err(e) => eprintln!("server: --http: {e}"),
                    }
                });
            }
            // SIGHUP reloads the listener's certificates and tokens, and the
            // HTTP API's tokens, namespaces and codec URL, for connections
            // made after it. A file that does not load keeps the old
            // settings. The replica protocol's settings need a restart.
            {
                let (server, flags) = (server.clone(), flags.clone());
                let (client, read) = (client_token_file.clone(), read_token_file.clone());
                tokio::spawn(async move {
                    let Ok(mut hup) =
                        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::hangup())
                    else {
                        return;
                    };
                    while hup.recv().await.is_some() {
                        let reloaded = flags.workers().and_then(|w| {
                            let scoped = flags.scoped(client.as_deref(), read.as_deref())?;
                            server.set_security_scoped(&w, scoped)
                        });
                        let http = match &http_reload {
                            Some((files, tx)) => files
                                .options()
                                .map(|o| {
                                    let _ = tx.send(o);
                                })
                                .map_err(std::io::Error::other),
                            None => Ok(()),
                        };
                        match reloaded.and(http) {
                            Ok(()) => eprintln!("fusee server: reloaded certificates and tokens"),
                            Err(e) => eprintln!("fusee server: kept the old settings: {e}"),
                        }
                    }
                });
            }
            if !invokers.is_empty() {
                let n = invokers.len();
                if let Err(e) = server.set_invokers(invokers) {
                    eprintln!("server: --invoke: {e}");
                    std::process::exit(1);
                }
                eprintln!(
                    "fusee server: invoking workers at {n} endpoint(s) for shards without one"
                );
            }
            // SIGTERM (systemd, `docker stop`, Kubernetes) stops it as SIGINT
            // does. As PID 1 in a container, a signal without a handler is
            // ignored, and the stop would wait for SIGKILL.
            let mut term =
                tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                    .unwrap_or_else(|e| server_error(&format!("cannot handle SIGTERM: {e}")));
            tokio::select! {
                r = fusee_server::server::serve_tcp(server, tcp) => {
                    if let Err(e) = r {
                        eprintln!("server: {e}");
                        std::process::exit(1);
                    }
                }
                _ = tokio::signal::ctrl_c() => eprintln!("fusee server: shutting down"),
                _ = term.recv() => eprintln!("fusee server: shutting down"),
                _ = async {
                    if let Some(node) = &node {
                        node.observer().wait_stopped().await;
                    } else {
                        std::future::pending::<()>().await;
                    }
                } => {eprintln!("replica stopped after a storage or protocol error");std::process::exit(1);}
            }
        }
        Some("restore") => {
            if let Err(e) = restore_cmd(&args) {
                eprintln!("restore failed: {e}");
                std::process::exit(1);
            }
        }
        Some("backups") => {
            if let Err(e) = backups_cmd(&args) {
                eprintln!("backups: {e}");
                std::process::exit(1);
            }
        }
        Some("backup") => {
            if let Err(e) = backup_cmd(&args).await {
                eprintln!("backup failed: {e}");
                std::process::exit(1);
            }
        }
        Some("replicas") => replicas_command(&args[1..]).await,
        Some("version") => println!("fusee {}", env!("CARGO_PKG_VERSION")),
        _ => usage(),
    }
}
