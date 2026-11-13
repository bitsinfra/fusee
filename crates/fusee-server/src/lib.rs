#![doc(html_no_source)]

//! Fusee server mode.
//!
//! The same engine runs as `fusee server`: the server holds each shard's
//! log, run store and checkpoints, and workflow code still runs in the
//! application. The application opens an [`Engine`](fusee::Engine) with
//! [`remote_config`]; its shard runtimes use [`remote::RemoteEnv`], so each
//! flush is one pipelined round trip to the server plus the server's own
//! flush.
//!
//! The server keeps one log for every shard and leases the shards one at a
//! time, each with its own epoch. A worker takes every free shard, or at
//! most `max-shards=N` of them, and waits as a standby for the rest; when a
//! holder goes away its shards go to waiting workers with higher epochs,
//! which recover them from the server's disk and resume their workflows.
//! Requests carrying a stale epoch are refused, so a deposed worker can
//! never append again.

pub mod client;
mod endpoint;
pub mod foreign;
pub mod invoke;
pub mod limits;
pub mod proto;
pub mod remote;
pub mod replicated;
pub mod security;
pub mod server;
mod shm;
mod spin;

use fusee::Config;
use std::sync::Arc;

/// An engine configuration whose storage is the server at `addr`
/// (`host:port`, `unix:/path/to.sock` or `replicas://...`, optionally
/// followed by connection options such as `?ca=ca.pem&token-file=token`; see
/// [`security::Security::from_options`]). Blocks until the lease is held.
pub fn remote_config(addr: &str) -> std::io::Result<Config> {
    Ok(remote_worker(addr)?.0)
}

/// Like [`remote_config`], also returning the connection, so the worker can
/// serve calls other machines send through the server
/// ([`client::serve_calls`]).
pub fn remote_worker(addr: &str) -> std::io::Result<(Config, remote::Remote)> {
    worker(remote::Remote::connect(addr)?)
}

/// Like [`remote_worker`], with TLS and the token given as [`security::Security`]
/// instead of address options.
pub fn remote_worker_with(
    addr: &str,
    security: &security::Security,
) -> std::io::Result<(Config, remote::Remote)> {
    worker(remote::Remote::connect_with(addr, security)?)
}

fn worker(remote: remote::Remote) -> std::io::Result<(Config, remote::Remote)> {
    let handle = remote.clone();
    let limits: Arc<dyn fusee::limits::LimitBackend> = remote.limits();
    let shards = remote.shards();
    let factory: fusee::engine::EnvFactory = Arc::new(move |shard| {
        let env: Box<dyn fusee::env::Env> = Box::new(remote.env(shard));
        Ok(env)
    });
    let mut cfg = Config::memory();
    cfg.durability = fusee::Durability::Custom(factory);
    cfg.shards = Some(shards);
    // Parking writes go to the server; keep them modest.
    cfg.park_after = std::time::Duration::from_secs(5);
    // Limits live on the server; permits are leases.
    cfg.limits = Some(limits);
    Ok((cfg, handle))
}

pub use client::{Client, ClientStream, serve_calls};
pub use server::{Liveness, Server, ServerConfig};
