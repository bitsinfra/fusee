//! A server connection or elected replica prepared off the host language's
//! event loop. The foreign shard runtimes themselves stay on that loop.
use crate::remote::{RelayedCall, Remote};
use crate::replicated::{ReplicaConfig, ReplicaNode, ReplicaObserver};
use crate::security::Security;
use fusee::Config;
use fusee::env::Notify;
use std::io;
use std::path::PathBuf;
use std::sync::{Arc, OnceLock};
use tokio::runtime::Runtime;
use tokio::sync::mpsc::Receiver;

/// A member can report its counters while its application is waiting to
/// become the primary. Allocate one handle per replica start.
#[derive(Clone, Default)]
pub struct ReplicaMetrics(Arc<OnceLock<ReplicaObserver>>);
impl ReplicaMetrics {
    pub fn snapshot(&self) -> Option<serde_json::Value> {
        let (s, sent, received) = self.0.get()?.snapshot();
        Some(
            serde_json::json!({"view":s.view,"primary":s.primary,"ready":s.ready,"stopped":s.stopped,
            "op":s.op,"commit":s.commit,"applied":s.applied,"sentBytes":sent,"receivedBytes":received}),
        )
    }
}

pub struct ForeignConnection {
    pub config: Config,
    link: Link,
}

enum Link {
    Server {
        remote: Remote,
        calls: Receiver<RelayedCall>,
    },
    Replica {
        node: ReplicaNode,
        runtime: Option<Runtime>,
        epoch: u64,
    },
}

impl ForeignConnection {
    pub fn prepare(
        mode: &str,
        address: Option<String>,
        id: Option<u8>,
        peers: Option<Vec<String>>,
        directory: Option<String>,
    ) -> io::Result<Self> {
        Self::prepare_with(mode, address, id, peers, directory, None, None)
    }

    /// `address` is the server's address in server mode. In replicated mode
    /// it is optional and carries only this member's connection options, as
    /// `?cert=PATH&key=PATH&ca=PATH&token-file=PATH`
    /// ([`Security::from_options`](crate::security::Security::from_options)).
    pub fn prepare_with(
        mode: &str,
        address: Option<String>,
        id: Option<u8>,
        peers: Option<Vec<String>>,
        directory: Option<String>,
        flush: Option<String>,
        metrics: Option<ReplicaMetrics>,
    ) -> io::Result<Self> {
        let mut local = fusee::LocalOptions::default();
        if let Some(method) = flush {
            local.flush = match method.as_str() {
                "fsync" => fusee::FlushMethod::Fsync,
                "fdatasync" => fusee::FlushMethod::Fdatasync,
                "fullfsync" => fusee::FlushMethod::FullFsync,
                "dsync" => fusee::FlushMethod::Dsync,
                _ => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "unknown flush method",
                    ));
                }
            };
        }
        let missing = |msg: &str| io::Error::new(io::ErrorKind::InvalidInput, msg.to_owned());
        match mode {
            "server" => Self::server(
                &address
                    .filter(|s| !s.is_empty())
                    .ok_or_else(|| missing("server mode requires an address"))?,
            ),
            "replicated" => Self::replica_with(
                id.ok_or_else(|| missing("replicated mode requires a replica ID"))?,
                peers.ok_or_else(|| missing("replicated mode requires peer addresses"))?,
                PathBuf::from(
                    directory
                        .ok_or_else(|| missing("replicated mode requires a data directory"))?,
                ),
                local,
                metrics,
                replica_security(address.as_deref())?,
            ),
            _ => Err(missing("unsupported remote durability")),
        }
    }

    pub fn next_call_json(&mut self) -> io::Result<Option<String>> {
        self.next_call().map(|c| {
            let args: serde_json::Value = serde_json::from_slice(&c.args)?;
            Ok(serde_json::json!({"relay":c.relay.to_string(), "method":c.method, "args":args}).to_string())
        }).transpose()
    }

    /// May wait for the previous worker to relinquish its lease.
    pub fn server(addr: &str) -> io::Result<Self> {
        let (config, remote) = crate::remote_worker(addr)?;
        let calls = remote.take_calls();
        Ok(Self {
            config,
            link: Link::Server { remote, calls },
        })
    }

    /// Every peer runs the protocol while its SDK waits to become primary.
    pub fn replica(id: u8, peers: Vec<String>, dir: PathBuf) -> io::Result<Self> {
        Self::replica_with(
            id,
            peers,
            dir,
            Default::default(),
            None,
            Security::default(),
        )
    }
    fn replica_with(
        id: u8,
        peers: Vec<String>,
        dir: PathBuf,
        local: fusee::LocalOptions,
        metrics: Option<ReplicaMetrics>,
        security: Security,
    ) -> io::Result<Self> {
        if !matches!(peers.len(), 3 | 5) || usize::from(id) >= peers.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "expected three or five peers and a member ID",
            ));
        }
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()?;
        let mut node = runtime.block_on(ReplicaNode::start(ReplicaConfig {
            id,
            peers,
            join: false,
            dir,
            local,
            timing: Default::default(),
            security,
        }))?;
        if let Some(metrics) = metrics {
            metrics
                .0
                .set(node.observer())
                .map_err(|_| io::Error::other("metrics handle is already observing a replica"))?;
        }
        // A group that is still forming can hand the role on within a few
        // election timeouts: open the engine once this replica has stayed
        // the ready primary of one view for 150 ms.
        let status = runtime.block_on(async {
            loop {
                node.wait_ready().await;
                let status = node.status();
                if !status.ready {
                    return status;
                }
                tokio::time::sleep(std::time::Duration::from_millis(150)).await;
                let now = node.status();
                if now.ready && now.view == status.view {
                    return now;
                }
            }
        });
        if !status.ready {
            return Err(io::Error::other(
                "replica stopped before acquiring the primary role",
            ));
        }
        let config = node.engine_config();
        Ok(Self {
            config,
            link: Link::Replica {
                node,
                runtime: Some(runtime),
                epoch: status.view,
            },
        })
    }

    pub fn set_notify(&self, notify: Notify) {
        match &self.link {
            Link::Server { remote, .. } => remote.set_call_notify(notify),
            Link::Replica {
                node,
                runtime: Some(rt),
                epoch,
            } => {
                let mut status = node.subscribe();
                let view = *epoch;
                rt.spawn(async move {
                    let _ = status.wait_for(|s| !s.ready || s.view != view).await;
                    notify();
                });
            }
            Link::Replica { .. } => {}
        }
    }

    pub fn check(&self) -> io::Result<()> {
        if let Link::Server { remote, .. } = &self.link {
            return remote.healthy();
        }
        if let Link::Replica {
            node,
            epoch,
            runtime,
        } = &self.link
        {
            let status = node.status();
            if !status.ready || status.view != *epoch {
                return Err(io::Error::other("replica lost its primary role"));
            }
            // Local reads are served under the read lease. Without one,
            // confirm through the protocol (a round trip) or refuse.
            if !node.lease_valid()
                && let Some(rt) = runtime
            {
                let reads = node.reads();
                let (tx, rx) = std::sync::mpsc::channel();
                rt.spawn(async move {
                    let _ = tx.send(reads.confirm_reads().await);
                });
                return rx
                    .recv_timeout(std::time::Duration::from_secs(2))
                    .map_err(|_| {
                        io::Error::new(io::ErrorKind::TimedOut, "replica quorum is unavailable")
                    })?;
            }
        }
        Ok(())
    }

    pub fn next_call(&mut self) -> Option<RelayedCall> {
        match &mut self.link {
            Link::Server { calls, .. } => calls.try_recv().ok(),
            Link::Replica { .. } => None,
        }
    }

    /// An invoked worker ([`crate::invoke`]): `running`, `idle`, `expired`
    /// or `lost` (see [`crate::invoke::Progress`]); `None` for any other
    /// connection. `awaiting`: workflow types the host has runs of and no
    /// code for. Poll every few milliseconds; once it is not `running`,
    /// close the engine and drop this connection, which reports to the
    /// server and leaves.
    pub fn invocation_state(&self, awaiting: &[String]) -> Option<&'static str> {
        match &self.link {
            Link::Server { remote, .. } if remote.invocation().is_some() => {
                Some(remote.invocation_poll(awaiting).as_str())
            }
            _ => None,
        }
    }

    pub fn reply(&self, relay: u64, payload: Vec<u8>) -> io::Result<()> {
        match &self.link {
            Link::Server { remote, .. } => remote.inner().reply_call(relay, Ok(payload)),
            Link::Replica { .. } => Err(io::Error::other("replica has no server call to answer")),
        }
    }
}

/// A replicated member's connection options from the `address` argument.
fn replica_security(options: Option<&str>) -> io::Result<Security> {
    match options.filter(|s| !s.is_empty()) {
        None => Ok(Security::default()),
        Some(options) => match options.strip_prefix('?') {
            Some(options) => Security::from_options(options),
            None => Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "in replicated mode the address holds only connection options, like ?cert=...&key=...&ca=...&token-file=...",
            )),
        },
    }
}

impl Drop for ForeignConnection {
    fn drop(&mut self) {
        // An invoked worker reports what its closed shards have left.
        if let Link::Server { remote, .. } = &self.link {
            remote.finish_invocation();
        }
        if let Link::Replica { runtime, .. } = &mut self.link
            && let Some(runtime) = runtime.take()
        {
            runtime.shutdown_background();
        }
    }
}
