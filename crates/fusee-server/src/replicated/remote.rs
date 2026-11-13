//! Storage and quorum permit sessions for a worker connected to a dedicated primary.
use super::*;

#[derive(Clone)]
pub(crate) struct ReplicaStorage {
    shared: Arc<Shared>,
}

impl ReplicaStorage {
    pub(crate) fn new(node: &ReplicaNode) -> Self {
        Self {
            shared: node.shared.clone(),
        }
    }

    pub(crate) fn status(&self) -> watch::Receiver<NodeStatus> {
        self.shared.status.subscribe()
    }

    fn epoch(&self) -> io::Result<u64> {
        let epoch = self.shared.epoch.load(Ordering::Acquire);
        if self.shared.status.borrow().ready {
            Ok(epoch)
        } else {
            Err(other("replica is not the ready primary"))
        }
    }

    /// A segment group's only shard (format 2 batches, materialized).
    pub(crate) fn open(&self, shard: u32, epoch: u64) -> io::Result<ReplicatedEnv> {
        if shard != 0 || self.epoch()? != epoch {
            return Err(other("replica cannot open this shard or epoch"));
        }
        if let Kind::Log(n) = self.shared.decide(Kind::Segments) {
            return Err(other(format!(
                "this group's log holds the {n} shards of an embedded engine; open it with \
                 ReplicaNode::engine_config, not as remote storage of a dedicated server"
            )));
        }
        let dir = self.shard_dir(0)?;
        let worker_lock = fusee::local::lock_directory(&dir)?;
        let local = LocalEnv::open(&dir, self.shared.local.clone())?;
        Ok(ReplicatedEnv::new(
            local,
            self.shared.clone(),
            epoch,
            Mode::Segments {
                slot: Arc::new(Mutex::new(Slot::default())),
                inflight: false,
            },
            dir,
            worker_lock,
        ))
    }

    /// Shard `shard` of a log group of `shards` (log format 3 sections).
    pub(crate) fn open_log(
        &self,
        shard: u32,
        shards: u32,
        epoch: u64,
    ) -> io::Result<ReplicatedEnv> {
        if shard >= shards || self.epoch()? != epoch {
            return Err(other("replica cannot open this shard or epoch"));
        }
        // The committed log as of now: an engine reopened after a clean
        // shutdown in the same primary epoch sees what the last one wrote.
        let (tx, rx) = std::sync::mpsc::sync_channel(1);
        self.shared
            .barriers
            .send(Barrier::Recovery { epoch, reply: tx })
            .map_err(|_| other("replica stopped"))?;
        let recovery = rx
            .recv()
            .map_err(|_| other("replica stopped before the engine opened"))??;
        let dir = self.shard_dir(shard)?;
        let worker_lock = fusee::local::lock_directory(&dir)?;
        let local = LocalEnv::open(&dir, self.shared.local.clone())?;
        Ok(ReplicatedEnv::new(
            local,
            self.shared.clone(),
            epoch,
            Mode::Log {
                index: shard,
                slot: Arc::new(log3::ShardSlot::default()),
                want: None,
                reported: 0,
                recovery: Some(recovery),
                resumed_ns: 0,
            },
            dir,
            worker_lock,
        ))
    }

    /// A standby shard of a log group: opened on any member, it waits for
    /// this replica to become the ready primary (see
    /// [`ReplicaNode::standby_config`]). Its run store and checkpoints are
    /// opened now; the committed log is taken at promotion.
    pub(super) fn open_standby(&self, shard: u32, standby: Standby) -> io::Result<ReplicatedEnv> {
        if shard >= standby.shards {
            return Err(other("replica cannot open this shard"));
        }
        if matches!(self.shared.kind(), Kind::Segments) {
            return Err(other(
                "a replicated segment group has no standby engine; open it with engine_config",
            ));
        }
        let dir = self.shard_dir(shard)?;
        let worker_lock = fusee::local::lock_directory(&dir)?;
        let local = LocalEnv::open(&dir, self.shared.local.clone())?;
        let mut env = ReplicatedEnv::new(
            local,
            self.shared.clone(),
            0,
            Mode::Log {
                index: shard,
                slot: Arc::new(log3::ShardSlot::default()),
                want: None,
                reported: 0,
                recovery: None,
                resumed_ns: 0,
            },
            dir,
            worker_lock,
        );
        env.standby = Some(standby);
        Ok(env)
    }

    fn shard_dir(&self, shard: u32) -> io::Result<PathBuf> {
        let dir = self
            .shared
            .engine_dir
            .lock()
            .map_err(|_| other("poisoned engine directory"))?
            .join(format!("shard-{shard:03}"));
        std::fs::create_dir_all(&dir)?;
        Ok(dir)
    }

    fn proposal(
        &self,
        epoch: u64,
        request: leases::Request,
        reply: impl FnOnce(io::Result<u64>) + Send + 'static,
    ) -> Proposal {
        Proposal {
            queued: self.shared.traffic.timings.start(),
            body: Arc::from([]),
            last_lsn: 0,
            epoch,
            control: Some(request),
            slot: Arc::new(Mutex::new(Slot {
                callback: Some(Box::new(reply)),
                ..Slot::default()
            })),
        }
    }

    /// For synchronous callers: waits on this thread while the queue is full.
    fn request(
        &self,
        epoch: u64,
        request: leases::Request,
        reply: impl FnOnce(io::Result<u64>) + Send + 'static,
    ) -> io::Result<()> {
        self.shared
            .proposals
            .send(self.proposal(epoch, request, reply))
            .map_err(|_| other("replica stopped"))
    }

    /// Proposes and waits for the committed answer without blocking the runtime.
    async fn ask(
        &self,
        epoch: u64,
        request: leases::Request,
        stopped: &'static str,
    ) -> io::Result<u64> {
        let (tx, rx) = oneshot::channel();
        let proposal = self.proposal(epoch, request, move |result| {
            let _ = tx.send(result);
        });
        self.shared
            .proposals
            .send_async(proposal)
            .await
            .map_err(|_| other("replica stopped"))?;
        rx.await.map_err(|_| other(stopped))?
    }

    /// Whether this replica is the ready primary and holds a read lease:
    /// no other member can lead a newer view before it lapses.
    pub(crate) fn lease_valid(&self) -> bool {
        self.shared.status.borrow().ready
            && now_ms() < self.shared.lease_until.load(Ordering::Acquire)
    }

    /// Returns once this primary has shown it still leads the group: at
    /// once under a read lease, else after a no-op commits through a quorum.
    /// A read of this replica's engine started after it returns sees every
    /// write the group acknowledged before the call.
    pub(crate) async fn confirm_quorum(&self) -> io::Result<()> {
        if self.lease_valid() {
            return Ok(());
        }
        self.probe().await
    }

    /// Commits a no-op through a quorum, lease or not.
    pub(crate) async fn probe(&self) -> io::Result<()> {
        self.ask(
            self.epoch()?,
            leases::Request::Probe,
            "primary stopped before quorum confirmation",
        )
        .await?;
        Ok(())
    }
    pub(crate) async fn claim(&self) -> io::Result<(WorkerSession, ReplicatedEnv)> {
        let epoch = self.epoch()?;
        let env = self.open(0, epoch)?;
        let session = self
            .ask(
                epoch,
                leases::Request::Claim,
                "replica stopped before worker claim",
            )
            .await?;
        Ok((
            WorkerSession {
                storage: self.clone(),
                epoch,
                session,
            },
            env,
        ))
    }
}

pub(crate) struct WorkerSession {
    storage: ReplicaStorage,
    epoch: u64,
    session: Op,
}

impl WorkerSession {
    pub(crate) fn request_limit(
        &self,
        operation: u8,
        name: String,
        ticket: u64,
        concurrent: u32,
        per_second: f64,
        priority: fusee::limits::Priority,
        tx: mpsc::UnboundedSender<io::Result<Vec<u8>>>,
    ) -> io::Result<()> {
        use crate::limits::op;
        match operation {
            op::DEFINE => fusee::limits::LimitSpec {
                concurrent,
                per_second,
            }
            .validate()
            .map_err(other)?,
            op::ACQUIRE | op::RELEASE if ticket > 0 => {}
            op::RENEW => {}
            _ => return Err(other("invalid replicated limit operation or ticket")),
        }
        let per_second = if operation == op::DEFINE {
            per_second
        } else {
            0.0
        };
        let reply = move |result: io::Result<u64>| {
            let result = result.map(|answer| {
                let mut frame = Vec::new();
                if operation == crate::limits::op::ACQUIRE && answer != 0 {
                    use fusee::limits::answer::{Answer, decode};
                    let (granted, delay) = match decode(ticket, answer) {
                        Answer::Granted(t) => (Some(t), 0),
                        Answer::RetryAfter(d) => (None, d),
                        Answer::Queued => (None, 0),
                    };
                    let mut b = Vec::new();
                    put_u64(&mut b, granted.unwrap_or(ticket));
                    b.push(u8::from(granted.is_some()));
                    put_u64(
                        &mut b,
                        if delay > 0 {
                            fusee::local::unix_ms().saturating_add(delay)
                        } else {
                            0
                        },
                    );
                    put_u64(&mut b, delay);
                    frame::encode(proto::resp::LIMITED, &b, &mut frame);
                }
                frame
            });
            if !matches!(&result,Ok(bytes) if bytes.is_empty()) {
                let _ = tx.send(result);
            }
        };
        let command = leases::Command::new(operation, name, ticket, concurrent, per_second)
            .with_priority(priority);
        self.storage.request(
            self.epoch,
            leases::Request::Worker(self.session, command),
            reply,
        )
    }
}

impl ReplicatedEnv {
    pub(crate) fn local(&mut self) -> &mut LocalEnv {
        &mut self.local
    }

    /// A standby shard: waits up to `timeout` for this replica to become
    /// the ready primary, then takes the committed log batches from the
    /// I/O thread's memory and runs in that primary epoch. `Ok(false)`: not
    /// yet. An error ends the shard: the replica stopped, or the engine
    /// cannot run (it ran in an earlier epoch, or the group's log holds
    /// another engine).
    pub(super) fn wait_promotion(&mut self, timeout: Duration) -> io::Result<bool> {
        let Some(standby) = &self.standby else {
            return Ok(true);
        };
        let deadline = Instant::now() + timeout;
        let epoch = {
            let mut guard = self.shared.role.0.lock().unwrap_or_else(|e| e.into_inner());
            loop {
                // Read under the lock the status sender notifies under, so a
                // promotion between the read and the wait is not missed.
                let s = *self.shared.status.borrow();
                if s.stopped || s.retired {
                    return Err(other("replica stopped"));
                }
                if s.ready {
                    break self.shared.epoch.load(Ordering::Acquire);
                }
                let left = deadline.saturating_duration_since(Instant::now());
                if left.is_zero() {
                    return Ok(false);
                }
                guard = self
                    .shared
                    .role
                    .1
                    .wait_timeout(guard, left)
                    .unwrap_or_else(|e| e.into_inner())
                    .0;
            }
        };
        // One primary epoch for the whole engine, its permits included.
        if let Err(was) =
            standby
                .tenure
                .compare_exchange(0, epoch, Ordering::AcqRel, Ordering::Acquire)
            && was != epoch
        {
            return Err(other(
                "this standby engine ran in an earlier primary epoch; open another",
            ));
        }
        match self.shared.decide(Kind::Log(standby.shards)) {
            Kind::Log(n) if n == standby.shards => {}
            _ => {
                return Err(other(
                    "the group's log was written by an engine with another layout or shard count",
                ));
            }
        }
        let (tx, rx) = std::sync::mpsc::sync_channel(1);
        self.shared
            .barriers
            .send(Barrier::Recovery { epoch, reply: tx })
            .map_err(|_| other("replica stopped"))?;
        let recovery = rx
            .recv()
            .map_err(|_| other("replica stopped before the shard started"))??;
        // A checkpoint image installed while this shard stood by moved the
        // engine directory: open the new one.
        let Mode::Log { index, .. } = self.mode else {
            return Err(other("a replicated segment group's shard has no standby"));
        };
        let dir = ReplicaStorage {
            shared: self.shared.clone(),
        }
        .shard_dir(index)?;
        if dir != self.shard {
            self._worker_lock = fusee::local::lock_directory(&dir)?;
            self.local = LocalEnv::open(&dir, self.shared.local.clone())?;
            self.shard = dir;
        }
        if let Mode::Log { recovery: r, .. } = &mut self.mode {
            *r = Some(recovery);
        }
        self.epoch = epoch;
        self.standby = None;
        Ok(true)
    }
}
