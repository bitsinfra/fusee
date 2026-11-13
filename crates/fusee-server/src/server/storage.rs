//! A remote worker reads the primary's local materialization. Its log flushes
//! go through the selected durability backend.
use crate::replicated::{ReplicaStorage, ReplicatedEnv};
use fusee::env::Env;
use fusee::local::LocalEnv;
use std::ops::{Deref, DerefMut};

pub(super) enum WorkerDisk {
    Local(Box<LocalEnv>),
    Replicated(Box<ReplicatedEnv>),
}

impl WorkerDisk {
    pub(super) fn files(&mut self) -> &mut LocalEnv {
        match self {
            Self::Local(env) => env,
            Self::Replicated(env) => env.local(),
        }
    }
}

impl Deref for WorkerDisk {
    type Target = dyn Env;
    fn deref(&self) -> &Self::Target {
        match self {
            Self::Local(env) => &**env,
            Self::Replicated(env) => &**env,
        }
    }
}

impl DerefMut for WorkerDisk {
    fn deref_mut(&mut self) -> &mut Self::Target {
        match self {
            Self::Local(env) => &mut **env,
            Self::Replicated(env) => &mut **env,
        }
    }
}

/// One quorum check per group guards all connected workers and clients. A
/// primary isolated from every backup cannot keep serving an idle socket.
pub(super) struct Dedicated {
    pub replica: ReplicaStorage,
    pub fence: tokio::sync::watch::Sender<u64>,
    guard: tokio::task::JoinHandle<()>,
}

impl Dedicated {
    pub fn new(replica: ReplicaStorage) -> Self {
        use std::time::Duration;
        let (fence, _) = tokio::sync::watch::channel(0u64);
        let guard = tokio::spawn({
            let replica = replica.clone();
            let fence = fence.clone();
            async move {
                let mut tick = tokio::time::interval(Duration::from_secs(1));
                tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                loop {
                    tick.tick().await;
                    if fence.receiver_count() == 0 || !replica.status().borrow().ready {
                        continue;
                    }
                    if !matches!(
                        tokio::time::timeout(Duration::from_secs(2), replica.confirm_quorum())
                            .await,
                        Ok(Ok(()))
                    ) {
                        fence.send_modify(|generation| *generation = generation.wrapping_add(1));
                    }
                }
            }
        });
        Self {
            replica,
            fence,
            guard,
        }
    }
}

impl Drop for Dedicated {
    fn drop(&mut self) {
        self.guard.abort();
    }
}
