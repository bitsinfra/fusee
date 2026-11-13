//! Checkpoint transfer on the existing framed peer streams. A receiver pulls
//! one page at a time and stays on its old generation while bytes are missing.
use super::generation::Generation;
use super::snapshot::{Boundary, Image, PAGE, Sources};
use super::*;
use fusee_core::vsr::Msg;
use serde::{Deserialize, Serialize};
use std::fs;
use std::thread::{self, JoinHandle};

const WIRE: u8 = 0xfa;

/// How often the I/O thread checks whether to build a checkpoint image
/// when nothing it reads has signalled a change.
const CAPTURE_POLL_MS: u64 = 25;

/// After publishing a generation, a member captures the next one only once
/// this long has passed, unless its protocol log has outgrown
/// [`CAPTURE_LOG_CAP_EXTENTS`]. Publishing holds the I/O thread for several
/// syncs; at the 1,024-entry threshold alone a busy group would publish
/// about ten times a second, and each publication delays every replicated
/// step behind it.
const CAPTURE_MIN_INTERVAL_MS: u64 = 1_000;
const CAPTURE_LOG_CAP_EXTENTS: u64 = 64;

/// The image bytes a group publishes per second at most, on average
/// (`Checkpoints::cadence`): each is written once here and sent to and
/// unpacked on every backup.
const CAPTURE_IMAGE_BYTES_PER_S: u64 = 16 << 20;

/// Backups that hold a received checkpoint's base install it this far
/// apart, in the order of their IDs: each installation holds its I/O
/// thread for the generation's syncs (about a millisecond on NVMe), and
/// two backups installing together would leave the primary without a
/// quorum for that long.
const INSTALL_STAGGER_MS: u64 = 200;

/// A configured member that has neither sent a frame nor accepted this
/// replica's connection for this long does not hold back the first
/// checkpoint. See [`Checkpoints::holds_back`].
#[cfg(not(test))]
pub(super) const ABSENT_MS: u64 = 30_000;
#[cfg(test)]
pub(super) const ABSENT_MS: u64 = 1_500;

#[derive(Serialize, Deserialize)]
enum Wire {
    Hello {
        base: Op,
        #[serde(default)]
        empty_engine: bool,
    },
    Offer {
        view: u64,
        commit: Op,
        image: Image,
    },
    Get {
        view: u64,
        digest: [u8; 32],
        offset: u64,
    },
    Unavailable {
        view: u64,
        digest: [u8; 32],
    },
    Page {
        view: u64,
        digest: [u8; 32],
        offset: u64,
    },
    GetTail {
        view: u64,
        digest: [u8; 32],
        first: Op,
        last: Op,
    },
    Tail {
        view: u64,
        digest: [u8; 32],
    },
}

impl Wire {
    fn encode(&self, data: &[u8]) -> io::Result<Vec<u8>> {
        let meta = serde_json::to_vec(self)?;
        let mut bytes = Vec::with_capacity(meta.len() + data.len() + 3);
        bytes.push(WIRE);
        bytes.extend_from_slice(&u16::try_from(meta.len()).map_err(other)?.to_le_bytes());
        bytes.extend(meta);
        bytes.extend_from_slice(data);
        Ok(bytes)
    }

    fn decode(bytes: &[u8]) -> Option<(Self, &[u8])> {
        let size = u16::from_le_bytes(bytes.get(1..3)?.try_into().ok()?) as usize;
        let header = bytes.get(3..3 + size)?;
        Some((serde_json::from_slice(header).ok()?, &bytes[3 + size..]))
    }
}

struct Built {
    epoch: u64,
    image: Image,
    ledger: Vec<u8>,
}

/// A generation [`Generation::stage`] writes in the background. Writing
/// the log tail, superblock and lease state of a new generation took 13 to
/// 21 ms; on the I/O thread it held every replicated step behind it (and a
/// backup installs each of its primary's checkpoints). Only the catch-up
/// and the pointer switch run on the I/O thread now.
struct Staging {
    job: JoinHandle<io::Result<Staged>>,
    g: Generation,
    /// The view the tail was taken in: a view change discards the staging.
    view: u64,
    kind: StagedFor,
}

/// Where a staged generation's tail comes from.
enum TailSource {
    /// Entries the I/O thread holds (a tail fetched from peers).
    Entries(Vec<Entry>),
    /// This member's own protocol log file, the entries after `after`
    /// (op, checksum) in its first `end` bytes ([`VsrLog::read_tail`]):
    /// copying them on the I/O thread took milliseconds a second at
    /// 250,000 steps/s.
    File {
        path: PathBuf,
        after: (u64, u64),
        end: u64,
    },
}

/// What the staging thread hands back.
struct Staged {
    /// The new generation's log, holding `tail`.
    log: VsrLog,
    tail: Vec<Entry>,
    /// The last entry the staged log holds: `(op, checksum)`.
    last: (u64, u64),
    /// A received tail as the replica's log (built off the I/O thread), and
    /// whether its chain was intact.
    map: Option<(std::collections::BTreeMap<u64, Entry>, bool)>,
}

/// Frees `value` on a thread of its own: dropping a protocol log of
/// thousands of entries took milliseconds on the I/O thread.
fn drop_elsewhere<T: Send + 'static>(value: T) {
    let _ = thread::Builder::new()
        .name("fusee-drop".into())
        .spawn(move || drop(value));
}

enum StagedFor {
    /// This member's own checkpoint (`activate_local`).
    Local { epoch: u64, ledger: Vec<u8> },
    /// A checkpoint received from a peer (`activate_received`).
    Received { rx: Incoming },
    /// A received checkpoint whose base this member's log holds: its tail
    /// is this member's own entries, and installing it compacts the log
    /// (`Replica::install_own`).
    Own { rx: Incoming },
}
// Keep one immutable object per receiver while a newer local checkpoint
// is published. An open descriptor survives unlink and a slow receiver can
// finish its transfer without repeatedly starting over.
struct Outgoing {
    file: Arc<File>,
    image: Image,
    view: u64,
    last_read: u64,
}
struct Offer {
    peer: ReplicaId,
    view: u64,
    commit: Op,
    image: Image,
}
struct Prepared {
    engine: String,
    ledger: Vec<u8>,
}
struct Incoming {
    offer: Offer,
    bytes_from: ReplicaId,
    progress_at: u64,
    file: File,
    offset: u64,
    tail: Vec<Entry>,
    prepared: Option<Prepared>,
    /// When `prepared` was set (ms): backups install one after another
    /// ([`INSTALL_STAGGER_MS`]).
    ready_at: u64,
    last_request: u64,
}

impl Incoming {
    /// The next member (of `members`, in order) to ask for the image bytes.
    fn next_source(&mut self, id: ReplicaId, members: &[ReplicaId]) {
        let others: Vec<ReplicaId> = members.iter().copied().filter(|m| *m != id).collect();
        if let Some(i) = others.iter().position(|m| *m == self.bytes_from) {
            self.bytes_from = others[(i + 1) % others.len()];
        } else if let Some(first) = others.first() {
            self.bytes_from = *first;
        }
        self.progress_at = now_ms();
        self.last_request = self.progress_at;
    }
}

pub(super) struct Checkpoints {
    root: PathBuf,
    pub(super) quarantine: bool,
    current: Option<Generation>,
    ledger: Vec<u8>,
    /// Members that said Hello since this process started, and whether they
    /// support empty-engine images.
    peers: BTreeMap<ReplicaId, bool>,
    /// Members that sent a frame since the last Hello round, one bit per ID.
    heard: u32,
    /// When each member last sent a frame or had this replica's connection
    /// open, at Hello-round resolution. Starts at process start.
    reachable: Vec<u64>,
    sending: BTreeMap<ReplicaId, Outgoing>,
    hello: Option<u64>,
    cleanup_due: bool,
    queued: Option<(u64, Sources, Boundary)>,
    /// A log group: each shard's newest checkpoint of this primary epoch.
    pinned: BTreeMap<u32, Arc<Sources>>,
    pinned_epoch: u64,
    /// When the I/O thread last asked every shard for a checkpoint.
    round_at: u64,
    /// A log group: where each shard's records start past its checkpoint,
    /// kept as entries commit (see [`log3::Uncovered`]).
    uncovered: log3::Uncovered,
    /// Something the next image depends on changed (a checkpoint pinned, a
    /// build finished, the log replaced) since the last capture check.
    capture_due: bool,
    /// When the I/O thread last checked whether to build an image.
    capture_checked_at: u64,
    /// When this member last published a generation of its own (ms).
    published_at: u64,
    build: Option<JoinHandle<io::Result<Built>>>,
    /// A new generation being written off the I/O thread (see
    /// `Core::stage_generation`); nothing else captures, installs or
    /// reclaims meanwhile.
    staging: Option<Staging>,
    incoming: Option<Incoming>,
    unpack: Option<JoinHandle<io::Result<Prepared>>>,
    /// The thread that reads and sends image pages (`serve_page`).
    pages: Option<std::sync::mpsc::Sender<PageJob>>,
}

/// One image page for the page thread to read and send.
struct PageJob {
    peer: Arc<transport::Peer>,
    traffic: Arc<Traffic>,
    file: Arc<File>,
    me: ReplicaId,
    page: Wire,
    offset: u64,
    size: usize,
}

impl PageJob {
    fn run(self) {
        let mut chunk = vec![0; self.size];
        // A page that cannot be read is not sent: the receiver asks again
        // and then tries another member.
        if self.file.read_exact_at(&mut chunk, self.offset).is_err() {
            return;
        }
        let Ok(bytes) = self.page.encode(&chunk) else {
            return;
        };
        let mut frame = Vec::with_capacity(bytes.len() + 16);
        frame::encode(self.me, &bytes, &mut frame);
        self.peer.send(frame, &self.traffic);
    }
}

impl Checkpoints {
    pub(super) fn new(
        root: PathBuf,
        current: Option<Generation>,
        ledger: Vec<u8>,
        quarantine: bool,
        members: u8,
    ) -> Self {
        Self {
            root,
            quarantine,
            current,
            ledger,
            peers: BTreeMap::new(),
            heard: 0,
            reachable: (0..fusee_core::vsr::MAX_MEMBERS.max(usize::from(members)))
                .map(|i| {
                    if i < usize::from(members) {
                        now_ms()
                    } else {
                        u64::MAX
                    }
                })
                .collect(),
            sending: BTreeMap::new(),
            hello: None,
            cleanup_due: true,
            queued: None,
            pinned: BTreeMap::new(),
            pinned_epoch: 0,
            round_at: 0,
            uncovered: log3::Uncovered::default(),
            capture_due: true,
            capture_checked_at: 0,
            published_at: 0,
            build: None,
            staging: None,
            incoming: None,
            unpack: None,
            pages: None,
        }
    }

    /// Hands `job` to the page thread, starting it on first use.
    fn serve_page(&mut self, job: PageJob) -> io::Result<()> {
        let job = match &self.pages {
            Some(tx) => match tx.send(job) {
                Ok(()) => return Ok(()),
                Err(std::sync::mpsc::SendError(job)) => job,
            },
            None => job,
        };
        let (tx, rx) = std::sync::mpsc::channel::<PageJob>();
        thread::Builder::new()
            .name("fusee-pages".into())
            .spawn(move || {
                while let Ok(job) = rx.recv() {
                    job.run();
                }
            })?;
        tx.send(job).map_err(|_| other("page thread stopped"))?;
        self.pages = Some(tx);
        Ok(())
    }
}

impl Checkpoints {
    /// Whether `peer` holds back compaction on this primary.
    ///
    /// A member running a binary from before checkpoints cannot install one,
    /// so the group's first checkpoint waits for every member to advertise
    /// support with Hello, which current members send every 500 ms. A member
    /// that has neither sent a frame nor kept a connection open for
    /// `ABSENT_MS` is down or cut off: it no longer holds the log, and when
    /// it returns it installs the primary's offered image through the normal
    /// transfer path. A reachable member that never says Hello (an older
    /// binary) still holds the first checkpoint back. Once this replica holds
    /// a generation, the group has passed its first checkpoint and nobody
    /// holds compaction back.
    ///
    /// This rule decides only when compaction may start, never what it may
    /// drop: compaction covers committed entries in a durable image, and a
    /// member below the new base takes the image and a verified suffix.
    fn holds_back(&self, peer: ReplicaId, now: u64) -> bool {
        self.current.is_none()
            && !self.peers.contains_key(&peer)
            && self
                .reachable
                .get(usize::from(peer))
                .is_some_and(|&seen| seen != u64::MAX && now.saturating_sub(seen) < ABSENT_MS)
    }

    /// How long after a publication the next capture waits, in ms, and
    /// the protocol log size that captures sooner: both grow with the
    /// current image. An image holds every shard's run store, so it grows
    /// with every run kept; building it, sending it to each backup and
    /// unpacking it there takes the disks and links the protocol log
    /// needs. The interval keeps that under
    /// [`CAPTURE_IMAGE_BYTES_PER_S`]; the cap keeps replaying the log
    /// cheaper than the image it saves rewriting.
    fn cadence(&self) -> (u64, u64) {
        let image = self.current.as_ref().map_or(0, |g| g.image.len);
        (
            CAPTURE_MIN_INTERVAL_MS.max(image.saturating_mul(1000) / CAPTURE_IMAGE_BYTES_PER_S),
            (CAPTURE_LOG_CAP_EXTENTS * VSR_EXTENT).max(image.saturating_mul(2)),
        )
    }

    fn next_sequence(&self) -> io::Result<u64> {
        self.current
            .as_ref()
            .map_or(0, |g| g.sequence)
            .checked_add(1)
            .ok_or_else(|| other("replica generation exhausted"))
    }
}
impl Checkpoints {
    /// The image of the current generation.
    pub(super) fn image(&self) -> Option<&Image> {
        self.current.as_ref().map(|g| &g.image)
    }
}

impl Drop for Checkpoints {
    fn drop(&mut self) {
        if let Some(job) = self.build.take() {
            let _ = job.join();
        }
        if let Some(job) = self.unpack.take() {
            let _ = job.join();
        }
    }
}

impl Core {
    /// Queues a transfer frame behind this round's protocol frames.
    fn wire(&mut self, to: ReplicaId, message: Wire, data: &[u8]) -> io::Result<()> {
        let bytes = message.encode(data)?;
        if let Some(buf) = self.outbox.get_mut(usize::from(to)) {
            frame::encode(self.id, &bytes, buf);
        }
        Ok(())
    }

    fn offer(&mut self, to: ReplicaId) -> io::Result<()> {
        if self.checkpoints.quarantine {
            return Ok(());
        }
        let Some(g) = &self.checkpoints.current else {
            return Ok(());
        };
        if g.image.lsn == 0 && !self.checkpoints.peers.get(&to).copied().unwrap_or(false) {
            return Ok(());
        }
        let view = self.replica.view();
        let slot = self.checkpoints.sending.entry(to);
        let sending = match slot {
            std::collections::btree_map::Entry::Occupied(entry) if entry.get().view == view => {
                entry.into_mut()
            }
            entry => {
                let item = Outgoing {
                    file: Arc::new(File::open(self.checkpoints.root.join(g.image.name()))?),
                    image: g.image.clone(),
                    view,
                    last_read: now_ms(),
                };
                match entry {
                    std::collections::btree_map::Entry::Vacant(e) => e.insert(item),
                    std::collections::btree_map::Entry::Occupied(mut e) => {
                        e.insert(item);
                        e.into_mut()
                    }
                }
            }
        };
        let image = sending.image.clone();
        self.wire(
            to,
            Wire::Offer {
                view,
                commit: self.replica.commit(),
                image,
            },
            &[],
        )
    }

    pub(super) fn receive(
        &mut self,
        from: ReplicaId,
        bytes: &[u8],
        out: &mut Vec<Action>,
    ) -> io::Result<()> {
        if from == self.id || usize::from(from) >= fusee_core::vsr::MAX_MEMBERS {
            return Ok(());
        }
        self.checkpoints.heard |= 1 << from;
        if bytes.first() == Some(&WIRE) {
            if !out.is_empty() {
                self.process(out)?;
            }
            if let Some((msg, data)) = Wire::decode(bytes) {
                self.on_wire(from, msg, data)?;
            }
        } else if let Ok(msg) = decode_msg(bytes) {
            if self.checkpoints.quarantine {
                return Ok(());
            }
            match &msg {
                Msg::GetState { view, after }
                    if self.replica.is_primary()
                        && *view <= self.replica.view()
                        && *after < self.replica.base() =>
                {
                    if !out.is_empty() {
                        self.process(out)?;
                    }
                    self.offer(from)?
                }
                Msg::GetEntries {
                    view, from: first, ..
                } if *view == self.replica.view() && *first <= self.replica.base() => {
                    if !out.is_empty() {
                        self.process(out)?;
                    }
                    self.offer(from)?
                }
                _ => self.replica.on_message(from, msg, now_ms(), out),
            }
        }
        Ok(())
    }

    fn on_wire(&mut self, from: ReplicaId, msg: Wire, data: &[u8]) -> io::Result<()> {
        if self.checkpoints.quarantine && matches!(msg, Wire::Get { .. } | Wire::GetTail { .. }) {
            return Ok(());
        }
        match msg {
            Wire::Hello { base, empty_engine } => {
                self.checkpoints.peers.insert(from, empty_engine);
                if self
                    .checkpoints
                    .sending
                    .get(&from)
                    .is_some_and(|s| base >= s.image.op)
                {
                    self.checkpoints.sending.remove(&from);
                }
                if base < self.replica.base() && self.replica.is_primary() {
                    self.offer(from)?;
                }
            }
            Wire::Offer {
                view,
                commit,
                image,
            } => {
                if self.checkpoints.quarantine
                    && self.checkpoints.current.as_ref().is_some_and(|g| {
                        image.op < g.image.op
                            || (image.op == g.image.op && image.checksum != g.image.checksum)
                    })
                {
                    return Ok(());
                }
                // Nor while a received checkpoint is being installed: its
                // offer is no longer `incoming`, and the same offer again
                // would unpack the same image into the engine directory the
                // installation is about to open (a replica stopped on
                // "File exists" in a saturation run). The primary offers
                // again after it.
                if self.ready
                    || !self
                        .replica
                        .accepts_checkpoint(from, view, image.op, commit)
                    || image.len < 34
                    || self.checkpoints.unpack.is_some()
                    || self.checkpoints.staging.as_ref().is_some_and(|s| {
                        matches!(s.kind, StagedFor::Own { .. } | StagedFor::Received { .. })
                    })
                {
                    return Ok(());
                }
                if let Some(rx) = &mut self.checkpoints.incoming {
                    if rx.offer.peer == from && rx.offer.view == view && rx.offer.image == image {
                        rx.offer.commit = commit;
                        return Ok(());
                    }
                    if rx.offer.view == view && rx.offer.image.op >= image.op {
                        return Ok(());
                    }
                }
                let file = File::create(self.checkpoints.root.join("receiving.tmp"))?;
                self.checkpoints.incoming = Some(Incoming {
                    offer: Offer {
                        peer: from,
                        view,
                        commit,
                        image,
                    },
                    file,
                    bytes_from: from,
                    progress_at: now_ms(),
                    offset: 0,
                    tail: Vec::new(),
                    prepared: None,
                    ready_at: 0,
                    last_request: 0,
                });
                self.request_checkpoint_page()?;
            }
            Wire::Get {
                view,
                digest,
                offset,
            } => {
                // The role-approved offer fixes the digest. Another member
                // that retained the same immutable image may supply its bytes.
                if self
                    .checkpoints
                    .sending
                    .get(&from)
                    .is_none_or(|s| s.image.sha256 != digest || s.view != view)
                {
                    let Some(g) = &self.checkpoints.current else {
                        return self.wire(from, Wire::Unavailable { view, digest }, &[]);
                    };
                    if g.image.sha256 != digest {
                        return self.wire(from, Wire::Unavailable { view, digest }, &[]);
                    }
                    let file = Arc::new(File::open(self.checkpoints.root.join(g.image.name()))?);
                    self.checkpoints.sending.insert(
                        from,
                        Outgoing {
                            file,
                            image: g.image.clone(),
                            view,
                            last_read: now_ms(),
                        },
                    );
                }
                let s = self.checkpoints.sending.get_mut(&from).unwrap();
                if offset >= s.image.len {
                    return Ok(());
                }
                let size = (s.image.len - offset).min(PAGE as u64) as usize;
                s.last_read = now_ms();
                let file = s.file.clone();
                let Some(peer) = self.senders.get(&from).cloned() else {
                    return Ok(());
                };
                // Read and sent off this thread: a page is a megabyte of
                // the image, and reading it here held the protocol lock.
                let job = PageJob {
                    peer,
                    traffic: self.shared.traffic.clone(),
                    file,
                    me: self.id,
                    page: Wire::Page {
                        view,
                        digest,
                        offset,
                    },
                    offset,
                    size,
                };
                self.checkpoints.serve_page(job)?;
            }
            Wire::Unavailable { view, digest } => {
                let id = self.id;
                let n = self.replica.membership().members.clone();
                if let Some(rx) = self.checkpoints.incoming.as_mut()
                    && rx.bytes_from == from
                    && rx.offer.view == view
                    && rx.offer.image.sha256 == digest
                    && rx.offset < rx.offer.image.len
                {
                    rx.next_source(id, &n);
                    // Let an original donor that compacted again offer a newer anchor.
                    if from == rx.offer.peer {
                        self.checkpoints.hello = None;
                    }
                }
            }
            Wire::Page {
                view,
                digest,
                offset,
            } => {
                let Some(rx) = &mut self.checkpoints.incoming else {
                    return Ok(());
                };
                let o = &rx.offer;
                if from != rx.bytes_from
                    || view != o.view
                    || digest != o.image.sha256
                    || offset != rx.offset
                    || !self
                        .replica
                        .accepts_checkpoint(o.peer, view, o.image.op, o.commit)
                    || offset >= o.image.len
                    || data.len() as u64 != (o.image.len - offset).min(PAGE as u64)
                {
                    return Ok(());
                }
                rx.file.write_all(data)?;
                rx.offset += data.len() as u64;
                rx.progress_at = now_ms();
                self.replica.checkpoint_progress(now_ms());
                if rx.offset == o.image.len {
                    // Verification, unpacking and file syncs can take longer
                    // than an election interval. Keep processing peer traffic.
                    let root = self.checkpoints.root.clone();
                    let method = self.shared.local.flush;
                    let sequence = self
                        .checkpoints
                        .current
                        .as_ref()
                        .map_or(0, |g| g.sequence)
                        .checked_add(1)
                        .ok_or_else(|| other("replica generation exhausted"))?;
                    let image = o.image.clone();
                    let input = root.join("receiving.tmp");
                    // Synced on the unpacking thread: the whole image's
                    // sync held this one for up to 9 ms.
                    let received = rx.file.try_clone()?;
                    self.checkpoints.unpack = Some(
                        thread::Builder::new()
                            .name("fusee-checkpoint-install".into())
                            .spawn(move || {
                                flush_file(&received, method)?;
                                drop(received);
                                let parent = root
                                    .parent()
                                    .ok_or_else(|| other("missing replica directory"))?;
                                let engine =
                                    format!("engine-{}", Generation::slot(&image, sequence));
                                let engine_dir = parent.join(&engine);
                                generation::remove_unpublished(&engine_dir)?;
                                let ledger = snapshot::unpack(&input, &image, &engine_dir, method)?;
                                std::fs::create_dir_all(engine_dir.join("shard-000"))?;
                                sync_dir(&engine_dir, method)?;
                                sync_dir(parent, method)?;
                                fs::rename(input, root.join(image.name()))?;
                                sync_dir(&root, method)?;
                                Ok(Prepared { engine, ledger })
                            })?,
                    );
                } else {
                    self.request_checkpoint_page()?;
                }
            }
            Wire::GetTail {
                view,
                digest,
                first,
                last,
            } => {
                let Some(s) = self.checkpoints.sending.get_mut(&from) else {
                    return self.offer(from);
                };
                if view != self.replica.view() || s.view != view || s.image.sha256 != digest {
                    return self.offer(from);
                }
                if first <= self.replica.base() {
                    // The requested tail is in a newer committed image.
                    self.checkpoints.sending.remove(&from);
                    return self.offer(from);
                }
                if last < first || last > self.replica.commit() {
                    return Ok(());
                }
                s.last_read = now_ms();
                let entries = self.replica.committed_page(first, last);
                let mut bytes = Vec::new();
                encode_msg(
                    &Msg::Entries {
                        view,
                        entries,
                        commit: self.replica.commit(),
                    },
                    &mut bytes,
                );
                self.wire(from, Wire::Tail { view, digest }, &bytes)?;
            }
            Wire::Tail { view, digest } => {
                let Some(rx) = &mut self.checkpoints.incoming else {
                    return Ok(());
                };
                let o = &rx.offer;
                if rx.prepared.is_none()
                    || from != o.peer
                    || view != o.view
                    || digest != o.image.sha256
                    || !self
                        .replica
                        .accepts_checkpoint(from, view, o.image.op, o.commit)
                {
                    return Ok(());
                }
                let Ok(Msg::Entries {
                    view: v,
                    entries,
                    commit,
                }) = decode_msg(data)
                else {
                    return Ok(());
                };
                if v != view || commit < self.replica.commit() {
                    return Ok(());
                }
                let (mut op, mut checksum) = rx
                    .tail
                    .last()
                    .map_or((o.image.op, o.image.checksum), |e| (e.op, e.checksum));
                for e in entries {
                    if e.op != op + 1
                        || e.parent != checksum
                        || e.op > self.replica.commit()
                        || !e.valid()
                    {
                        break;
                    }
                    (op, checksum) = (e.op, e.checksum);
                    rx.tail.push(e);
                    self.replica.checkpoint_progress(now_ms());
                }
                rx.last_request = 0;
            }
        }
        Ok(())
    }

    fn request_checkpoint_page(&mut self) -> io::Result<()> {
        let Some(rx) = &mut self.checkpoints.incoming else {
            return Ok(());
        };
        let o = &rx.offer;
        let msg = if rx.offset < o.image.len {
            Wire::Get {
                view: o.view,
                digest: o.image.sha256,
                offset: rx.offset,
            }
        } else if rx.prepared.is_some() {
            let first = rx.tail.last().map_or(o.image.op, |e| e.op) + 1;
            if first > self.replica.commit() {
                return Ok(());
            }
            Wire::GetTail {
                view: o.view,
                digest: o.image.sha256,
                first,
                last: self.replica.commit(),
            }
        } else {
            return Ok(());
        };
        rx.last_request = now_ms();
        let peer = if rx.offset < o.image.len {
            rx.bytes_from
        } else {
            o.peer
        };
        self.wire(peer, msg, &[])
    }

    pub(super) fn capture_checkpoint(&mut self, epoch: u64, sources: Sources) -> io::Result<()> {
        if !self.ready || self.shared.epoch.load(Ordering::Acquire) != epoch {
            return Ok(());
        }
        if let Some(index) = sources.index() {
            if self.checkpoints.pinned_epoch != epoch {
                self.checkpoints.pinned.clear();
                self.checkpoints.pinned_epoch = epoch;
            }
            self.checkpoints.pinned.insert(index, Arc::new(sources));
            self.checkpoints.capture_due = true;
            if index < 64 {
                self.shared
                    .checkpoint_wanted
                    .fetch_and(!(1 << index), Ordering::AcqRel);
            }
            return Ok(());
        }
        let mut anchor = None;
        for e in self
            .replica
            .entries()
            .rev()
            .filter(|e| e.op <= self.lease_applied && is_segment_batch(&e.body))
        {
            let batch = proto::decode_batch(&mut Reader::new(&e.body)).map_err(other)?;
            if batch.last_lsn < sources.lsn() {
                break;
            }
            if batch.last_lsn == sources.lsn() {
                let chunk = batch
                    .chunks
                    .last()
                    .ok_or_else(|| other("checkpoint names an empty log batch"))?;
                let length = chunk
                    .offset
                    .checked_add(chunk.bytes.len() as u64)
                    .ok_or_else(|| other("checkpoint segment length overflow"))?;
                let file = sources.boundary(chunk.segment, length)?;
                anchor = Some(Boundary {
                    op: e.op,
                    checksum: e.checksum,
                    segment: chunk.segment,
                    length,
                    file,
                });
                break;
            }
        }
        let Some(anchor) = anchor else { return Ok(()) };
        // Hold the boundary descriptor together with the checkpoint and store
        // prefix while waiting for the next compaction threshold.
        self.checkpoints.queued = Some((epoch, sources, anchor));
        Ok(())
    }

    fn begin_capture(&mut self, now: u64) -> io::Result<()> {
        if !self.ready
            || self.checkpoints.build.is_some()
            || self.checkpoints.staging.is_some()
            || self
                .senders
                .keys()
                .any(|&peer| self.checkpoints.holds_back(peer, now))
        {
            return Ok(());
        }
        // The engine can checkpoint after small changes to its run store.
        // Rewriting and syncing an entire replica generation for each such
        // change competes with quorum writes. Keep a bounded suffix instead.
        let (interval, cap) = self.checkpoints.cadence();
        let below_threshold = (self.replica.commit().saturating_sub(self.replica.base()) < 1024
            && self.log.end < 4 * VSR_EXTENT)
            || (now.saturating_sub(self.checkpoints.published_at) < interval && self.log.end < cap);
        if self.checkpoints.current.is_some() && below_threshold {
            return Ok(());
        }
        if let Kind::Log(shards) = self.shared.kind() {
            return self.begin_log_capture(shards, now, below_threshold);
        }
        let Some((epoch, sources, mut anchor)) = self.checkpoints.queued.take() else {
            if !below_threshold {
                self.begin_control_capture()?;
            }
            return Ok(());
        };
        if !self.ready || self.shared.epoch.load(Ordering::Acquire) != epoch {
            return Ok(());
        }
        if anchor.op <= self.replica.base()
            || self
                .replica
                .entry(anchor.op)
                .is_none_or(|e| e.checksum != anchor.checksum)
        {
            return Ok(());
        }
        // Control entries do not change the captured workflow files.
        let first = anchor.op;
        for e in self
            .replica
            .entries()
            .skip_while(|e| e.op <= first)
            .take_while(|e| e.op <= self.lease_applied && leases::is_control(&e.body))
        {
            anchor.op = e.op;
            anchor.checksum = e.checksum;
        }

        let mut ledger = leases::Ledger::restore(&self.checkpoints.ledger)?;
        for e in self
            .replica
            .entries()
            .take_while(|e| e.op <= anchor.op)
            .filter(|e| leases::is_control(&e.body))
        {
            ledger.apply(&e.body)?;
        }
        let ledger = ledger.snapshot()?;
        let root = self.checkpoints.root.clone();
        let method = self.shared.local.flush;
        self.checkpoints.build = Some(
            thread::Builder::new()
                .name("fusee-checkpoint-copy".into())
                .spawn(move || {
                    let image = snapshot::create(&root, sources, anchor, &ledger, method)?;
                    snapshot::verify(&root.join(image.name()), &image)?;
                    Ok(Built {
                        epoch,
                        image,
                        ledger,
                    })
                })?,
        );
        Ok(())
    }

    /// A log group's image: every shard's newest checkpoint, at the last op
    /// through which each shard's records are covered by its checkpoint.
    fn begin_log_capture(
        &mut self,
        shards: u32,
        now: u64,
        below_threshold: bool,
    ) -> io::Result<()> {
        let epoch = self.shared.epoch.load(Ordering::Acquire);
        if self.checkpoints.pinned_epoch != epoch {
            self.checkpoints.pinned.clear();
            self.checkpoints.pinned_epoch = epoch;
        }
        let base = self.replica.base();
        let Some(limit) = self.index_uncovered()? else {
            return Ok(());
        };
        let index = &self.checkpoints.uncovered;
        // Only permit commands since the image: copy its engine state.
        if !index.any_batch(limit) {
            return if below_threshold {
                Ok(())
            } else {
                self.begin_control_capture()
            };
        }
        let prior: Vec<u64> = self
            .checkpoints
            .current
            .as_ref()
            .map(|g| g.image.covered.clone())
            .unwrap_or_default();
        let covered: Vec<Lsn> = (0..shards)
            .map(|i| self.checkpoints.pinned.get(&i).map_or(0, |s| s.lsn()))
            .collect();
        let first = index.first_uncovered(&covered, limit);
        debug_assert_eq!(
            first,
            log3::first_uncovered(
                self.replica
                    .entries()
                    .filter(|e| e.op > base && e.op <= limit),
                &covered,
            )?,
            "incremental checkpoint index disagrees with the log"
        );
        // A shard whose state the current image holds must contribute a
        // newer checkpoint, or the new image would drop it.
        let mut wanted = 0u64;
        for i in 0..shards {
            let held = prior.get(i as usize).is_some_and(|&lsn| lsn > 0);
            if held && !self.checkpoints.pinned.contains_key(&i) && i < 64 {
                wanted |= 1 << i;
            }
        }
        let anchor = first
            .iter()
            .map(|f| f.map_or(limit, |op| op - 1))
            .min()
            .unwrap_or(limit);
        if wanted != 0 || anchor <= base {
            // Ask the shards holding the image back for a checkpoint: those
            // with nothing new when due, the rest at the next round.
            for (i, f) in first.iter().enumerate() {
                if f.is_some_and(|op| op - 1 == anchor) && i < 64 {
                    wanted |= 1 << i;
                }
            }
            self.shared
                .checkpoint_wanted
                .fetch_or(wanted, Ordering::AcqRel);
            if !below_threshold && now.saturating_sub(self.checkpoints.round_at) >= 200 {
                self.checkpoints.round_at = now;
                self.shared.checkpoint_round.fetch_add(1, Ordering::AcqRel);
            }
            return Ok(());
        }
        let checksum = self
            .replica
            .entry(anchor)
            .map(|e| e.checksum)
            .ok_or_else(|| other("checkpoint anchor left the protocol log"))?;
        // The lease ledger at the anchor: the image's, plus the permit
        // commands since, found through the index rather than a walk.
        let mut ledger = leases::Ledger::restore(&self.checkpoints.ledger)?;
        for op in self.checkpoints.uncovered.control(anchor) {
            let e = self
                .replica
                .entry(op)
                .ok_or_else(|| other("permit command left the protocol log"))?;
            ledger.apply(&e.body)?;
        }
        let ledger = ledger.snapshot()?;
        let sources: Vec<Arc<Sources>> = self.checkpoints.pinned.values().cloned().collect();
        let root = self.checkpoints.root.clone();
        let method = self.shared.local.flush;
        self.checkpoints.build = Some(
            thread::Builder::new()
                .name("fusee-checkpoint-copy".into())
                .spawn(move || {
                    let image = snapshot::create_log(
                        &root, &sources, anchor, checksum, shards, &ledger, method,
                    )?;
                    snapshot::verify(&root.join(image.name()), &image)?;
                    Ok(Built {
                        epoch,
                        image,
                        ledger,
                    })
                })?,
        );
        Ok(())
    }

    /// Indexes what committed since the last call: each entry is decoded
    /// once, not on every tick, and on every
    /// tick of a primary rather than at the next capture check, which
    /// after a publication came a second later and decoded a second's
    /// entries at once (3 ms at 250,000 steps/s). Returns the limit it
    /// indexed through: released, committed and durable here.
    pub(super) fn index_uncovered(&mut self) -> io::Result<Option<Op>> {
        let base = self.replica.base();
        let limit = self
            .lease_applied
            .min(self.replica.commit())
            .min(self.replica.durable());
        if limit <= base {
            return Ok(None);
        }
        let index = &mut self.checkpoints.uncovered;
        index.compact(base);
        let through = index.through();
        index.extend(
            self.replica
                .entries_after(through)
                .take_while(|e| e.op <= limit),
            leases::is_control,
        )?;
        Ok(Some(limit))
    }

    fn begin_control_capture(&mut self) -> io::Result<()> {
        let current = self.checkpoints.current.as_ref();
        if current.is_none_or(|g| g.image.lsn == 0)
            && self.checkpoints.peers.values().any(|supported| !supported)
        {
            return Ok(());
        }
        // A workflow batch is the boundary: it needs its own engine checkpoint.
        // Do not consume any part of the log beyond the locally applied prefix.
        let mut bytes = 0usize;
        let mut anchor = None;
        for e in self
            .replica
            .entries()
            .take_while(|e| e.op <= self.lease_applied && leases::is_control(&e.body))
        {
            bytes = bytes.saturating_add(e.body.len());
            anchor = Some((e.op, e.checksum));
        }
        let Some((op, checksum)) = anchor else {
            return Ok(());
        };
        if op - self.replica.base() < 1024 && bytes < 4 * VSR_EXTENT as usize {
            return Ok(());
        }
        let root = self.checkpoints.root.clone();
        let previous = current
            .map(|g| File::open(root.join(g.image.name())).map(|f| (g.image.clone(), f)))
            .transpose()?;
        let ledger = if op == self.lease_applied {
            self.leases.snapshot()?
        } else {
            let mut ledger = leases::Ledger::restore(&self.checkpoints.ledger)?;
            for e in self.replica.entries().take_while(|e| e.op <= op) {
                ledger.apply(&e.body)?;
            }
            ledger.snapshot()?
        };
        let epoch = self.shared.epoch.load(Ordering::Acquire);
        let method = self.shared.local.flush;
        self.checkpoints.build = Some(
            thread::Builder::new()
                .name("fusee-checkpoint-copy".into())
                .spawn(move || {
                    let image = snapshot::copy_control_prefix(
                        &root, previous, op, checksum, &ledger, method,
                    )?;
                    snapshot::verify(&root.join(image.name()), &image)?;
                    Ok(Built {
                        epoch,
                        image,
                        ledger,
                    })
                })?,
        );
        Ok(())
    }

    fn publish_generation(
        &mut self,
        g: Generation,
        new_log: VsrLog,
        ledger: Vec<u8>,
    ) -> io::Result<()> {
        // Background writes go to the log being replaced; the new one holds
        // every entry after the image durably.
        if let Some(op) = self.drain_writes()? {
            self.replica.on_durable(op, &mut Vec::new());
        }
        let mut new_log = new_log;
        // The new generation's log is written as the old one was.
        new_log.refused = self.log.refused.clone();
        new_log.use_direct(self.log.want_direct)?;
        self.log = new_log;
        let prior = self.checkpoints.current.replace(g);
        self.checkpoints.quarantine = false;
        self.checkpoints.ledger = ledger;
        self.checkpoints.cleanup_due = true;
        let root = &self.checkpoints.root;
        generation::retire_prefix_log(root, self.shared.local.flush)?;
        if let Some(old) = prior {
            // Unreferenced since the pointer switched: deleted off this
            // thread (see `generation::discard`).
            generation::discard(&old.log_dir(root), root)?;
            if self
                .checkpoints
                .current
                .as_ref()
                .is_none_or(|g| g.image.name() != old.image.name())
            {
                generation::discard(&root.join(old.image.name()), root)?;
            }
        }
        Ok(())
    }

    fn activate_local(&mut self, b: Built) -> io::Result<()> {
        if !self.ready
            || self.shared.epoch.load(Ordering::Acquire) != b.epoch
            || b.image.op <= self.replica.base()
            || self
                .replica
                .entry(b.image.op)
                .is_none_or(|e| e.checksum != b.image.checksum)
        {
            return Ok(());
        }
        self.save_hint()?;
        let dir = self
            .shared
            .engine_dir
            .lock()
            .map_err(|_| other("poisoned engine directory"))?
            .clone();
        let name = dir
            .file_name()
            .and_then(|n| n.to_str())
            .ok_or_else(|| other("invalid engine path"))?;
        let g = Generation::new(
            b.image,
            name.to_owned(),
            &b.ledger,
            self.checkpoints.next_sequence()?,
        );
        let sb = Superblock {
            view: self.replica.view(),
            last_normal: self.replica.last_normal(),
            commit: self.replica.commit(),
            applied: self.applied,
        };
        let tail = self.own_tail(g.image.op, g.image.checksum);
        let ledger = b.ledger.clone();
        self.stage_generation(
            g,
            sb,
            tail,
            ledger,
            StagedFor::Local {
                epoch: b.epoch,
                ledger: b.ledger,
            },
        )
    }

    /// Starts writing generation `g` with `tail` in the background; its
    /// commit runs when it is done (`finish_staging`).
    fn stage_generation(
        &mut self,
        g: Generation,
        sb: Superblock,
        source: TailSource,
        ledger: Vec<u8>,
        kind: StagedFor,
    ) -> io::Result<()> {
        let (root, method, staged) = (
            self.checkpoints.root.clone(),
            self.shared.local.flush,
            g.clone(),
        );
        let received = matches!(kind, StagedFor::Received { .. });
        let job = thread::Builder::new()
            .name("fusee-generation".into())
            .spawn(move || {
                let tail = match source {
                    TailSource::Entries(tail) => tail,
                    TailSource::File { path, after, end } => VsrLog::read_tail(&path, after, end)?,
                };
                let last = tail
                    .last()
                    .map_or((staged.image.op, staged.image.checksum), |e| {
                        (e.op, e.checksum)
                    });
                let log = staged.stage(&root, method, tail.iter(), &sb, &ledger)?;
                // A received tail becomes the replica's log: checked and
                // built here, not on the I/O thread.
                let map = received.then(|| {
                    let intact = fusee_core::vsr::Replica::chain_intact(
                        staged.image.op,
                        staged.image.checksum,
                        &tail,
                    );
                    (tail.iter().map(|e| (e.op, e.clone())).collect(), intact)
                });
                Ok(Staged {
                    log,
                    tail,
                    last,
                    map,
                })
            })?;
        self.checkpoints.staging = Some(Staging {
            job,
            g,
            view: self.replica.view(),
            kind,
        });
        Ok(())
    }

    /// The entries of this member's log after `op` (with `checksum`), as a
    /// staging thread reads them from the log file.
    fn own_tail(&self, op: u64, checksum: u64) -> TailSource {
        TailSource::File {
            path: self.log.dir.join("log"),
            after: (op, checksum),
            end: self.log.end,
        }
    }

    /// A staged generation is written: catch up, commit and switch to it,
    /// or discard it if what it was staged for no longer holds.
    fn finish_staging(&mut self, s: Staging, out: &mut Vec<Action>) -> io::Result<()> {
        let Staged {
            log,
            tail,
            last,
            map,
        } = s
            .job
            .join()
            .map_err(|_| other("generation write panicked"))??;
        let g = s.g;
        let (last_op, last_sum) = last;
        let discard = |root: &Path| generation::remove_unpublished(&g.log_dir(root));
        match s.kind {
            StagedFor::Own { rx } => {
                let checked = self.shared.traffic.timings.start();
                let o = &rx.offer;
                // The tail is this member's own entries: what it took since
                // they were read follows them in its log (checked as taken).
                let still = !self.ready
                    && self.replica.view() == s.view
                    && self.replica.holds_checkpoint(
                        o.peer,
                        o.view,
                        o.image.op,
                        o.image.checksum,
                        o.commit,
                    )
                    && (last_op == o.image.op
                        || self
                            .replica
                            .entry(last_op)
                            .is_some_and(|e| e.checksum == last_sum));
                if !still {
                    return discard(&self.checkpoints.root);
                }
                let extra: Vec<Entry> = self.replica.entries_after(last_op).cloned().collect();
                self.shared.traffic.timings.install_check.observe(checked);
                self.activate_own(rx, g, log, &extra, out)
            }
            StagedFor::Local { epoch, ledger } => {
                let still = self.ready
                    && self.shared.epoch.load(Ordering::Acquire) == epoch
                    && self.replica.view() == s.view
                    && g.image.op > self.replica.base()
                    && self
                        .replica
                        .entry(g.image.op)
                        .is_some_and(|e| e.checksum == g.image.checksum)
                    && (last_op == g.image.op
                        || self
                            .replica
                            .entry(last_op)
                            .is_some_and(|e| e.checksum == last_sum));
                if !still {
                    return discard(&self.checkpoints.root);
                }
                // What the log took since the tail was read: every entry
                // after it, as a publication from scratch would write.
                let extra: Vec<Entry> = self.replica.entries_after(last_op).cloned().collect();
                let written = self.shared.traffic.timings.start();
                let new_log =
                    g.commit(&self.checkpoints.root, self.shared.local.flush, log, &extra)?;
                self.shared
                    .traffic
                    .timings
                    .generation_write
                    .observe(written);
                match self
                    .replica
                    .compact_through_taking(g.image.op, g.image.checksum)
                {
                    Some(dropped) => drop_elsewhere(dropped),
                    None => return Err(other("checkpoint no longer matches the protocol log")),
                }
                self.checkpoints.published_at = now_ms();
                let switched = self.shared.traffic.timings.start();
                let r = self.publish_generation(g, new_log, ledger);
                self.shared
                    .traffic
                    .timings
                    .generation_switch
                    .observe(switched);
                r
            }
            StagedFor::Received { rx } => {
                let checked = self.shared.traffic.timings.start();
                let Some((mut map, intact)) = map else {
                    return discard(&self.checkpoints.root);
                };
                // The entries this member took since the tail was copied
                // (checked as they are taken).
                let staged_len = tail.len();
                let extra = self.replica.checkpoint_suffix(last_op, last_sum);
                let mut tail = tail;
                tail.extend(extra.iter().cloned());
                let o = &rx.offer;
                let last = tail.last().map_or(o.image.op, |e| e.op);
                let still = intact
                    && !self.ready
                    && self.replica.view() == s.view
                    && self
                        .replica
                        .accepts_checkpoint(o.peer, o.view, o.image.op, o.commit)
                    && self.replica.checkpoint_matches_checked(
                        o.image.op,
                        o.image.checksum,
                        &tail,
                        staged_len,
                    )
                    && last >= self.replica.commit()
                    && !(self.replica.status() == fusee_core::vsr::Status::Normal
                        && last < self.replica.durable());
                if !still {
                    drop_elsewhere(map);
                    return discard(&self.checkpoints.root);
                }
                for e in &extra {
                    map.insert(e.op, e.clone());
                }
                self.shared.traffic.timings.install_check.observe(checked);
                self.activate_received(rx, tail, Some((g, log, extra, map)), out)
            }
        }
    }

    /// Installs a received checkpoint. Without `staged` it writes the new
    /// generation in the background first and returns (`finish_staging`
    /// calls this again with it: the staged generation, its log and the
    /// entries to append to it).
    fn activate_received(
        &mut self,
        rx: Incoming,
        tail: Vec<Entry>,
        staged: Option<(
            Generation,
            VsrLog,
            Vec<Entry>,
            std::collections::BTreeMap<u64, Entry>,
        )>,
        out: &mut Vec<Action>,
    ) -> io::Result<()> {
        // A staged installation was checked as it finished (`finish_staging`).
        if staged.is_none()
            && !self
                .replica
                .checkpoint_matches(rx.offer.image.op, rx.offer.image.checksum, &tail)
        {
            return Ok(());
        }
        let Some((g, staged_log, extra, map)) = staged else {
            let prepared = rx
                .prepared
                .as_ref()
                .ok_or_else(|| other("unpacked checkpoint missing"))?;
            let o = &rx.offer;
            let sb = Superblock {
                view: self.replica.view().max(o.view),
                last_normal: self.replica.last_normal(),
                commit: self.replica.commit().max(o.image.op),
                applied: o.image.op,
            };
            let g = Generation::new(
                o.image.clone(),
                prepared.engine.clone(),
                &prepared.ledger,
                self.checkpoints.next_sequence()?,
            );
            let ledger = prepared.ledger.clone();
            return self.stage_generation(
                g,
                sb,
                TailSource::Entries(tail),
                ledger,
                StagedFor::Received { rx },
            );
        };
        let o = rx.offer;
        let prepared = rx
            .prepared
            .ok_or_else(|| other("unpacked checkpoint missing"))?;
        let engine_dir = self
            .checkpoints
            .root
            .parent()
            .ok_or_else(|| other("missing replica directory"))?
            .join(&prepared.engine);
        let writer =
            BatchWriter::new(&engine_dir.join("shard-000/log"), self.shared.local.clone())?;
        let written = self.shared.traffic.timings.start();
        let new_log = g.commit(
            &self.checkpoints.root,
            self.shared.local.flush,
            staged_log,
            &extra,
        )?;
        self.shared
            .traffic
            .timings
            .generation_write
            .observe(written);
        let replaced = self.shared.traffic.timings.start();
        // Checked as the staging finished, in this same tick.
        let last = tail.last().map_or(o.image.op, |e| e.op);
        match self.replica.install_matched(
            o.peer,
            o.view,
            o.image.op,
            o.image.checksum,
            map,
            last,
            now_ms(),
            out,
        ) {
            Some(old) => drop_elsewhere(old),
            None => return Err(other("checkpoint donor changed during installation")),
        }
        self.shared
            .traffic
            .timings
            .install_replica
            .observe(replaced);
        self.writer = writer;
        self.applied = o.image.op;
        self.lease_applied = o.image.op;
        // The protocol log is now the image and its tail.
        self.checkpoints.uncovered = log3::Uncovered::new(o.image.op);
        self.checkpoints.capture_due = true;
        self.leases = leases::Ledger::restore(&prepared.ledger)?;
        *self
            .shared
            .engine_dir
            .lock()
            .map_err(|_| other("poisoned engine directory"))? = engine_dir;
        let switched = self.shared.traffic.timings.start();
        let r = self.publish_generation(g, new_log, prepared.ledger);
        self.shared
            .traffic
            .timings
            .generation_switch
            .observe(switched);
        r
    }

    /// Stages a received checkpoint whose base this member's log holds
    /// (`StagedFor::Own`).
    fn stage_own(&mut self, rx: Incoming) -> io::Result<()> {
        let prepared = rx
            .prepared
            .as_ref()
            .ok_or_else(|| other("unpacked checkpoint missing"))?;
        let o = &rx.offer;
        let sb = Superblock {
            view: self.replica.view().max(o.view),
            last_normal: self.replica.last_normal(),
            commit: self.replica.commit().max(o.image.op),
            applied: o.image.op,
        };
        let g = Generation::new(
            o.image.clone(),
            prepared.engine.clone(),
            &prepared.ledger,
            self.checkpoints.next_sequence()?,
        );
        let ledger = prepared.ledger.clone();
        let tail = self.own_tail(o.image.op, o.image.checksum);
        self.stage_generation(g, sb, tail, ledger, StagedFor::Own { rx })
    }

    /// Installs a staged `StagedFor::Own` generation: commits it with the
    /// entries taken since its tail was read, then compacts the log
    /// (`Replica::install_own`), as `activate_received` replaces it.
    fn activate_own(
        &mut self,
        rx: Incoming,
        g: Generation,
        staged_log: VsrLog,
        extra: &[Entry],
        out: &mut Vec<Action>,
    ) -> io::Result<()> {
        let o = rx.offer;
        let prepared = rx
            .prepared
            .ok_or_else(|| other("unpacked checkpoint missing"))?;
        let engine_dir = self
            .checkpoints
            .root
            .parent()
            .ok_or_else(|| other("missing replica directory"))?
            .join(&prepared.engine);
        let writer =
            BatchWriter::new(&engine_dir.join("shard-000/log"), self.shared.local.clone())?;
        let written = self.shared.traffic.timings.start();
        let new_log = g.commit(
            &self.checkpoints.root,
            self.shared.local.flush,
            staged_log,
            extra,
        )?;
        self.shared
            .traffic
            .timings
            .generation_write
            .observe(written);
        let replaced = self.shared.traffic.timings.start();
        // Checked as the staging finished, in this same tick.
        match self.replica.install_own(
            o.peer,
            o.view,
            o.image.op,
            o.image.checksum,
            o.commit,
            now_ms(),
            out,
        ) {
            Some(old) => drop_elsewhere(old),
            None => return Err(other("checkpoint donor changed during installation")),
        }
        self.shared
            .traffic
            .timings
            .install_replica
            .observe(replaced);
        self.writer = writer;
        self.applied = o.image.op;
        self.lease_applied = o.image.op;
        self.checkpoints.uncovered = log3::Uncovered::new(o.image.op);
        self.checkpoints.capture_due = true;
        self.leases = leases::Ledger::restore(&prepared.ledger)?;
        *self
            .shared
            .engine_dir
            .lock()
            .map_err(|_| other("poisoned engine directory"))? = engine_dir;
        let switched = self.shared.traffic.timings.start();
        let r = self.publish_generation(g, new_log, prepared.ledger);
        self.shared
            .traffic
            .timings
            .generation_switch
            .observe(switched);
        r
    }

    pub(super) fn service_checkpoints(&mut self, out: &mut Vec<Action>) -> io::Result<()> {
        let now = now_ms();
        if self.ready && matches!(self.shared.kind(), Kind::Log(_)) {
            self.index_uncovered()?;
        }
        if self
            .checkpoints
            .hello
            .is_none_or(|t| now.saturating_sub(t) >= 500)
        {
            self.checkpoints.hello = Some(now);
            let heard = std::mem::take(&mut self.checkpoints.heard);
            for (&peer, link) in &self.senders {
                if heard & (1 << peer) != 0 || link.connected() {
                    self.checkpoints.reachable[usize::from(peer)] = now;
                }
            }
            self.checkpoints.sending.retain(|_, s| {
                s.view == self.replica.view() && now.saturating_sub(s.last_read) < 60_000
            });
            if self.checkpoints.cleanup_due
                && self.checkpoints.build.is_none()
                && self.checkpoints.staging.is_none()
                && self.checkpoints.incoming.is_none()
                && self.checkpoints.unpack.is_none()
            {
                let started = self.shared.traffic.timings.start();
                self.checkpoints.cleanup_due = generation::reclaim(
                    &self.checkpoints.root,
                    self.checkpoints.current.as_ref(),
                    self.shared.local.flush,
                )?;
                self.shared
                    .traffic
                    .timings
                    .checkpoint_reclaim
                    .observe(started);
            }
            let peers: Vec<ReplicaId> = self.senders.keys().copied().collect();
            for peer in peers {
                if self.checkpoints.quarantine {
                    out.push(Action::Send {
                        to: peer,
                        msg: Msg::GetState {
                            view: self.replica.view(),
                            after: 0,
                        },
                    });
                }
                self.wire(
                    peer,
                    Wire::Hello {
                        base: self.replica.base(),
                        empty_engine: true,
                    },
                    &[],
                )?;
            }
        }
        if self
            .checkpoints
            .staging
            .as_ref()
            .is_some_and(|s| s.job.is_finished())
        {
            let s = self.checkpoints.staging.take().unwrap();
            let started = self.shared.traffic.timings.start();
            let local = matches!(s.kind, StagedFor::Local { .. });
            self.finish_staging(s, out)?;
            if local {
                self.shared
                    .traffic
                    .timings
                    .checkpoint_publish
                    .observe(started);
            } else {
                self.shared
                    .traffic
                    .timings
                    .checkpoint_install
                    .observe(started);
            }
        }
        if self
            .checkpoints
            .build
            .as_ref()
            .is_some_and(|job| job.is_finished())
        {
            let result = self
                .checkpoints
                .build
                .take()
                .unwrap()
                .join()
                .map_err(|_| other("checkpoint copy panicked"))??;
            self.checkpoints.cleanup_due = true;
            self.checkpoints.capture_due = true;
            self.activate_local(result)?;
        }
        // Not on every tick: only when an input changed, or the poll is due
        // for what changes continuously (commits, reachability, the round).
        if self.checkpoints.capture_due
            || now.saturating_sub(self.checkpoints.capture_checked_at) >= CAPTURE_POLL_MS
        {
            self.checkpoints.capture_due = false;
            self.checkpoints.capture_checked_at = now;
            let started = self.shared.traffic.timings.start();
            self.begin_capture(now)?;
            self.shared
                .traffic
                .timings
                .checkpoint_capture
                .observe(started);
        }
        if self
            .checkpoints
            .unpack
            .as_ref()
            .is_some_and(|j| j.is_finished())
        {
            let result = self
                .checkpoints
                .unpack
                .take()
                .unwrap()
                .join()
                .map_err(|_| other("checkpoint install panicked"))?;
            match result {
                Ok(prepared) => {
                    if let Some(rx) = self.checkpoints.incoming.as_mut() {
                        rx.prepared = Some(prepared);
                        rx.ready_at = now;
                    }
                }
                Err(e)
                    if matches!(
                        e.kind(),
                        io::ErrorKind::Other
                            | io::ErrorKind::UnexpectedEof
                            | io::ErrorKind::InvalidData
                    ) =>
                {
                    if let Some(rx) = self.checkpoints.incoming.as_mut() {
                        eprintln!(
                            "Fusee replica {} rejected checkpoint bytes from {}: {e}",
                            self.id, rx.bytes_from
                        );
                        rx.next_source(self.id, &self.replica.membership().members.clone());
                        rx.file = File::create(self.checkpoints.root.join("receiving.tmp"))?;
                        rx.offset = 0;
                    }
                }
                Err(e) => return Err(e),
            }
        }
        let Some(rx) = self.checkpoints.incoming.as_mut() else {
            return Ok(());
        };
        let o = &rx.offer;
        if !self
            .replica
            .accepts_checkpoint(o.peer, o.view, o.image.op, o.commit)
            || self.ready
        {
            if self.checkpoints.unpack.is_none() {
                self.checkpoints.incoming = None;
            }
            return Ok(());
        }
        if rx.prepared.is_some() && rx.tail.is_empty() {
            // A backup that holds the image's base installs it by staging
            // its own entries after it (read from its log file off this
            // thread) and compacting: no tail to copy, check or rebuild.
            let holds = self
                .replica
                .entry(o.image.op)
                .is_some_and(|e| e.checksum == o.image.checksum);
            if holds {
                let rank = self
                    .replica
                    .membership()
                    .members
                    .iter()
                    .filter(|&&m| m != o.peer && m < self.id)
                    .count() as u64;
                if self.checkpoints.staging.is_some()
                    || now.saturating_sub(rx.ready_at) < rank * INSTALL_STAGGER_MS
                {
                    return Ok(());
                }
                if self.replica.holds_checkpoint(
                    o.peer,
                    o.view,
                    o.image.op,
                    o.image.checksum,
                    o.commit,
                ) {
                    let rx = self.checkpoints.incoming.take().unwrap();
                    return self.stage_own(rx);
                }
                if self.replica.status() == fusee_core::vsr::Status::Normal
                    && self.replica.commit() < o.image.op
                    && now.saturating_sub(rx.ready_at) < 1000 + rank * INSTALL_STAGGER_MS
                {
                    // A backup in normal operation: its commit reaches the
                    // base within a round or two. A replica recovering
                    // (restarted, or in a view change) learns no commit
                    // until it holds the state: it installs at once below.
                    return Ok(());
                }
            }
        }
        if rx.prepared.is_some() {
            let (base, sum) = rx
                .tail
                .last()
                .map_or((o.image.op, o.image.checksum), |e| (e.op, e.checksum));
            rx.tail.extend(self.replica.checkpoint_suffix(base, sum));
            let last = rx.tail.last().map_or(o.image.op, |e| e.op);
            if last >= self.replica.commit() {
                if self.replica.status() == fusee_core::vsr::Status::Normal
                    && last < self.replica.durable()
                {
                    return Ok(());
                }
                if self.checkpoints.staging.is_some() {
                    return Ok(());
                }
                let mut rx = self.checkpoints.incoming.take().unwrap();
                let tail = std::mem::take(&mut rx.tail);
                return self.activate_received(rx, tail, None, out);
            }
        }
        if rx.offset < rx.offer.image.len && now.saturating_sub(rx.progress_at) >= 1000 {
            rx.next_source(self.id, &self.replica.membership().members.clone());
        }
        if now.saturating_sub(rx.last_request) >= 100 {
            self.request_checkpoint_page()?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_reachable_members_without_hello_hold_back_the_first_checkpoint() {
        let mut c = Checkpoints::new(PathBuf::from("unused"), None, Vec::new(), false, 3);
        let start = c.reachable[2];
        // At process start every member counts as reachable.
        assert!(c.holds_back(1, start));
        assert!(c.holds_back(2, start + ABSENT_MS - 1));
        // Hello advertises checkpoint support.
        c.peers.insert(1, false);
        assert!(!c.holds_back(1, start));
        // A member out of reach for the whole window no longer holds the log.
        assert!(!c.holds_back(2, start + ABSENT_MS));
        // Reachable again, but without a Hello.
        c.reachable[2] = start + 4 * ABSENT_MS;
        assert!(c.holds_back(2, start + 5 * ABSENT_MS - 1));
        assert!(!c.holds_back(2, start + 5 * ABSENT_MS));
        // Once a generation exists the group passed its first checkpoint.
        c.reachable[2] = start + 9 * ABSENT_MS;
        c.current = Some(Generation::new(
            Image {
                op: 1,
                checksum: 1,
                lsn: 1,
                len: 34,
                sha256: [0; 32],
                shards: 0,
                covered: Vec::new(),
            },
            "engine".into(),
            &[],
            1,
        ));
        assert!(!c.holds_back(2, start + 9 * ABSENT_MS));
        // IDs outside the group never hold anything back.
        c.current = None;
        assert!(!c.holds_back(7, start));
    }
}
