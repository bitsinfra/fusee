//! Replication: a primary-backup protocol in the style of Viewstamped
//! Replication Revisited, as a pure state machine.
//!
//! Each shard group has 3 or 5 replicas. In a view, one replica is primary
//! (`view % n`). The primary appends entries (the shard's records, in
//! canonical encoding, one per LSN) and sends them to the backups at once,
//! before its own flush finishes. A backup appends, flushes, and answers
//! `PrepareOk` with how far it is durable. An entry is **committed** when a
//! quorum (`n / 2 + 1`, the primary included) holds it durably in the
//! current view; the primary releases outputs and opens step gates at the
//! commit position, so nothing leaves the process on state a minority of
//! failures could take back.
//!
//! Entries form a hash chain (`parent` is the previous entry's checksum),
//! so a replica can verify entries it receives from any peer.
//!
//! **View change.** A backup that stops hearing from the primary starts view
//! `v + 1` (`StartViewChange`). With a quorum of those, replicas send their
//! log state (`DoViewChange`) to the new primary, which takes the log with
//! the highest (last normal view, op), fetches any committed entries it is
//! missing, and announces the view (`StartView`). Backups drop uncommitted
//! entries that do not match and fetch what they lack. A primary that is
//! cut off cannot commit (it has no quorum), so its outputs never leave.
//!
//! **Protocol-aware recovery.** A replica restarting with a damaged entry
//! in the middle of its log (see [`crate::segment::Tail::Corrupt`]) does
//! not truncate committed work. It stays `Recovering`, takes no part in view
//! changes, fetches the missing range from peers, verifies it against the
//! hash chain, and rewrites its log from there.
//!
//! The host persists entries, reports durability, saves the view
//! ([`Action::SaveView`]) before sending anything in it, and carries
//! messages. Membership is fixed.

use crate::record::Payload;
use crate::wire::checksum64;
use std::collections::{BTreeMap, BTreeSet};

pub type ReplicaId = u8;
pub type Op = u64;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Entry {
    pub op: Op,
    /// The view in which the entry was first prepared.
    pub view: u64,
    pub parent: u64,
    pub checksum: u64,
    pub body: Payload,
}

impl Entry {
    pub fn new(op: Op, view: u64, parent: u64, body: Payload) -> Self {
        let checksum = Self::sum(op, view, parent, &body);
        Self {
            op,
            view,
            parent,
            checksum,
            body,
        }
    }

    fn sum(op: Op, view: u64, parent: u64, body: &[u8]) -> u64 {
        let mut head = [0u8; 24];
        head[..8].copy_from_slice(&op.to_le_bytes());
        head[8..16].copy_from_slice(&view.to_le_bytes());
        head[16..].copy_from_slice(&parent.to_le_bytes());
        checksum64(&[&head, body])
    }

    pub fn valid(&self) -> bool {
        self.checksum == Self::sum(self.op, self.view, self.parent, &self.body)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Msg {
    Prepare {
        view: u64,
        entries: Vec<Entry>,
        commit: Op,
    },
    PrepareOk {
        view: u64,
        op: Op,
    },
    Commit {
        view: u64,
        commit: Op,
    },
    StartViewChange {
        view: u64,
    },
    DoViewChange {
        view: u64,
        last_normal: u64,
        op: Op,
        commit: Op,
        /// Entries after `commit`.
        entries: Vec<Entry>,
    },
    StartView {
        view: u64,
        op: Op,
        commit: Op,
        entries: Vec<Entry>,
    },
    GetEntries {
        view: u64,
        from: Op,
        to: Op,
    },
    Entries {
        view: u64,
        entries: Vec<Entry>,
        commit: Op,
    },
    /// A recovering member may request an intact island from a partially
    /// damaged peer. Prepared bytes are accepted only with a surviving local
    /// successor that authenticates them, or a peer's committed watermark.
    GetRepair {
        view: u64,
        first: Op,
        last: Op,
    },
    RepairEntries {
        entries: Vec<Entry>,
        commit: Op,
    },
    GetState {
        view: u64,
        after: Op,
    },
    /// Larger state transfers are pulled in byte- and count-bounded pages.
    /// Each page is an ordinary Entries reply from this view's primary.
    StateOffer {
        view: u64,
        after: Op,
        commit: Op,
        through: Op,
    },
    NewState {
        view: u64,
        commit: Op,
        after: Op,
        entries: Vec<Entry>,
    },
    /// Sent by the primary with each heartbeat. A backup normal in `view`
    /// promises not to vote for [`Timing::promise_ms`] and answers with
    /// [`Msg::Pong`]. `stamp` is the primary's clock when it sent it.
    Ping {
        view: u64,
        stamp: u64,
    },
    Pong {
        view: u64,
        stamp: u64,
    },
    /// A member that saw a newer epoch than `epoch` (or a joining one) asks
    /// for the configuration.
    GetEpoch {
        epoch: u64,
    },
    /// The committed reconfiguration entry that began the sender's epoch.
    Epoch {
        entry: Entry,
    },
    /// A member asks the primary to change the membership (`body` is a
    /// reconfiguration body; its epoch is ignored).
    Reconfigure {
        body: Payload,
    },
}

impl Msg {
    /// The view a message belongs to, if it names one.
    fn view(&self) -> Option<u64> {
        match self {
            Msg::Prepare { view, .. }
            | Msg::PrepareOk { view, .. }
            | Msg::Commit { view, .. }
            | Msg::StartViewChange { view }
            | Msg::DoViewChange { view, .. }
            | Msg::StartView { view, .. }
            | Msg::GetEntries { view, .. }
            | Msg::Entries { view, .. }
            | Msg::GetRepair { view, .. }
            | Msg::GetState { view, .. }
            | Msg::StateOffer { view, .. }
            | Msg::NewState { view, .. }
            | Msg::Ping { view, .. }
            | Msg::Pong { view, .. } => Some(*view),
            Msg::RepairEntries { .. }
            | Msg::GetEpoch { .. }
            | Msg::Epoch { .. }
            | Msg::Reconfigure { .. } => None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Action {
    Send {
        to: ReplicaId,
        msg: Msg,
    },
    /// Append these entries to the local log, in order, right after the
    /// current last op; then report durability with [`Replica::on_durable`].
    Persist {
        entries: Vec<Entry>,
    },
    /// Atomically replace everything after `after` with `entries` (which
    /// start at `after + 1`). Until the replacement is durable the old log
    /// must remain intact; a crash leaves one or the other, never a mix.
    /// Used when a view change installs a new log and for protocol-aware
    /// repair (where committed entries are rewritten unchanged).
    Replace {
        after: Op,
        entries: Vec<Entry>,
    },
    /// Entries through `commit` are committed.
    Committed {
        commit: Op,
    },
    /// Persist durably before sending anything that follows. A barrier:
    /// every earlier `Persist` and `Replace` must be durable first, because
    /// `last_normal` vouches for the log it was saved with.
    SaveView {
        view: u64,
        last_normal: u64,
        commit: Op,
    },
    /// This replica became primary (resume workflows) or backup (stop them).
    Role {
        primary: bool,
        view: u64,
    },
    /// Persist this membership durably before anything that follows: from
    /// here on the replica acts in its epoch (or, not a member, retires).
    SaveMembership {
        membership: Membership,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Status {
    Normal,
    ViewChange,
    Recovering,
}

/// What a replica found on its disk at start.
#[derive(Clone, Debug, Default)]
pub struct Recovered {
    pub view: u64,
    pub last_normal: u64,
    pub commit: Op,
    /// Entries in order from `base + 1`.
    pub entries: Vec<Entry>,
    /// Checksum of the entry at `base` (0 when `base` is 0).
    pub base_checksum: u64,
    pub base: Op,
    /// A damaged range inside `entries`' span: these ops must be fetched.
    pub damaged: Option<(Op, Op)>,
    /// The saved membership (`None`: epoch 0, members `0..n`).
    pub membership: Option<Membership>,
    /// A new member joining a running group: `membership` lists whom to
    /// ask; it takes part in nothing until a member tells it the epoch
    /// that includes it.
    pub joining: bool,
}

/// Member IDs are below this (the host keeps one bit per member).
pub const MAX_MEMBERS: usize = 32;
/// The most members a configuration may have.
pub const MAX_GROUP: usize = 9;
const NO_ONE: ReplicaId = ReplicaId::MAX;

/// A view carries its epoch (the configuration it belongs to) in its high
/// 32 bits, so views of different configurations never coincide and the
/// order of views is the order of epochs, then of views within one.
pub fn epoch_of(view: u64) -> u64 {
    view >> 32
}

pub fn first_view(epoch: u64) -> u64 {
    epoch << 32
}

/// The members of one epoch.
///
/// **Membership change.** Membership is fixed within an epoch. A change is
/// a log entry (a reconfiguration body naming the next epoch's members and
/// addresses) that the primary proposes as the last entry of its epoch: it
/// proposes nothing after it. Once that entry commits, every member of the
/// new configuration that holds the log through it changes to the new
/// epoch's first view by an ordinary view change among the new members;
/// members not in it retire; new members fetch the epoch and the log from
/// the members, and take part in no view change until they hold the log
/// through the entry. A change requires that a quorum of the new members
/// are members now (so a quorum of the new epoch starts with the whole
/// committed log): one step can add or remove members, or replace a
/// minority, but not replace a majority.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Membership {
    pub epoch: u64,
    /// In order: the primary of the epoch's view `v` is
    /// `members[(v - first_view(epoch)) % len]`.
    pub members: Vec<ReplicaId>,
    /// The committed reconfiguration entry that began this epoch (none for
    /// epoch 0).
    pub entry: Option<Entry>,
    /// The members this replica knew before (they hold the committed log
    /// through `entry` and serve it to members catching up into the epoch).
    pub previous: Vec<ReplicaId>,
}

impl Membership {
    pub fn initial(n: u8) -> Self {
        Self {
            epoch: 0,
            members: (0..n).collect(),
            entry: None,
            previous: Vec::new(),
        }
    }

    /// From a committed reconfiguration entry.
    pub fn from_entry(e: &Entry) -> Option<Self> {
        let (epoch, members) = decode_reconfig(&e.body)?;
        Some(Self {
            epoch,
            members: members.iter().map(|(id, _)| *id).collect(),
            entry: Some(e.clone()),
            previous: Vec::new(),
        })
    }

    /// Every member's address, as the reconfiguration entry names them
    /// (empty for epoch 0, whose addresses the host configures).
    pub fn addresses(&self) -> Vec<(ReplicaId, String)> {
        self.entry
            .as_ref()
            .and_then(|e| decode_reconfig(&e.body))
            .map_or_else(Vec::new, |(_, members)| members)
    }

    /// The op of the entry that began this epoch.
    pub fn start(&self) -> Op {
        self.entry.as_ref().map_or(0, |e| e.op)
    }

    pub fn contains(&self, id: ReplicaId) -> bool {
        self.members.contains(&id)
    }

    pub fn quorum(&self) -> usize {
        self.members.len() / 2 + 1
    }

    pub fn primary_of(&self, view: u64) -> ReplicaId {
        if epoch_of(view) != self.epoch || self.members.is_empty() {
            return NO_ONE;
        }
        let i = (view - first_view(self.epoch)) % self.members.len() as u64;
        self.members[i as usize]
    }

    /// Checks a requested membership for the next epoch and orders it: the
    /// current `primary` first if it stays (it then leads the new epoch's
    /// first view), the other current members next, new members last.
    pub fn next(
        &self,
        primary: ReplicaId,
        members: &[(ReplicaId, String)],
    ) -> Result<Vec<(ReplicaId, String)>, &'static str> {
        if members.is_empty() || members.len() > MAX_GROUP {
            return Err("a group has 1 to 9 members");
        }
        let mut ids = BTreeSet::new();
        for (id, addr) in members {
            if usize::from(*id) >= MAX_MEMBERS || addr.is_empty() || !ids.insert(*id) {
                return Err("member IDs must be distinct and below 32, each with an address");
            }
        }
        let current: BTreeSet<ReplicaId> = self.members.iter().copied().collect();
        if ids == current {
            return Err("the membership is unchanged");
        }
        let staying = ids.intersection(&current).count();
        if staying < members.len() / 2 + 1 {
            return Err(
                "a quorum of the new members must be members now: replace a minority at a time",
            );
        }
        let rank = |id: &ReplicaId| {
            if *id == primary {
                (0, 0)
            } else if let Some(i) = self.members.iter().position(|m| m == id) {
                (1, i)
            } else {
                (2, members.iter().position(|(m, _)| m == id).unwrap_or(0))
            }
        };
        let mut ordered = members.to_vec();
        ordered.sort_by_key(|(id, _)| rank(id));
        Ok(ordered)
    }
}

const RECONFIG: [u8; 4] = [0xF5, 0x00, b'M', 1];

/// Whether an entry body is a reconfiguration. `0xF5 0x00` starts neither a
/// segment-group batch (a canonical varint), a log batch (`0xF3 0x00`) nor a
/// permit command (`0`).
pub fn is_reconfig(body: &[u8]) -> bool {
    body.starts_with(&RECONFIG)
}

pub fn encode_reconfig(epoch: u64, members: &[(ReplicaId, String)]) -> Payload {
    let mut out = RECONFIG.to_vec();
    put_varint(&mut out, epoch);
    put_varint(&mut out, members.len() as u64);
    for (id, addr) in members {
        out.push(*id);
        put_bytes(&mut out, addr.as_bytes());
    }
    Payload::from(out)
}

pub fn decode_reconfig(body: &[u8]) -> Option<(u64, Vec<(ReplicaId, String)>)> {
    let mut r = Reader::new(body.strip_prefix(&RECONFIG[..])?);
    let epoch = r.varint().ok()?;
    let n = r.varint().ok()?;
    if n == 0 || n > MAX_GROUP as u64 || epoch == 0 {
        return None;
    }
    let mut members = Vec::new();
    for _ in 0..n {
        let id = r.u8().ok()?;
        let addr = String::from_utf8(r.bytes().ok()?.to_vec()).ok()?;
        if usize::from(id) >= MAX_MEMBERS {
            return None;
        }
        members.push((id, addr));
    }
    r.is_empty().then_some((epoch, members))
}

#[derive(Clone, Debug)]
struct Dvc {
    last_normal: u64,
    op: Op,
    commit: Op,
    entries: Vec<Entry>,
}

/// The requester retains its durable log until all pages have arrived. Each
/// continuation is authenticated against the preceding entry's checksum.
struct StateTransfer {
    view: u64,
    primary: ReplicaId,
    after: Op,
    through: Op,
    commit: Op,
    fetched: Vec<Entry>,
}

impl StateTransfer {
    fn request(&self) -> Action {
        Action::Send {
            to: self.primary,
            msg: Msg::GetEntries {
                view: self.view,
                from: self.fetched.last().map_or(self.after, |entry| entry.op) + 1,
                to: self.through,
            },
        }
    }
}

/// A primary-elect stages committed entries from the replica selected by the
/// view-change quorum. The old durable log remains intact until the complete
/// prefix and its selected suffix form one verified chain.
struct Adoption {
    donor: ReplicaId,
    through: Op,
    max_commit: Op,
    suffix: Vec<Entry>,
    fetched: Vec<Entry>,
}

/// Failure detection, in milliseconds of each member's monotonic clock.
///
/// An idle primary sends a heartbeat every `heartbeat_ms`. A backup that
/// hears nothing from its primary for `election_ms`, plus a jitter drawn
/// again for every election from `0..election_ms`, starts a view change, so
/// the loss of a primary is noticed within one to two election timeouts.
#[derive(Clone, Debug)]
pub struct Timing {
    pub heartbeat_ms: u64,
    pub election_ms: u64,
}

impl Default for Timing {
    fn default() -> Self {
        Self {
            heartbeat_ms: 5,
            election_ms: 40,
        }
    }
}

impl Timing {
    /// A heartbeat of at least 1 ms and an election timeout of at least
    /// four heartbeats, so a backup can miss a few before it suspects the
    /// primary.
    pub fn validate(&self) -> Result<(), &'static str> {
        if self.heartbeat_ms == 0 {
            return Err("the heartbeat interval must be at least 1 ms");
        }
        if self.election_ms < 4 * self.heartbeat_ms {
            return Err("the election timeout must be at least four heartbeat intervals");
        }
        if self.election_ms > 600_000 {
            return Err("the election timeout must be at most ten minutes");
        }
        Ok(())
    }

    /// How long a backup promises, after each ping from its primary, not to
    /// take part in a view change: half the election timeout, so an
    /// election that times out normally never breaks a promise.
    pub fn promise_ms(&self) -> u64 {
        (self.election_ms / 2).max(2)
    }

    /// How long after it sent a ping that a quorum answered the primary may
    /// serve reads from its own state. The promise as the slowest allowed
    /// backup clock measures it, as the fastest allowed primary clock
    /// measures it, less a millisecond at each end for clock granularity:
    /// `(P - 1)(1 - ρ)/(1 + ρ) - 1` with `ρ` = [`MAX_CLOCK_DRIFT_PPM`].
    pub fn lease_ms(&self) -> u64 {
        let p = self.promise_ms() - 1;
        (p * (1_000_000 - MAX_CLOCK_DRIFT_PPM) / (1_000_000 + MAX_CLOCK_DRIFT_PPM))
            .saturating_sub(1)
    }

    /// How long a primary that hears from no quorum keeps its role: four
    /// election timeouts, and at least 200 ms. A backup that is merely slow
    /// (a stalled flush, a starting process) answers late; one that lost
    /// this primary has elected another long before.
    pub fn step_down_ms(&self) -> u64 {
        (self.election_ms * 4).max(200)
    }

    /// How long a primary waits for a backup's progress before it resends
    /// a bounded suffix the backup may have lost.
    fn retransmit_ms(&self) -> u64 {
        self.heartbeat_ms.max(10)
    }

    /// How often a member retries a repair, catch-up or adoption request.
    fn repair_ms(&self) -> u64 {
        (self.election_ms / 2).max(20)
    }
}

/// The clock-rate assumption behind read leases: every member's clock
/// (the `now` the host passes in) advances at a rate within this many parts
/// per million of real time, 1 %. Message delays and scheduling pauses are
/// unbounded; only rates are assumed. A clock that stops while time passes
/// (a suspended host whose clock does not count the suspension) breaks it.
pub const MAX_CLOCK_DRIFT_PPM: u64 = 10_000;

/// A small deterministic generator for election jitter (seeded per replica).
fn next_random(state: &mut u64) -> u64 {
    *state = state.wrapping_add(0x9e37_79b9_7f4a_7c15);
    let mut z = *state;
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}

pub struct Replica {
    pub id: ReplicaId,
    pub n: u8,
    view: u64,
    last_normal: u64,
    status: Status,
    log: BTreeMap<Op, Entry>,
    base: Op,
    base_checksum: u64,
    op: Op,
    durable: Op,
    commit: Op,
    acked: Vec<Op>,
    svc: BTreeSet<ReplicaId>,
    dvc: BTreeMap<ReplicaId, Dvc>,
    dvc_sent: bool,
    /// New primary: waiting for committed entries it lacked, through
    /// `.2`, before appending the adopted suffix `.0` and committing `.1`.
    adopting: Option<Adoption>,
    transfer: Option<StateTransfer>,
    damaged: Option<(Op, Op)>,
    repair_parts: BTreeMap<Op, Entry>,
    last_heard: u64,
    last_beat: u64,
    last_repair: u64,
    timing: Timing,
    /// This election's extra wait, from `0..election_ms`, drawn again for
    /// every view change so two members rarely time out together twice.
    jitter: u64,
    rng: u64,
    /// The latest time this replica was told (ticks and messages).
    clock: u64,
    /// Primary: when it last proposed, and when each backup's acknowledged
    /// position last advanced (a heartbeat resends only to a stalled one).
    proposed_at: u64,
    progress_at: Vec<u64>,
    /// When this view change last broadcast its StartViewChange.
    last_svc: u64,
    /// The view this replica led (or was to lead) before it restarted: it
    /// never leads that view again (rule 2).
    retired_view: Option<u64>,
    /// Until this time the replica takes no part in any view change: it
    /// answered a ping of its primary (or it just started and cannot know
    /// what it promised before). See [`Timing::lease_ms`].
    promised_until: u64,
    /// The newest view others asked this replica to change to while it was
    /// bound by a promise; it joins once the promise lapses.
    deferred_view: u64,
    /// The primary's stream to this backup closed and nothing arrived since:
    /// the backup elects as soon as its promise allows.
    primary_lost: bool,
    /// Primary: per member, the newest ping stamp it echoed in this view
    /// (0: none), and when this primary last heard from it.
    lease_acks: Vec<u64>,
    heard_at: Vec<u64>,
    /// Negative control for the simulator: ignore promises.
    #[cfg(test)]
    pub(crate) ignore_promises: bool,
    membership: Membership,
    /// Not a member of its epoch: takes part in nothing, answers GetEpoch.
    retired: bool,
    /// A new member that does not know its epoch yet.
    joining: bool,
    /// A committed reconfiguration seen inside a step, entered at its end.
    pending_epoch: Option<Membership>,
    last_epoch_ask: u64,
    /// In its epoch without the log through the entry that began it: it
    /// fetches that committed prefix from any member (pages in
    /// `catch_up`), verified against the entry, before it takes part.
    epoch_catch_up: bool,
    catch_up: Vec<Entry>,
    /// When each member was last told this epoch's reconfiguration.
    told: Vec<u64>,
}

impl Replica {
    pub fn new(
        id: ReplicaId,
        n: u8,
        mut rec: Recovered,
        timing: Timing,
        seed: u64,
        now: u64,
    ) -> Self {
        let membership = rec
            .membership
            .take()
            .unwrap_or_else(|| Membership::initial(n));
        let joining = rec.joining;
        let retired = !joining && !membership.contains(id);
        let n = membership.members.len() as u8;
        let mut log = BTreeMap::new();
        let mut op = rec.base;
        for e in rec.entries {
            op = e.op;
            log.insert(e.op, e);
        }
        // A replica that crashed during a view change must not act on its
        // log as if it belonged to the new view.
        // A restarted primary must not lead its view again either: entries
        // a quorum of backups held (and may have committed) can be missing
        // from its own disk, and it would reissue those ops differently.
        let was_primary = !joining && membership.primary_of(rec.view) == id;
        let status = if rec.damaged.is_some() {
            Status::Recovering
        } else if rec.view != rec.last_normal || was_primary || joining || retired {
            Status::ViewChange
        } else {
            Status::Normal
        };
        let mut rng = seed;
        let jitter = next_random(&mut rng) % timing.election_ms.max(1);
        let timing_promise = timing.promise_ms();
        if rec.damaged.is_some() {
            op = op.max(rec.commit);
        }
        let mut replica = Self {
            id,
            n,
            view: rec.view,
            last_normal: rec.last_normal,
            status,
            log,
            base: rec.base,
            base_checksum: rec.base_checksum,
            op,
            durable: rec.damaged.map_or(op, |(first, _)| first.saturating_sub(1)),
            commit: rec.commit.min(op),
            acked: vec![0; MAX_MEMBERS],
            svc: BTreeSet::new(),
            dvc: BTreeMap::new(),
            dvc_sent: false,
            adopting: None,
            transfer: None,
            damaged: rec.damaged,
            repair_parts: BTreeMap::new(),
            last_heard: now,
            last_beat: 0,
            last_repair: 0,
            timing,
            jitter,
            rng,
            clock: now,
            proposed_at: now,
            progress_at: vec![now; MAX_MEMBERS],
            last_svc: now,
            retired_view: was_primary.then_some(rec.view),
            promised_until: now.saturating_add(timing_promise),
            deferred_view: 0,
            primary_lost: false,
            lease_acks: vec![0; MAX_MEMBERS],
            heard_at: vec![now; MAX_MEMBERS],
            #[cfg(test)]
            ignore_promises: false,
            membership,
            retired,
            joining,
            pending_epoch: None,
            last_epoch_ask: 0,
            epoch_catch_up: false,
            catch_up: Vec::new(),
            told: vec![0; MAX_MEMBERS],
        };
        // Restarted inside an epoch whose start it never received.
        if !replica.joining && !replica.retired && !replica.holds_start() {
            replica.epoch_catch_up = true;
            replica.status = Status::Recovering;
        }
        replica
    }

    /// Fetches the committed prefix through the entry that began the epoch
    /// from any member: committed entries are the same everywhere.
    fn request_epoch_prefix(&self, out: &mut Vec<Action>) {
        if let Some(start) = &self.membership.entry {
            let from = self.catch_up.last().map_or(self.commit, |e| e.op) + 1;
            let msg = Msg::GetEntries {
                view: self.view,
                from,
                to: start.op,
            };
            self.broadcast(msg.clone(), out);
            // Members the change removed hold it too, and serve it.
            for &to in &self.membership.previous {
                out.push(Action::Send {
                    to,
                    msg: msg.clone(),
                });
            }
        }
    }

    /// Holds the epoch's start now: a sound vote. It looks for the epoch's
    /// primary first (a recovering replica with an intact log); only if
    /// none answers does it start a view change (see `election_due`), so a
    /// member that caught up never disturbs a working primary.
    fn finish_epoch_catch_up(&mut self, out: &mut Vec<Action>) {
        self.epoch_catch_up = false;
        self.catch_up.clear();
        self.status = Status::Recovering;
        self.adopting = None;
        self.transfer = None;
        self.last_heard = self.clock;
        self.last_repair = self.clock;
        self.save_view(out);
        self.broadcast(
            Msg::GetState {
                view: self.view,
                after: self.commit,
            },
            out,
        );
    }

    /// Pages of the committed prefix, verified to chain from this
    /// replica's commit to the entry that began its epoch.
    fn catch_up_epoch(&mut self, from: ReplicaId, entries: Vec<Entry>, out: &mut Vec<Action>) {
        let Some(start) = self.membership.entry.clone() else {
            return;
        };
        // Only a log without the start is replaced: everything after this
        // replica's commit goes, and nothing it acknowledged in this epoch
        // can be there (it never took part).
        if self.holds_start() || self.commit >= start.op {
            return;
        }
        let mut last = self.catch_up.last().map_or(self.commit, |e| e.op);
        let mut parent = self
            .catch_up
            .last()
            .map(|e| e.checksum)
            .or_else(|| self.checksum_at(self.commit));
        let before = self.catch_up.len();
        for e in entries {
            if e.op <= last {
                continue;
            }
            if e.op != last + 1 || e.op > start.op || Some(e.parent) != parent || !e.valid() {
                break;
            }
            parent = Some(e.checksum);
            last = e.op;
            self.catch_up.push(e);
        }
        if last == start.op && parent == Some(start.checksum) {
            let fetched = std::mem::take(&mut self.catch_up);
            let commit = self.commit;
            if self.replace(commit, fetched, out) {
                self.set_commit(start.op, out);
                self.pending_epoch = None;
                self.finish_epoch_catch_up(out);
            }
        } else if self.catch_up.len() > before {
            out.push(Action::Send {
                to: from,
                msg: Msg::GetEntries {
                    view: self.view,
                    from: last + 1,
                    to: start.op,
                },
            });
        }
    }

    /// Whether this replica may take part in a view change at `now`: a
    /// member of its epoch, holding the log through the entry that began
    /// it, and bound by no promise.
    fn may_vote(&self, now: u64) -> bool {
        if self.retired || self.joining || !self.holds_start() {
            return false;
        }
        #[cfg(test)]
        if self.ignore_promises {
            return true;
        }
        now >= self.promised_until
    }

    /// Whether this replica holds the log through the reconfiguration that
    /// began its epoch, so its log is a sound vote in the epoch.
    fn holds_start(&self) -> bool {
        let start = self.membership.start();
        start == 0
            || self.base >= start
            || self.checksum_at(start) == self.membership.entry.as_ref().map(|e| e.checksum)
    }

    /// A reconfiguration is in the uncommitted log: the epoch ends with it,
    /// so nothing more is proposed in it.
    pub fn sealed(&self) -> bool {
        self.log
            .range(self.commit + 1..)
            .any(|(_, e)| is_reconfig(&e.body))
    }

    /// The next view to change to: never one of an earlier epoch.
    fn next_view(&self) -> u64 {
        (self.view + 1).max(first_view(self.membership.epoch))
    }

    fn quorum(&self) -> usize {
        self.membership.quorum()
    }

    pub fn membership(&self) -> &Membership {
        &self.membership
    }

    /// Not a member of its epoch any more: it takes part in nothing.
    pub fn retired(&self) -> bool {
        self.retired
    }

    /// A new member that has not yet learned the epoch that includes it.
    pub fn joining(&self) -> bool {
        self.joining
    }

    /// Primary: proposes the next epoch's membership as the last entry of
    /// this epoch (see [`Membership`]). Returns the entry's op; the change
    /// is done once this replica's membership reaches the next epoch.
    pub fn reconfigure(
        &mut self,
        members: &[(ReplicaId, String)],
        out: &mut Vec<Action>,
    ) -> Result<Op, &'static str> {
        if !self.is_primary() {
            return Err("this replica is not the primary");
        }
        if self.sealed() {
            return Err("a membership change is already in progress");
        }
        let ordered = self.membership.next(self.id, members)?;
        let body = encode_reconfig(self.membership.epoch + 1, &ordered);
        self.propose(body, out)
            .ok_or("this replica is not the primary")
    }

    /// Enters the next epoch once its reconfiguration has committed.
    /// `holds`: this replica holds the log through that entry; otherwise it
    /// catches up from the epoch's primary before it takes part.
    fn enter_epoch(&mut self, mut m: Membership, holds: bool, out: &mut Vec<Action>) {
        let now = self.clock;
        if !self.joining {
            m.previous = self
                .membership
                .members
                .iter()
                .copied()
                .filter(|id| !m.members.contains(id))
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect();
        }
        self.membership = m.clone();
        self.n = m.members.len() as u8;
        self.joining = false;
        out.push(Action::SaveMembership {
            membership: m.clone(),
        });
        self.svc.clear();
        self.dvc.clear();
        self.dvc_sent = false;
        self.adopting = None;
        self.transfer = None;
        self.deferred_view = 0;
        self.primary_lost = false;
        self.lease_acks = vec![0; MAX_MEMBERS];
        let first = first_view(m.epoch);
        if !m.contains(self.id) {
            self.retired = true;
            self.status = Status::ViewChange;
            self.view = self.view.max(first);
            self.save_view(out);
            out.push(Action::Role {
                primary: false,
                view: self.view,
            });
            return;
        }
        self.retired = false;
        if holds && self.status != Status::Recovering {
            // The old epoch's traffic no longer matters: change to the new
            // epoch's first view as soon as promises allow.
            self.status = Status::ViewChange;
            out.push(Action::Role {
                primary: false,
                view: self.view,
            });
            if self.may_vote(now) {
                self.start_view_change(first.max(self.view + 1), now, out);
            } else {
                self.deferred_view = first;
            }
        } else {
            self.view = self.view.max(first);
            self.status = Status::Recovering;
            self.last_repair = now;
            self.last_heard = now;
            self.catch_up.clear();
            self.epoch_catch_up = !self.holds_start();
            self.save_view(out);
            out.push(Action::Role {
                primary: false,
                view: self.view,
            });
            if self.epoch_catch_up {
                self.request_epoch_prefix(out);
            }
            self.broadcast(
                Msg::GetState {
                    view: self.view,
                    after: self.commit,
                },
                out,
            );
        }
    }

    /// Ends a step: enters an epoch whose reconfiguration committed in it,
    /// and drops messages addressed to the primary of a view outside this
    /// replica's epoch (there is none to send them to).
    fn after_step(&mut self, out: &mut Vec<Action>) {
        if let Some(m) = self.pending_epoch.take()
            && m.epoch > self.membership.epoch
        {
            self.enter_epoch(m, true, out);
        }
        out.retain(|a| !matches!(a, Action::Send { to, .. } if *to == NO_ONE));
    }

    fn ask_epoch(&mut self, from: ReplicaId, now: u64, out: &mut Vec<Action>) {
        if now.saturating_sub(self.last_epoch_ask) >= self.timing.retransmit_ms() {
            self.last_epoch_ask = now;
            out.push(Action::Send {
                to: from,
                msg: Msg::GetEpoch {
                    epoch: if self.joining {
                        0
                    } else {
                        self.membership.epoch
                    },
                },
            });
        }
    }

    /// The sender began `entry`'s epoch (or a later one), so the entry is
    /// committed.
    fn on_epoch(&mut self, entry: Entry, out: &mut Vec<Action>) {
        let Some(m) = Membership::from_entry(&entry) else {
            return;
        };
        if !entry.valid()
            || (self.joining && !m.contains(self.id))
            || (!self.joining && m.epoch <= self.membership.epoch)
        {
            return;
        }
        let holds = !self.joining
            && (self.base >= entry.op || self.checksum_at(entry.op) == Some(entry.checksum));
        if holds && self.damaged.is_none() {
            // Everything through it is committed history in this log.
            self.set_commit(entry.op, out);
            self.pending_epoch = None;
            self.enter_epoch(m, true, out);
        } else {
            self.enter_epoch(m, false, out);
        }
    }

    /// Primary: the time (on this replica's clock) until which it may serve
    /// reads from its own state: no other replica can lead a newer view
    /// before then. `None` when it holds no lease.
    pub fn lease_until(&self) -> Option<u64> {
        if !self.is_primary() {
            return None;
        }
        let need = self.quorum() - 1;
        let mut stamps: Vec<u64> = self
            .others()
            .map(|r| self.lease_acks[usize::from(r)])
            .collect();
        stamps.sort_unstable_by(|a, b| b.cmp(a));
        let oldest = match need {
            0 => return None,
            k => *stamps.get(k - 1)?,
        };
        (oldest > 0).then(|| oldest + self.timing.lease_ms())
    }

    pub fn lease_valid(&self, now: u64) -> bool {
        self.lease_until().is_some_and(|until| now < until)
    }

    /// The host noticed that `from`'s stream to this replica closed. If it
    /// is this backup's primary, the backup does not wait out the election
    /// timeout: it elects as soon as its promise allows (a crashed process
    /// closes its sockets at once; a silent one is still caught by the
    /// timeout).
    pub fn peer_closed(&mut self, from: ReplicaId, now: u64) {
        self.clock = self.clock.max(now);
        if self.status == Status::Normal && from != self.id && from == self.primary_of(self.view) {
            self.primary_lost = true;
        }
    }

    /// Whether a tick at `now` would start a view change because the
    /// primary went quiet. A host whose thread was descheduled reads its
    /// sockets first: the heartbeats may be waiting there.
    pub fn election_due(&self, now: u64) -> bool {
        match self.status {
            Status::Normal if self.is_primary() => false,
            Status::Normal if self.primary_lost && self.may_vote(now) => true,
            Status::Normal | Status::ViewChange => {
                now.saturating_sub(self.last_heard) >= self.timing.election_ms + self.jitter
            }
            Status::Recovering => {
                self.damaged.is_none()
                    && now.saturating_sub(self.last_heard)
                        >= self.timing.election_ms * 2 + self.jitter
            }
        }
    }

    pub fn view(&self) -> u64 {
        self.view
    }

    pub fn timing(&self) -> &Timing {
        &self.timing
    }

    pub fn status(&self) -> Status {
        self.status
    }

    pub fn last_normal(&self) -> u64 {
        self.last_normal
    }

    pub fn op(&self) -> Op {
        self.op
    }

    pub fn commit(&self) -> Op {
        self.commit
    }

    pub fn durable(&self) -> Op {
        self.durable
    }

    /// Primary: whether its own log becoming durable through its last op
    /// would advance the commit position, because enough other members
    /// already acknowledged entries above it. The host asks before it
    /// waits, to know whether its own write finishing must wake it; when
    /// this is false only another acknowledgment can commit more.
    pub fn commit_waits_on_own_durability(&self) -> bool {
        if !self.is_primary() || self.durable >= self.op {
            return false;
        }
        let q = self.quorum();
        if q < 2 {
            return true;
        }
        let mut acks: Vec<Op> = self.others().map(|r| self.acked[usize::from(r)]).collect();
        acks.sort_unstable_by(|a, b| b.cmp(a));
        acks.get(q - 2)
            .is_some_and(|&a| a.min(self.op) > self.commit)
    }

    /// The primary of `view` in this replica's epoch (none for another).
    pub fn primary_of(&self, view: u64) -> ReplicaId {
        if self.joining {
            return NO_ONE;
        }
        self.membership.primary_of(view)
    }

    pub fn is_primary(&self) -> bool {
        !self.retired && self.primary_of(self.view) == self.id && self.status == Status::Normal
    }

    pub fn entry(&self, op: Op) -> Option<&Entry> {
        self.log.get(&op)
    }

    pub fn entries(&self) -> impl DoubleEndedIterator<Item = &Entry> {
        self.log.values()
    }

    /// The entries above `op`, in order, without walking those below.
    pub fn entries_after(&self, op: Op) -> impl DoubleEndedIterator<Item = &Entry> {
        self.log
            .range((std::ops::Bound::Excluded(op), std::ops::Bound::Unbounded))
            .map(|(_, e)| e)
    }

    /// First position represented by the durable checkpoint.
    pub fn base(&self) -> Op {
        self.base
    }

    /// A checkpoint donor can serve this range even while changing views.
    /// Never expose uncommitted entries as a repair for a decided position.
    pub fn committed_page(&self, first: Op, last: Op) -> Vec<Entry> {
        self.page(first, last.min(self.commit))
    }

    /// An immutable local image is already durable. Keep every operation
    /// after it, including prepares a backup has acknowledged in this view.
    pub fn compact_through(&mut self, op: Op, checksum: u64) -> bool {
        self.compact_through_taking(op, checksum).is_some()
    }

    /// [`compact_through`](Self::compact_through), returning the entries it
    /// dropped (for a host to free off its I/O thread), or `None` if it
    /// refused.
    pub fn compact_through_taking(&mut self, op: Op, checksum: u64) -> Option<BTreeMap<Op, Entry>> {
        if op <= self.base
            || op == Op::MAX
            || op > self.commit
            || self.checksum_at(op) != Some(checksum)
            || self.damaged.is_some()
        {
            return None;
        }
        let kept = self.log.split_off(&(op + 1));
        let dropped = std::mem::replace(&mut self.log, kept);
        self.base = op;
        self.base_checksum = checksum;
        Some(dropped)
    }

    /// Only the established primary, or the donor selected by a view-change
    /// quorum, can supply the contents of a remote checkpoint.
    pub fn accepts_checkpoint(
        &self,
        from: ReplicaId,
        view: u64,
        base: Op,
        donor_commit: Op,
    ) -> bool {
        // Catching up into a new epoch, perhaps before it has a primary: a
        // committed image from any member of the epoch.
        if self.epoch_catch_up
            && !self.holds_start()
            && self.damaged.is_none()
            && self.adopting.is_none()
            && from != self.id
            && (self.membership.contains(from) || self.membership.previous.contains(&from))
            && epoch_of(view) == self.membership.epoch
            && base > self.base
            && base != Op::MAX
            && donor_commit >= base
        {
            return true;
        }
        if from == self.id
            || !self.membership.contains(from)
            || (view != self.view && (self.status != Status::Recovering || self.damaged.is_none()))
            // A damaged member may be the only one to have entered its view.
            // An older primary's committed image can repair it only if the
            // image covers even its highest prepared position. Keep our view.
            || (view < self.view && base < self.op)
            || base <= self.base
            || base == Op::MAX
            || donor_commit < base
        {
            return false;
        }
        if let Some(a) = &self.adopting {
            return a.donor == from && base <= a.through && donor_commit >= a.through;
        }
        self.primary_of(view) == from
    }

    /// Preserve the remaining durable suffix when it chains to the checkpoint.
    /// Corrupt entries are never used to advance the new base.
    pub fn checkpoint_suffix(&self, base: Op, checksum: u64) -> Vec<Entry> {
        let mut parent = checksum;
        let mut next = base.saturating_add(1);
        self.log
            .range(next..)
            .map(|(_, e)| e)
            .take_while(|e| {
                if e.op != next || e.parent != parent || !e.valid() {
                    return false;
                }
                parent = e.checksum;
                next = next.saturating_add(1);
                true
            })
            .cloned()
            .collect()
    }

    pub fn checkpoint_progress(&mut self, now: u64) {
        self.last_heard = now;
    }

    /// Validate against every known committed checksum before replacing any
    /// durable files. A receiver with a higher committed watermark requires
    /// a complete chain through that watermark.
    pub fn checkpoint_matches(&self, base: Op, checksum: u64, tail: &[Entry]) -> bool {
        self.checkpoint_matches_checked(base, checksum, tail, 0)
    }

    /// The parts of [`checkpoint_matches`](Self::checkpoint_matches) that
    /// need nothing of the replica, which a host checks off its I/O thread:
    /// consecutive positions after `base`, each entry's parent the previous
    /// checksum (`checksum` first), each entry intact.
    pub fn chain_intact(base: Op, checksum: u64, tail: &[Entry]) -> bool {
        let mut parent = checksum;
        for (op, e) in (base.saturating_add(1)..).zip(tail) {
            if e.op != op || e.parent != parent || !e.valid() {
                return false;
            }
            parent = e.checksum;
        }
        true
    }

    /// [`checkpoint_matches`](Self::checkpoint_matches) for a tail whose
    /// first `intact` entries [`chain_intact`](Self::chain_intact) checked
    /// already: those are not hashed again.
    pub fn checkpoint_matches_checked(
        &self,
        base: Op,
        checksum: u64,
        tail: &[Entry],
        intact: usize,
    ) -> bool {
        if base <= self.base || base == Op::MAX {
            return false;
        }
        // A damaged replica restarted with holes: it may have acknowledged
        // every entry it still holds after them (its durable position was
        // lowered to the first hole), and it may be the only member of its
        // quorum that holds some. An image must not drop them.
        let preserve = if self.damaged.is_some() {
            self.op
        } else if self.status == Status::Normal {
            self.durable
        } else {
            self.commit
        };
        if base <= preserve && self.checksum_at(base).is_some_and(|old| old != checksum) {
            return false;
        }
        // The entries this replica holds up to `preserve`, walked beside
        // the tail (no lookup per entry).
        let mut own = (preserve > base)
            .then(|| self.log.range(base + 1..=preserve))
            .into_iter()
            .flatten()
            .peekable();
        let mut parent = checksum;
        for (i, (op, e)) in (base + 1..).zip(tail).enumerate() {
            if e.op != op || e.parent != parent || (i >= intact && !e.valid()) {
                return false;
            }
            if op <= preserve {
                while own.peek().is_some_and(|(o, _)| **o < op) {
                    own.next();
                }
                if let Some((o, old)) = own.peek()
                    && **o == op
                    && old.checksum != e.checksum
                {
                    return false;
                }
            }
            parent = e.checksum;
        }
        tail.last().map_or(base, |e| e.op) >= preserve.max(self.commit)
    }

    /// The host has atomically installed this image and the checked suffix.
    /// In the normal path all acknowledged prepares must survive installation.
    pub fn installed_checkpoint(
        &mut self,
        from: ReplicaId,
        view: u64,
        base: Op,
        checksum: u64,
        donor_commit: Op,
        tail: Vec<Entry>,
        now: u64,
        out: &mut Vec<Action>,
    ) -> bool {
        if !self.accepts_checkpoint(from, view, base, donor_commit)
            || !self.checkpoint_matches(base, checksum, &tail)
        {
            return false;
        }
        let last = tail.last().map_or(base, |e| e.op);
        let log = tail.into_iter().map(|e| (e.op, e)).collect();
        self.install_matched(from, view, base, checksum, log, last, now, out)
            .is_some()
    }

    /// The rest of [`installed_checkpoint`](Self::installed_checkpoint) for
    /// a tail the host checked (`accepts_checkpoint` and
    /// `checkpoint_matches_checked`), given as the log it becomes (built
    /// off the host's I/O thread; `last` its last position). Returns the
    /// log it replaced, for the host to free elsewhere, or `None` if it
    /// refused.
    #[allow(clippy::too_many_arguments)]
    pub fn install_matched(
        &mut self,
        from: ReplicaId,
        view: u64,
        base: Op,
        checksum: u64,
        log: BTreeMap<Op, Entry>,
        last: Op,
        now: u64,
        out: &mut Vec<Action>,
    ) -> Option<BTreeMap<Op, Entry>> {
        if self.status == Status::Normal && last < self.durable {
            return None;
        }
        self.base = base;
        self.base_checksum = checksum;
        self.view = self.view.max(view);
        let old = std::mem::replace(&mut self.log, log);
        self.op = last;
        self.durable = last;
        self.commit = self.commit.max(base);
        self.damaged = None;
        self.repair_parts.clear();
        self.transfer = None;
        self.catch_up.clear();
        self.last_heard = now;
        self.last_repair = now;
        if self.epoch_catch_up && self.status != Status::Normal {
            // Recovering into a new epoch: the next tick takes part if the
            // image covered the epoch's start, else keeps fetching.
            self.status = Status::Recovering;
            self.save_view(out);
            return Some(old);
        }
        if let Some(mut a) = self.adopting.take() {
            a.fetched.clear();
            if self.commit == a.through {
                let suffix = a
                    .suffix
                    .into_iter()
                    .filter(|e| e.op > self.commit)
                    .collect();
                self.finish_adoption(self.commit, suffix, a.max_commit, out);
            } else {
                self.adopting = Some(a);
                self.request_adoption_page(out);
            }
        } else if self.status == Status::Normal {
            // This backup was already in this view and kept its entire
            // acknowledged suffix. It may keep participating during compaction.
            self.save_view(out);
            out.push(Action::Send {
                to: from,
                msg: Msg::PrepareOk {
                    view,
                    op: self.durable,
                },
            });
        } else {
            self.status = Status::Recovering;
            self.save_view(out);
            out.push(Action::Send {
                to: from,
                msg: Msg::GetState {
                    view: self.view,
                    after: self.commit,
                },
            });
        }
        Some(old)
    }

    /// Whether a received checkpoint at `base` can be installed by
    /// compacting this replica's own log ([`install_own`](Self::install_own)):
    /// a backup in normal operation that holds `base`, committed, with this
    /// checksum, and nothing to repair or adopt.
    pub fn holds_checkpoint(
        &self,
        from: ReplicaId,
        view: u64,
        base: Op,
        checksum: u64,
        donor_commit: Op,
    ) -> bool {
        self.status == Status::Normal
            && self.adopting.is_none()
            && !self.epoch_catch_up
            && self.damaged.is_none()
            && base > self.base
            && base <= self.commit
            && self.checksum_at(base) == Some(checksum)
            && self.accepts_checkpoint(from, view, base, donor_commit)
    }

    /// Installs a received checkpoint whose base this replica's own log
    /// holds ([`holds_checkpoint`](Self::holds_checkpoint)): the host has
    /// made the image and this log's entries after `base` its durable
    /// generation, so the log is compacted rather than replaced, as
    /// [`install_matched`](Self::install_matched) would leave it. Returns the
    /// entries dropped, for the host to free elsewhere, or `None` if it
    /// refused.
    pub fn install_own(
        &mut self,
        from: ReplicaId,
        view: u64,
        base: Op,
        checksum: u64,
        donor_commit: Op,
        now: u64,
        out: &mut Vec<Action>,
    ) -> Option<BTreeMap<Op, Entry>> {
        if !self.holds_checkpoint(from, view, base, checksum, donor_commit) {
            return None;
        }
        let dropped = self.compact_through_taking(base, checksum)?;
        self.view = self.view.max(view);
        self.repair_parts.clear();
        self.transfer = None;
        self.catch_up.clear();
        self.last_heard = now;
        self.last_repair = now;
        // As `install_matched` in the normal case: still in this view, with
        // its whole acknowledged suffix.
        self.save_view(out);
        out.push(Action::Send {
            to: from,
            msg: Msg::PrepareOk {
                view,
                op: self.durable,
            },
        });
        Some(dropped)
    }

    fn checksum_at(&self, op: Op) -> Option<u64> {
        if op == self.base {
            return Some(self.base_checksum);
        }
        self.log.get(&op).map(|e| e.checksum)
    }

    /// The other members of this replica's epoch (for a joining replica,
    /// the members it was told to ask).
    fn others(&self) -> impl Iterator<Item = ReplicaId> + '_ {
        self.membership
            .members
            .iter()
            .copied()
            .filter(move |r| *r != self.id)
    }

    fn broadcast(&self, msg: Msg, out: &mut Vec<Action>) {
        for r in self.others() {
            out.push(Action::Send {
                to: r,
                msg: msg.clone(),
            });
        }
    }

    /// A single locally admissible entry may exceed the preferred page size.
    fn page(&self, first: Op, last: Op) -> Vec<Entry> {
        let mut entries = Vec::new();
        let mut bytes = 0usize;
        for op in first..=last.min(self.op) {
            let Some(entry) = self.log.get(&op) else {
                break;
            };
            let size = entry.body.len().saturating_add(64);
            if !entries.is_empty()
                && (entries.len() == 4096 || bytes.saturating_add(size) > 1 << 20)
            {
                break;
            }
            bytes = bytes.saturating_add(size);
            entries.push(entry.clone());
        }
        entries
    }

    fn range(&self, after: Op, to: Op) -> Vec<Entry> {
        if after >= to {
            return Vec::new();
        }
        self.log
            .range(after + 1..=to)
            .map(|(_, e)| e.clone())
            .collect()
    }

    /// Appends entries that continue the local log; stops at the first that
    /// does not. Returns how many were appended.
    fn append(&mut self, entries: &[Entry], out: &mut Vec<Action>) -> usize {
        let mut add = Vec::new();
        for e in entries {
            if e.op <= self.op {
                continue;
            }
            if e.op != self.op + 1 || !e.valid() || Some(e.parent) != self.checksum_at(self.op) {
                break;
            }
            self.op = e.op;
            self.log.insert(e.op, e.clone());
            add.push(e.clone());
        }
        let n = add.len();
        if n > 0 {
            out.push(Action::Persist { entries: add });
        }
        n
    }

    /// Replaces the log after `after` with `entries` if they form a valid
    /// chain from it. Returns whether it did.
    fn replace(&mut self, after: Op, entries: Vec<Entry>, out: &mut Vec<Action>) -> bool {
        let after = after.max(self.base);
        let Some(mut prev) = self.checksum_at(after) else {
            return false;
        };
        for (expect, e) in (after + 1..).zip(entries.iter()) {
            if e.op != expect || !e.valid() || e.parent != prev {
                return false;
            }
            prev = e.checksum;
        }
        // Nothing to do if the log already ends exactly like this.
        if self.op == after + entries.len() as u64
            && entries.iter().all(|e| {
                self.log
                    .get(&e.op)
                    .is_some_and(|x| x.checksum == e.checksum)
            })
        {
            return true;
        }
        self.log.split_off(&(after + 1));
        for e in &entries {
            self.log.insert(e.op, e.clone());
        }
        self.op = after + entries.len() as u64;
        self.durable = self.durable.min(after);
        self.commit = self.commit.min(self.op);
        out.push(Action::Replace { after, entries });
        true
    }

    fn set_commit(&mut self, c: Op, out: &mut Vec<Action>) {
        let c = c.min(self.op);
        if c > self.commit {
            let old = self.commit;
            self.commit = c;
            out.push(Action::Committed { commit: c });
            // A committed reconfiguration ends this epoch. It is entered at
            // the end of the step, once the handler's own work is done.
            if let Some(m) = self
                .log
                .range(old + 1..=c)
                .rev()
                .find(|(_, e)| is_reconfig(&e.body))
                .and_then(|(_, e)| Membership::from_entry(e))
                && m.epoch > self.membership.epoch
                && self
                    .pending_epoch
                    .as_ref()
                    .is_none_or(|p| p.epoch < m.epoch)
            {
                self.pending_epoch = Some(m);
            }
        }
    }

    fn save_view(&self, out: &mut Vec<Action>) {
        out.push(Action::SaveView {
            view: self.view,
            last_normal: self.last_normal,
            commit: self.commit,
        });
    }

    /// Primary: appends a new entry and replicates it. Returns its op.
    pub fn propose(&mut self, body: Payload, out: &mut Vec<Action>) -> Option<Op> {
        self.propose_many(std::iter::once(body), out)
            .map(|(first, _)| first)
    }

    /// Primary: appends one entry per body, in order, and replicates them in
    /// one Prepare (one frame per backup) and one local write. Returns the
    /// first and last op, or `None` (nothing appended) if this replica is
    /// not the primary or there is no body.
    pub fn propose_many(
        &mut self,
        bodies: impl IntoIterator<Item = Payload>,
        out: &mut Vec<Action>,
    ) -> Option<(Op, Op)> {
        // After a reconfiguration nothing more belongs to this epoch.
        if !self.is_primary() || self.sealed() {
            return None;
        }
        let mut parent = self.checksum_at(self.op)?;
        let mut entries = Vec::new();
        for body in bodies {
            let e = Entry::new(self.op + 1, self.view, parent, body);
            parent = e.checksum;
            self.op = e.op;
            self.log.insert(e.op, e.clone());
            entries.push(e);
        }
        let (first, last) = (entries.first()?.op, self.op);
        self.proposed_at = self.clock;
        // Send the Prepare before the local write so both replicas can
        // persist concurrently. An entry commits once any quorum holds it
        // durably, so the local write need not finish first.
        self.broadcast(
            Msg::Prepare {
                view: self.view,
                entries: entries.clone(),
                commit: self.commit,
            },
            out,
        );
        out.push(Action::Persist { entries });
        Some((first, last))
    }

    /// The local log is durable through `op`.
    pub fn on_durable(&mut self, op: Op, out: &mut Vec<Action>) {
        self.durable_through(op, out);
        self.after_step(out);
    }

    fn durable_through(&mut self, op: Op, out: &mut Vec<Action>) {
        let op = op.min(self.op);
        if op <= self.durable {
            return;
        }
        self.durable = op;
        if self.status != Status::Normal {
            return;
        }
        if self.is_primary() {
            self.acked[usize::from(self.id)] = self.durable;
            self.recompute_commit(out);
        } else {
            out.push(Action::Send {
                to: self.primary_of(self.view),
                msg: Msg::PrepareOk {
                    view: self.view,
                    op: self.durable,
                },
            });
        }
    }

    fn recompute_commit(&mut self, out: &mut Vec<Action>) {
        let mut acks: Vec<Op> = self.acked.clone();
        acks[usize::from(self.id)] = self.durable;
        acks.sort_unstable_by(|a, b| b.cmp(a));
        let q = self.quorum();
        if let Some(&c) = acks.get(q - 1) {
            self.set_commit(c, out);
        }
    }

    pub fn on_tick(&mut self, now: u64, out: &mut Vec<Action>) {
        self.clock = self.clock.max(now);
        if self.retired {
            return;
        }
        if self.joining {
            // Ask the members it was given until one names an epoch that
            // includes this replica.
            if now.saturating_sub(self.last_epoch_ask) >= self.timing.repair_ms() {
                self.last_epoch_ask = now;
                self.broadcast(Msg::GetEpoch { epoch: 0 }, out);
            }
            return;
        }
        self.tick(now, out);
        self.after_step(out);
    }

    fn tick(&mut self, now: u64, out: &mut Vec<Action>) {
        match self.status {
            Status::Normal if self.is_primary() => {
                // Step down when no quorum has been heard from for
                // `step_down_ms`: this primary can commit nothing, and its
                // engine should stop rather than wait.
                let quiet = self.timing.step_down_ms();
                let fresh = self
                    .others()
                    .filter(|r| now.saturating_sub(self.heard_at[usize::from(*r)]) < quiet)
                    .count();
                if fresh + 1 < self.quorum() {
                    self.start_view_change(self.next_view(), now, out);
                    return;
                }
                if now.saturating_sub(self.last_beat) >= self.timing.heartbeat_ms {
                    self.last_beat = now;
                    let resend = self.timing.retransmit_ms();
                    let backups: Vec<ReplicaId> = self.others().collect();
                    for to in backups {
                        let ack = self.acked[usize::from(to)];
                        // Under load acknowledgments trail the log by a
                        // round trip: resend only once this backup's
                        // position has stalled and nothing new went out.
                        let stalled = now.saturating_sub(self.proposed_at) >= resend
                            && now.saturating_sub(self.progress_at[usize::from(to)]) >= resend;
                        let msg = if ack < self.op && stalled {
                            // A dropped final Prepare or PrepareOk has no later
                            // proposal to drive repair. Retry a bounded suffix.
                            self.progress_at[usize::from(to)] = now;
                            Msg::Prepare {
                                view: self.view,
                                entries: self.page(ack + 1, self.op.min(ack.saturating_add(32))),
                                commit: self.commit,
                            }
                        } else {
                            Msg::Commit {
                                view: self.view,
                                commit: self.commit,
                            }
                        };
                        out.push(Action::Send { to, msg });
                        // Renews the read lease (a separate message, so a
                        // member that does not know it still hears the
                        // heartbeat above).
                        out.push(Action::Send {
                            to,
                            msg: Msg::Ping {
                                view: self.view,
                                stamp: now,
                            },
                        });
                    }
                }
            }
            Status::Normal | Status::ViewChange => {
                if self.adopting.is_some()
                    && now.saturating_sub(self.last_repair) >= self.timing.retransmit_ms()
                {
                    self.last_repair = now;
                    self.request_adoption_page(out);
                }
                if !self.may_vote(now) {
                    return;
                }
                if self.deferred_view > self.view {
                    // Asked to change views while bound by a promise.
                    let view = self.deferred_view;
                    self.start_view_change(view, now, out);
                } else if self.election_due(now) {
                    self.start_view_change(self.next_view(), now, out);
                } else {
                    // A restarted member may have reached a quorum of
                    // StartViewChange while it abstained.
                    self.maybe_send_dvc(out);
                    self.maybe_start_view(out);
                    self.retransmit_view_change(now, out);
                }
            }
            Status::Recovering => {
                if self.epoch_catch_up && self.holds_start() && self.damaged.is_none() {
                    // A committed image covered the epoch's start.
                    self.finish_epoch_catch_up(out);
                    return;
                }
                if now.saturating_sub(self.last_repair) >= self.timing.repair_ms() {
                    self.last_repair = now;
                    self.request_repair(out);
                }
                if !self.may_vote(now) || self.damaged.is_some() {
                    return;
                }
                // An intact log may vote: if no primary answers, elect one.
                if self.deferred_view > self.view {
                    let view = self.deferred_view;
                    self.start_view_change(view, now, out);
                } else if self.election_due(now) {
                    self.start_view_change(self.next_view(), now, out);
                }
            }
        }
    }

    /// The durable suffix can have several holes. Request the first missing
    /// span, preserving the intact islands between them.
    fn repair_gap(&self, first: Op, last: Op) -> Option<(Op, Op)> {
        let missing = (first..=last)
            .find(|op| !self.log.contains_key(op) && !self.repair_parts.contains_key(op))?;
        let next = self.log.range(missing..=last).next().map(|(n, _)| *n);
        Some((missing, next.map_or(last, |op| op - 1)))
    }

    fn repair_entries(
        &mut self,
        entries: Vec<Entry>,
        committed: Op,
        now: u64,
        out: &mut Vec<Action>,
    ) {
        let Some((first, last)) = self.damaged else {
            return;
        };
        let Some((start, end)) = self.repair_gap(first, last) else {
            return;
        };
        let prev = |n: Op| {
            self.repair_parts
                .get(&n)
                .map(|e| e.checksum)
                .or_else(|| self.checksum_at(n))
        };
        let Some(mut parent) = prev(start - 1) else {
            return;
        };
        let mut expected = start;
        for e in entries {
            if e.op < start {
                continue;
            }
            if e.op != expected || e.op > end || e.parent != parent || !e.valid() {
                break;
            }
            if e.op == end {
                let successor = self.log.get(&(end + 1));
                if successor.is_some_and(|next| next.parent != e.checksum) {
                    // A speculative page may have followed another branch.
                    // Drop only staged guesses leading into this anchor.
                    let left = self
                        .log
                        .range(first..start)
                        .next_back()
                        .map_or(first, |(n, _)| n + 1);
                    self.repair_parts.retain(|op, _| *op < left || *op > end);
                    return;
                }
                if successor.is_none() && committed < end {
                    break;
                }
            }
            parent = e.checksum;
            expected += 1;
            self.repair_parts.insert(e.op, e);
        }
        if expected == start {
            return;
        }
        if let Some((next, end)) = self.repair_gap(first, last) {
            self.broadcast(
                Msg::GetRepair {
                    view: self.view,
                    first: next,
                    last: end,
                },
                out,
            );
            return;
        }
        let mut replacement = Vec::new();
        for op in first..=self.op {
            let Some(e) = self.repair_parts.get(&op).or_else(|| self.log.get(&op)) else {
                return;
            };
            replacement.push(e.clone());
        }
        let commit = self.commit;
        if !self.replace(first - 1, replacement, out) {
            self.repair_parts.clear();
            return;
        }
        self.damaged = None;
        self.repair_parts.clear();
        self.commit = commit;
        self.status = if self.view == self.last_normal && self.primary_of(self.view) != self.id {
            Status::Normal
        } else {
            Status::Recovering
        };
        self.save_view(out);
        self.last_heard = now;
    }
    fn request_repair(&mut self, out: &mut Vec<Action>) {
        if let Some(transfer) = &self.transfer {
            out.push(transfer.request());
        }
        if self.epoch_catch_up && !self.holds_start() && self.damaged.is_none() {
            self.request_epoch_prefix(out);
        }

        if let Some((first, last)) = self.damaged {
            let (from, to) = self.repair_gap(first, last).unwrap_or((first, last));
            self.broadcast(
                Msg::GetRepair {
                    view: self.view,
                    first: from,
                    last: to,
                },
                out,
            );
            self.broadcast(
                Msg::GetEntries {
                    view: self.view,
                    from,
                    to,
                },
                out,
            );
            // If our entries after the damage belong to another history the
            // range cannot be verified against them; a recovering replica
            // never votes, so it may instead take the primary's log from
            // below the damage.
            self.broadcast(
                Msg::GetState {
                    view: self.view,
                    after: self.commit.min(first - 1),
                },
                out,
            );
        } else {
            // Rejoining a newer view: ask everyone; the primary answers.
            self.broadcast(
                Msg::GetState {
                    view: self.view,
                    after: self.commit,
                },
                out,
            );
        }
    }

    /// Callers check [`Self::may_vote`] first: a replica bound by a promise
    /// never enters a view change.
    fn start_view_change(&mut self, view: u64, now: u64, out: &mut Vec<Action>) {
        self.view = view;
        self.status = Status::ViewChange;
        self.primary_lost = false;
        self.jitter = next_random(&mut self.rng) % self.timing.election_ms.max(1);
        self.svc.clear();
        self.svc.insert(self.id);
        self.dvc.clear();
        self.dvc_sent = false;
        self.adopting = None;
        self.transfer = None;
        self.last_heard = now;
        self.last_svc = now;
        self.save_view(out);
        out.push(Action::Role {
            primary: false,
            view,
        });
        self.broadcast(Msg::StartViewChange { view }, out);
        self.maybe_send_dvc(out);
    }

    fn maybe_send_dvc(&mut self, out: &mut Vec<Action>) {
        if self.dvc_sent
            || self.status != Status::ViewChange
            || self.svc.len() < self.quorum()
            || !self.may_vote(self.clock)
        {
            return;
        }
        self.dvc_sent = true;
        let p = self.primary_of(self.view);
        if p == self.id {
            let d = self.own_dvc();
            self.dvc.insert(self.id, d);
            self.maybe_start_view(out);
        } else {
            self.send_dvc(out);
        }
    }

    fn own_dvc(&self) -> Dvc {
        Dvc {
            last_normal: self.last_normal,
            op: self.op,
            commit: self.commit,
            entries: self.range(self.commit, self.op),
        }
    }

    fn send_dvc(&self, out: &mut Vec<Action>) {
        let d = self.own_dvc();
        out.push(Action::Send {
            to: self.primary_of(self.view),
            msg: Msg::DoViewChange {
                view: self.view,
                last_normal: d.last_normal,
                op: d.op,
                commit: d.commit,
                entries: d.entries,
            },
        });
    }

    /// Primary: the StartView of this view again, for one member, with the
    /// log as it is now.
    fn resend_start_view(&self, to: ReplicaId, out: &mut Vec<Action>) {
        out.push(Action::Send {
            to,
            msg: Msg::StartView {
                view: self.view,
                op: self.op,
                commit: self.commit,
                entries: self.range(self.commit, self.op),
            },
        });
    }

    /// A view change in progress repeats its messages: a member that was
    /// down or unreachable when they were first sent joins this view instead
    /// of waiting out another timeout, and a dropped vote is sent again.
    fn retransmit_view_change(&mut self, now: u64, out: &mut Vec<Action>) {
        // Only a view change this process started or joined: a replica that
        // restarted in view-change status waits for its timeout (rule 2).
        if self.status != Status::ViewChange
            || !self.svc.contains(&self.id)
            || now.saturating_sub(self.last_svc) < self.timing.retransmit_ms()
        {
            return;
        }
        self.last_svc = now;
        self.broadcast(Msg::StartViewChange { view: self.view }, out);
        if self.dvc_sent && self.primary_of(self.view) != self.id {
            self.send_dvc(out);
        }
    }

    fn maybe_start_view(&mut self, out: &mut Vec<Action>) {
        if self.status != Status::ViewChange
            || self.primary_of(self.view) != self.id
            || self.dvc.len() < self.quorum()
            || self.adopting.is_some()
            || self.retired_view == Some(self.view)
            || !self.may_vote(self.clock)
        {
            return;
        }
        let Some((&best_id, best)) = self
            .dvc
            .iter()
            .max_by_key(|(id, d)| (d.last_normal, d.op, std::cmp::Reverse(**id)))
        else {
            return;
        };
        let best = best.clone();
        let max_commit = self.dvc.values().map(|d| d.commit).max().unwrap_or(0);
        // Keep what is committed locally, adopt the best log after it. Our
        // own log stays untouched until the whole new log is in hand.
        let keep = self.commit;
        if keep < best.commit && best_id != self.id {
            // Committed entries this replica lacks: fetch them from `best`.
            self.adopting = Some(Adoption {
                donor: best_id,
                through: best.commit,
                max_commit,
                suffix: best.entries,
                fetched: Vec::new(),
            });
            out.push(Action::Send {
                to: best_id,
                msg: Msg::GetEntries {
                    view: self.view,
                    from: keep + 1,
                    to: best.commit,
                },
            });
            return;
        }
        let mut new_log = self.range(keep, best.commit.max(keep));
        new_log.retain(|e| e.op > keep);
        if best_id == self.id {
            new_log = self.range(keep, self.op);
        } else {
            new_log.extend(
                best.entries
                    .iter()
                    .filter(|e| e.op > keep.max(best.commit))
                    .cloned(),
            );
        }
        self.finish_adoption(keep, new_log, max_commit, out);
    }

    fn request_adoption_page(&self, out: &mut Vec<Action>) {
        if let Some(a) = &self.adopting {
            let after = a.fetched.last().map_or(self.commit, |entry| entry.op);
            out.push(Action::Send {
                to: a.donor,
                msg: Msg::GetEntries {
                    view: self.view,
                    from: after + 1,
                    to: a.through,
                },
            });
        }
    }

    fn finish_adoption(
        &mut self,
        keep: Op,
        new_log: Vec<Entry>,
        max_commit: Op,
        out: &mut Vec<Action>,
    ) {
        if !self.replace(keep, new_log, out) {
            // The adopted log does not chain onto ours: give up this view.
            self.adopting = None;
            return;
        }
        self.adopting = None;
        self.status = Status::Normal;
        self.epoch_catch_up = false;
        self.last_normal = self.view;
        self.acked = vec![0; MAX_MEMBERS];
        self.progress_at = vec![self.clock; MAX_MEMBERS];
        self.proposed_at = self.clock;
        // A lease is earned in this view, from pings of this view.
        self.lease_acks = vec![0; MAX_MEMBERS];
        self.heard_at = vec![self.clock; MAX_MEMBERS];
        self.set_commit(max_commit, out);
        self.save_view(out);
        out.push(Action::Role {
            primary: true,
            view: self.view,
        });
        let msg = Msg::StartView {
            view: self.view,
            op: self.op,
            commit: self.commit,
            entries: self.range(self.commit, self.op),
        };
        self.broadcast(msg, out);
        // Our own durability counts toward the new view's commits.
        self.acked[usize::from(self.id)] = self.durable;
    }

    pub fn on_message(&mut self, from: ReplicaId, msg: Msg, now: u64, out: &mut Vec<Action>) {
        if usize::from(from) >= MAX_MEMBERS || from == self.id {
            return;
        }
        self.clock = self.clock.max(now);
        match msg {
            Msg::GetEpoch { epoch } => {
                if epoch < self.membership.epoch
                    && let Some(entry) = self.membership.entry.clone()
                {
                    out.push(Action::Send {
                        to: from,
                        msg: Msg::Epoch { entry },
                    });
                }
                return;
            }
            Msg::Epoch { entry } => {
                self.on_epoch(entry, out);
                self.after_step(out);
                return;
            }
            _ => {}
        }
        if self.joining {
            return;
        }
        // A view of a later epoch: this replica missed a reconfiguration.
        if msg
            .view()
            .is_some_and(|v| epoch_of(v) > self.membership.epoch)
        {
            self.ask_epoch(from, now, out);
            return;
        }
        // A member of an earlier epoch (perhaps one this epoch removed)
        // learns the reconfiguration that it missed; a retired member tells
        // the members it left behind.
        if (self.retired
            || !self.membership.contains(from)
            || msg
                .view()
                .is_some_and(|v| epoch_of(v) < self.membership.epoch))
            && let Some(entry) = self.membership.entry.clone()
            && now.saturating_sub(self.told[usize::from(from)]) >= self.timing.repair_ms()
        {
            self.told[usize::from(from)] = now;
            out.push(Action::Send {
                to: from,
                msg: Msg::Epoch { entry },
            });
        }
        if self.retired {
            // A removed member still serves the committed log to members
            // catching up into the epoch that removed it.
            if let Msg::GetEntries { from: a, to: b, .. } = msg
                && self.membership.contains(from)
            {
                let entries = self.committed_page(a, b);
                if !entries.is_empty() && self.damaged.is_none() {
                    out.push(Action::Send {
                        to: from,
                        msg: Msg::Entries {
                            view: self.view,
                            entries,
                            commit: self.commit,
                        },
                    });
                }
            }
            return;
        }
        let served = self.epoch_catch_up
            && self.membership.previous.contains(&from)
            && matches!(msg, Msg::Entries { .. });
        if !self.membership.contains(from) && !served {
            return;
        }
        self.message(from, msg, now, out);
        self.after_step(out);
    }

    fn message(&mut self, from: ReplicaId, msg: Msg, now: u64, out: &mut Vec<Action>) {
        match msg {
            Msg::GetEpoch { .. } | Msg::Epoch { .. } => {}
            Msg::Reconfigure { body } => {
                if let Some((_, members)) = decode_reconfig(&body) {
                    let _ = self.reconfigure(&members, out);
                }
            }
            Msg::Prepare {
                view,
                entries,
                commit,
            } => {
                if self.primary_of(view) != from {
                    return;
                }
                if self.adopt_newer(view, from, now, out)
                    || self.missed_start_view(view, from, now, out)
                {
                    return;
                }
                if view != self.view || self.status != Status::Normal || self.is_primary() {
                    return;
                }
                self.last_heard = now;
                self.primary_lost = false;
                let before = self.op;
                self.append(&entries, out);
                if let Some(first) = entries.first()
                    && first.op > self.op + 1
                {
                    out.push(Action::Send {
                        to: from,
                        msg: Msg::GetEntries {
                            view,
                            from: self.op + 1,
                            to: first.op - 1,
                        },
                    });
                }
                if self.op == before && self.durable == self.op {
                    // A retransmission: re-acknowledge.
                    out.push(Action::Send {
                        to: from,
                        msg: Msg::PrepareOk {
                            view,
                            op: self.durable,
                        },
                    });
                }
                self.set_commit(commit, out);
            }
            Msg::PrepareOk { view, op } => {
                if view != self.view || !self.is_primary() {
                    return;
                }
                let i = usize::from(from);
                self.heard_at[i] = self.heard_at[i].max(now);
                if op > self.acked[i] {
                    self.acked[i] = op.min(self.op);
                    self.progress_at[i] = now;
                    self.recompute_commit(out);
                }
            }
            Msg::Ping { view, stamp } => {
                if self.primary_of(view) != from
                    || view != self.view
                    || self.status != Status::Normal
                    || self.is_primary()
                {
                    return;
                }
                // The promise: no vote in any view change for promise_ms.
                self.last_heard = now;
                self.primary_lost = false;
                self.promised_until = self
                    .promised_until
                    .max(now.saturating_add(self.timing.promise_ms()));
                out.push(Action::Send {
                    to: from,
                    msg: Msg::Pong { view, stamp },
                });
            }
            Msg::Pong { view, stamp } => {
                if view != self.view || !self.is_primary() || stamp > self.clock {
                    return;
                }
                let i = usize::from(from);
                self.heard_at[i] = self.heard_at[i].max(now);
                self.lease_acks[i] = self.lease_acks[i].max(stamp);
            }
            Msg::Commit { view, commit } => {
                if self.primary_of(view) != from {
                    return;
                }
                if self.adopt_newer(view, from, now, out)
                    || self.missed_start_view(view, from, now, out)
                {
                    return;
                }
                if view != self.view || self.status != Status::Normal {
                    return;
                }
                self.last_heard = now;
                self.primary_lost = false;
                self.set_commit(commit, out);
                if self.durable < self.op || self.op < commit {
                    // Make sure the primary knows how far we are.
                    out.push(Action::Send {
                        to: from,
                        msg: Msg::PrepareOk {
                            view,
                            op: self.durable,
                        },
                    });
                    if self.op < commit {
                        out.push(Action::Send {
                            to: from,
                            msg: Msg::GetEntries {
                                view,
                                from: self.op + 1,
                                to: commit,
                            },
                        });
                    }
                }
            }
            Msg::StartViewChange { view } => {
                // Only a replica with holes in its log must not vote.
                if self.damaged.is_some() {
                    return;
                }
                if self.status == Status::Recovering && view <= self.view {
                    return;
                }
                if view > self.view {
                    if !self.may_vote(now) {
                        // Bound by a promise to the primary: join later,
                        // once it lapses, unless the primary renews it.
                        self.deferred_view = self.deferred_view.max(view);
                        return;
                    }
                    self.start_view_change(view, now, out);
                }
                if view == self.view && self.status == Status::ViewChange {
                    self.svc.insert(from);
                    self.maybe_send_dvc(out);
                }
                // A member still changing to this view missed its StartView
                // (often: this primary's stream to it was not up yet).
                if view == self.view && self.is_primary() {
                    self.resend_start_view(from, out);
                }
            }
            Msg::DoViewChange {
                view,
                last_normal,
                op,
                commit,
                entries,
            } => {
                if self.damaged.is_some()
                    || (self.status == Status::Recovering && view <= self.view)
                {
                    return;
                }
                if view > self.view {
                    if !self.may_vote(now) {
                        // The sender repeats it once this replica joins.
                        self.deferred_view = self.deferred_view.max(view);
                        return;
                    }
                    self.start_view_change(view, now, out);
                }
                if view == self.view && self.is_primary() {
                    // A vote that arrived after the view started.
                    self.resend_start_view(from, out);
                    return;
                }
                if view != self.view
                    || self.status != Status::ViewChange
                    || self.primary_of(view) != self.id
                {
                    return;
                }
                self.dvc.insert(
                    from,
                    Dvc {
                        last_normal,
                        op,
                        commit,
                        entries,
                    },
                );
                // Our own DVC counts once we reached the SVC quorum; a DVC
                // from others also implies they saw one.
                if !self.dvc_sent {
                    self.svc.insert(from);
                    self.maybe_send_dvc(out);
                }
                self.maybe_start_view(out);
            }
            Msg::StartView {
                view,
                op: _,
                commit,
                entries,
            } => {
                if view < self.view || (view == self.view && self.status == Status::Normal) {
                    return;
                }
                if self.primary_of(view) != from {
                    return;
                }
                if self.status == Status::Recovering && self.damaged.is_some() {
                    return;
                }
                self.view = view;
                self.last_heard = now;
                self.adopting = None;
                // Everything through our own commit is committed, so the new
                // view's log agrees with it: keep it, replace the rest.
                let keep = self.commit;
                if keep < commit {
                    // We lack committed entries of the new view's log. Until
                    // we hold them we must not claim to be normal in it (a
                    // later view change would trust our log): fetch the
                    // whole state after `keep` first.
                    self.status = Status::Recovering;
                    self.last_repair = now;
                    self.save_view(out);
                    out.push(Action::Role {
                        primary: false,
                        view,
                    });
                    out.push(Action::Send {
                        to: from,
                        msg: Msg::GetState { view, after: keep },
                    });
                    return;
                }
                let new_log: Vec<Entry> = entries.into_iter().filter(|e| e.op > keep).collect();
                if !self.replace(keep, new_log, out) {
                    self.status = Status::Recovering;
                    self.last_repair = now;
                    self.save_view(out);
                    out.push(Action::Send {
                        to: from,
                        msg: Msg::GetState { view, after: keep },
                    });
                    return;
                }
                self.status = Status::Normal;
                self.epoch_catch_up = false;
                self.last_normal = view;
                self.save_view(out);
                out.push(Action::Role {
                    primary: false,
                    view,
                });
                self.set_commit(commit, out);
                // Acknowledge what we hold in the new view.
                out.push(Action::Send {
                    to: from,
                    msg: Msg::PrepareOk {
                        view,
                        op: self.durable.min(self.op),
                    },
                });
            }
            Msg::GetRepair {
                view: _,
                first,
                last,
            } => {
                let through = if self.status == Status::Recovering {
                    self.op
                } else {
                    self.durable
                };
                let entries = self.page(first, last.min(through));
                if !entries.is_empty() {
                    out.push(Action::Send {
                        to: from,
                        msg: Msg::RepairEntries {
                            entries,
                            commit: self.commit,
                        },
                    });
                }
            }
            Msg::RepairEntries { entries, commit } => {
                self.repair_entries(entries, commit, now, out)
            }
            Msg::GetEntries {
                view,
                from: a,
                to: b,
            } => {
                // Anyone with the entries may answer: entries are verified by
                // the hash chain, not by who sent them.
                let through = if self.status == Status::Normal {
                    b
                } else {
                    b.min(self.commit)
                };
                let entries = self.page(a, through);
                if !entries.is_empty() && self.damaged.is_none_or(|(x, _)| entries[0].op < x) {
                    out.push(Action::Send {
                        to: from,
                        msg: Msg::Entries {
                            view: view.max(self.view),
                            entries,
                            commit: self.commit,
                        },
                    });
                }
            }
            Msg::Entries {
                view,
                entries,
                commit,
            } => self.on_entries(from, view, entries, commit, now, out),
            Msg::GetState { view, after } => {
                if !self.is_primary() || view > self.view {
                    return;
                }
                if after < self.base {
                    return;
                }
                if after > self.op {
                    return;
                }
                let entries = if after < self.op {
                    self.page(after + 1, self.op)
                } else {
                    Vec::new()
                };
                let msg = if entries.last().map_or(after, |e| e.op) == self.op {
                    Msg::NewState {
                        view: self.view,
                        commit: self.commit,
                        after,
                        entries,
                    }
                } else {
                    Msg::StateOffer {
                        view: self.view,
                        commit: self.commit,
                        after,
                        through: self.op,
                    }
                };
                out.push(Action::Send { to: from, msg });
            }
            Msg::StateOffer {
                view,
                after,
                commit,
                through,
            } => {
                if self.primary_of(view) != from
                    || view < self.view
                    || through <= after
                    || commit > through
                    || after > self.commit
                    || commit < self.commit
                    || self.damaged.is_some_and(|(first, _)| after >= first)
                    || (self.status == Status::Normal && view == self.view)
                {
                    return;
                }
                if self
                    .transfer
                    .as_ref()
                    .is_none_or(|t| view > t.view || (view == t.view && t.after != after))
                {
                    self.transfer = Some(StateTransfer {
                        view,
                        primary: from,
                        after,
                        through,
                        commit,
                        fetched: Vec::new(),
                    });
                    self.view = view;
                    self.status = Status::Recovering;
                    self.adopting = None;
                    self.save_view(out);
                    self.last_heard = now;
                    self.last_repair = now;
                    out.push(Action::Role {
                        primary: false,
                        view,
                    });
                }
                if let Some(t) = &self.transfer {
                    out.push(t.request());
                }
            }
            Msg::NewState {
                view,
                commit,
                after,
                entries,
            } => {
                if view < self.view {
                    return;
                }
                if let Some((a, _)) = self.damaged {
                    if self.primary_of(view) != from || after >= a || after > self.commit {
                        return;
                    }
                    let new_log: Vec<Entry> =
                        entries.into_iter().filter(|e| e.op > after).collect();
                    let c = self.commit;
                    if commit < c || (c > after && new_log.last().is_none_or(|e| e.op < c)) {
                        return;
                    }
                    if !self.replace(after, new_log, out) {
                        return;
                    }
                    self.commit = c.min(self.op);
                    self.damaged = None;
                    self.repair_parts.clear();
                    self.transfer = None;
                    self.view = view;
                    self.status = Status::Normal;
                    self.epoch_catch_up = false;
                    self.last_normal = view;
                    self.last_heard = now;
                    self.save_view(out);
                    out.push(Action::Role {
                        primary: false,
                        view,
                    });
                    self.set_commit(commit, out);
                    out.push(Action::Send {
                        to: from,
                        msg: Msg::PrepareOk {
                            view,
                            op: self.durable,
                        },
                    });
                    return;
                }
                if self.status == Status::Normal && view == self.view {
                    return;
                }
                if self.primary_of(view) != from || after > self.commit {
                    return;
                }
                let keep = self.commit.max(after);
                let new_log: Vec<Entry> = entries.into_iter().filter(|e| e.op > keep).collect();
                if !self.replace(keep, new_log, out) {
                    return;
                }
                self.transfer = None;
                self.view = view;
                self.status = Status::Normal;
                self.epoch_catch_up = false;
                self.last_normal = view;
                self.last_heard = now;
                self.save_view(out);
                out.push(Action::Role {
                    primary: false,
                    view,
                });
                self.set_commit(commit, out);
                out.push(Action::Send {
                    to: from,
                    msg: Msg::PrepareOk {
                        view,
                        op: self.durable,
                    },
                });
            }
        }
    }

    /// A message from a newer view in normal operation: this replica fell
    /// behind (it was partitioned or restarted). It rejoins by fetching the
    /// view's log from its primary.
    fn adopt_newer(&mut self, view: u64, from: ReplicaId, now: u64, out: &mut Vec<Action>) -> bool {
        if view <= self.view {
            return false;
        }
        if self.damaged.is_some() {
            return true;
        }
        self.view = view;
        self.transfer = None;
        self.adopting = None;
        self.status = Status::Recovering;
        self.last_repair = now;
        self.save_view(out);
        out.push(Action::Role {
            primary: false,
            view,
        });
        out.push(Action::Send {
            to: from,
            msg: Msg::GetState {
                view,
                after: self.commit,
            },
        });
        true
    }

    /// Normal-operation traffic from the primary of the view we are
    /// changing into: the view change finished without us (we missed the
    /// `StartView`). Fetch the view's log.
    fn missed_start_view(
        &mut self,
        view: u64,
        from: ReplicaId,
        now: u64,
        out: &mut Vec<Action>,
    ) -> bool {
        if view != self.view || self.status != Status::ViewChange || from != self.primary_of(view) {
            return false;
        }
        self.status = Status::Recovering;
        self.last_repair = now;
        out.push(Action::Send {
            to: from,
            msg: Msg::GetState {
                view,
                after: self.commit,
            },
        });
        true
    }

    fn on_entries(
        &mut self,
        from: ReplicaId,
        view: u64,
        entries: Vec<Entry>,
        commit: Op,
        now: u64,
        out: &mut Vec<Action>,
    ) {
        if let Some(t) = &self.transfer
            && t.primary == from
            && t.view == view
            && self.view == view
        {
            let anchor = self.checksum_at(t.after);
            let t = self.transfer.as_mut().expect("receiving state");
            let before = t.fetched.len();
            for entry in entries {
                let after = t.fetched.last().map_or(t.after, |e| e.op);
                if entry.op <= after {
                    continue;
                }
                let checksum = t.fetched.last().map(|e| e.checksum).or(anchor);
                if entry.op != after + 1
                    || entry.op > t.through
                    || Some(entry.parent) != checksum
                    || !entry.valid()
                {
                    break;
                }
                t.fetched.push(entry);
            }
            if before == t.fetched.len() {
                return;
            }
            self.last_heard = now;
            self.last_repair = now;
            if t.fetched.last().is_some_and(|e| e.op == t.through) {
                let t = self.transfer.take().expect("receiving state");
                self.on_message(
                    from,
                    Msg::NewState {
                        view,
                        after: t.after,
                        commit: t.commit,
                        entries: t.fetched,
                    },
                    now,
                    out,
                );
            } else {
                out.push(t.request())
            }
            return;
        }
        if self.damaged.is_some() {
            self.repair_entries(entries, commit, now, out);
            return;
        }
        if self.epoch_catch_up && !self.holds_start() {
            self.catch_up_epoch(from, entries, out);
            return;
        }
        if view != self.view {
            return;
        }
        // A new primary fetching committed entries it lacked (from the
        // replica whose log it adopts; those entries are committed).
        if self.adopting.is_some() {
            let keep = self.commit;
            let anchor = self.checksum_at(keep);
            let a = self.adopting.as_mut().expect("adoption in progress");
            if from != a.donor || commit < a.through {
                return;
            }
            let before = a.fetched.len();
            for e in entries {
                let after = a.fetched.last().map_or(keep, |entry| entry.op);
                if e.op <= after {
                    continue;
                }
                let parent = a.fetched.last().map(|entry| entry.checksum).or(anchor);
                if e.op != after + 1 || e.op > a.through || Some(e.parent) != parent || !e.valid() {
                    break;
                }
                a.fetched.push(e);
            }
            if a.fetched.len() != before {
                self.last_heard = now;
            }
            if a.fetched.last().is_some_and(|entry| entry.op == a.through) {
                let mut a = self.adopting.take().expect("adoption in progress");
                a.fetched
                    .extend(a.suffix.into_iter().filter(|e| e.op > a.through));
                self.finish_adoption(keep, a.fetched, a.max_commit, out);
            } else if a.fetched.len() != before {
                self.last_repair = now;
                self.request_adoption_page(out);
            }
            return;
        }
        if self.status != Status::Normal || from != self.primary_of(view) {
            return;
        }
        let before = self.op;
        self.append(&entries, out);
        if self.op > before {
            self.set_commit(commit, out);
        }
    }
}

// ---------------------------------------------------------------------------
// Wire encoding of messages and entries (for transports and the log file).

use crate::wire::{DecodeError, DecodeResult, Reader, put_bytes, put_u64, put_varint};

pub fn encode_entry(e: &Entry, out: &mut Vec<u8>) {
    put_varint(out, e.op);
    put_varint(out, e.view);
    put_u64(out, e.parent);
    put_u64(out, e.checksum);
    put_bytes(out, &e.body);
}

pub fn decode_entry(r: &mut Reader<'_>) -> DecodeResult<Entry> {
    let op = r.varint()?;
    let view = r.varint()?;
    let parent = r.u64()?;
    let checksum = r.u64()?;
    let body: Payload = std::sync::Arc::from(r.bytes()?);
    let e = Entry {
        op,
        view,
        parent,
        checksum,
        body,
    };
    if !e.valid() {
        return Err(DecodeError("entry checksum mismatch"));
    }
    Ok(e)
}

fn put_entries(out: &mut Vec<u8>, es: &[Entry]) {
    put_varint(out, es.len() as u64);
    for e in es {
        encode_entry(e, out);
    }
}

fn get_entries(r: &mut Reader<'_>) -> DecodeResult<Vec<Entry>> {
    let n = r.varint()?;
    if n > 1 << 20 {
        return Err(DecodeError("too many entries"));
    }
    (0..n).map(|_| decode_entry(r)).collect()
}

pub fn encode_msg(m: &Msg, out: &mut Vec<u8>) {
    match m {
        Msg::Prepare {
            view,
            entries,
            commit,
        } => {
            out.push(1);
            put_varint(out, *view);
            put_varint(out, *commit);
            put_entries(out, entries);
        }
        Msg::PrepareOk { view, op } => {
            out.push(2);
            put_varint(out, *view);
            put_varint(out, *op);
        }
        Msg::Commit { view, commit } => {
            out.push(3);
            put_varint(out, *view);
            put_varint(out, *commit);
        }
        Msg::StartViewChange { view } => {
            out.push(4);
            put_varint(out, *view);
        }
        Msg::DoViewChange {
            view,
            last_normal,
            op,
            commit,
            entries,
        } => {
            out.push(5);
            put_varint(out, *view);
            put_varint(out, *last_normal);
            put_varint(out, *op);
            put_varint(out, *commit);
            put_entries(out, entries);
        }
        Msg::StartView {
            view,
            op,
            commit,
            entries,
        } => {
            out.push(6);
            put_varint(out, *view);
            put_varint(out, *op);
            put_varint(out, *commit);
            put_entries(out, entries);
        }
        Msg::GetEntries { view, from, to } => {
            out.push(7);
            put_varint(out, *view);
            put_varint(out, *from);
            put_varint(out, *to);
        }
        Msg::Entries {
            view,
            entries,
            commit,
        } => {
            out.push(8);
            put_varint(out, *view);
            put_varint(out, *commit);
            put_entries(out, entries);
        }
        Msg::GetRepair { view, first, last } => {
            out.push(12);
            put_varint(out, *view);
            put_varint(out, *first);
            put_varint(out, *last);
        }
        Msg::RepairEntries { entries, commit } => {
            out.push(13);
            put_varint(out, *commit);
            put_entries(out, entries);
        }
        Msg::StateOffer {
            view,
            after,
            commit,
            through,
        } => {
            out.push(11);
            put_varint(out, *view);
            put_varint(out, *after);
            put_varint(out, *commit);
            put_varint(out, *through);
        }
        Msg::GetState { view, after } => {
            out.push(9);
            put_varint(out, *view);
            put_varint(out, *after);
        }
        Msg::NewState {
            view,
            commit,
            after,
            entries,
        } => {
            out.push(10);
            put_varint(out, *view);
            put_varint(out, *commit);
            put_varint(out, *after);
            put_entries(out, entries);
        }
        Msg::Ping { view, stamp } => {
            out.push(14);
            put_varint(out, *view);
            put_varint(out, *stamp);
        }
        Msg::Pong { view, stamp } => {
            out.push(15);
            put_varint(out, *view);
            put_varint(out, *stamp);
        }
        Msg::GetEpoch { epoch } => {
            out.push(16);
            put_varint(out, *epoch);
        }
        Msg::Epoch { entry } => {
            out.push(17);
            encode_entry(entry, out);
        }
        Msg::Reconfigure { body } => {
            out.push(18);
            put_bytes(out, body);
        }
    }
}

pub fn decode_msg(bytes: &[u8]) -> DecodeResult<Msg> {
    let mut r = Reader::new(bytes);
    let m = match r.u8()? {
        1 => {
            let view = r.varint()?;
            let commit = r.varint()?;
            Msg::Prepare {
                view,
                commit,
                entries: get_entries(&mut r)?,
            }
        }
        2 => Msg::PrepareOk {
            view: r.varint()?,
            op: r.varint()?,
        },
        3 => Msg::Commit {
            view: r.varint()?,
            commit: r.varint()?,
        },
        4 => Msg::StartViewChange { view: r.varint()? },
        5 => Msg::DoViewChange {
            view: r.varint()?,
            last_normal: r.varint()?,
            op: r.varint()?,
            commit: r.varint()?,
            entries: get_entries(&mut r)?,
        },
        6 => Msg::StartView {
            view: r.varint()?,
            op: r.varint()?,
            commit: r.varint()?,
            entries: get_entries(&mut r)?,
        },
        7 => Msg::GetEntries {
            view: r.varint()?,
            from: r.varint()?,
            to: r.varint()?,
        },
        8 => {
            let view = r.varint()?;
            let commit = r.varint()?;
            Msg::Entries {
                view,
                commit,
                entries: get_entries(&mut r)?,
            }
        }
        9 => Msg::GetState {
            view: r.varint()?,
            after: r.varint()?,
        },
        10 => {
            let view = r.varint()?;
            let commit = r.varint()?;
            let after = r.varint()?;
            Msg::NewState {
                view,
                commit,
                after,
                entries: get_entries(&mut r)?,
            }
        }
        11 => Msg::StateOffer {
            view: r.varint()?,
            after: r.varint()?,
            commit: r.varint()?,
            through: r.varint()?,
        },
        12 => Msg::GetRepair {
            view: r.varint()?,
            first: r.varint()?,
            last: r.varint()?,
        },
        13 => Msg::RepairEntries {
            commit: r.varint()?,
            entries: get_entries(&mut r)?,
        },
        14 => Msg::Ping {
            view: r.varint()?,
            stamp: r.varint()?,
        },
        15 => Msg::Pong {
            view: r.varint()?,
            stamp: r.varint()?,
        },
        16 => Msg::GetEpoch { epoch: r.varint()? },
        17 => Msg::Epoch {
            entry: decode_entry(&mut r)?,
        },
        18 => Msg::Reconfigure {
            body: std::sync::Arc::from(r.bytes()?),
        },
        _ => return Err(DecodeError("unknown replication message")),
    };
    if !r.is_empty() {
        return Err(DecodeError("trailing bytes in replication message"));
    }
    Ok(m)
}

#[cfg(test)]
mod sim;
