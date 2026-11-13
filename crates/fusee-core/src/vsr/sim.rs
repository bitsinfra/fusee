//! Randomized simulation of the replication protocol under faults.

use super::*;
use std::sync::Arc;

struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }
    fn below(&mut self, n: u64) -> u64 {
        if n == 0 { 0 } else { self.next() % n }
    }
    fn chance(&mut self, per_mille: u64) -> bool {
        self.below(1000) < per_mille
    }
}

#[derive(Clone)]
enum Write {
    Append(Vec<Entry>),
    Replace(Op, Vec<Entry>),
}

#[derive(Default, Clone)]
struct Disk {
    // The simulated application is an ordered vector of entry checksums.
    // Its durable checkpoint replaces the equivalent protocol-log prefix.
    snapshot: Arc<Vec<u64>>,
    durable: Vec<Entry>,
    pending: Vec<Write>,
    damaged: BTreeSet<Op>,
    view: u64,
    last_normal: u64,
    commit: Op,
    /// The saved membership, and for a joining member the members it was
    /// told to ask.
    membership: Option<Membership>,
    provisional: Option<Membership>,
}

fn apply_write(log: &mut Vec<Entry>, w: Write) {
    match w {
        Write::Append(es) => log.extend(es),
        Write::Replace(after, es) => {
            log.retain(|e| e.op <= after);
            log.extend(es);
        }
    }
}

impl Disk {
    /// The log as the page cache sees it.
    fn view_log(&self) -> Vec<Entry> {
        let mut l = self.durable.clone();
        for w in &self.pending {
            apply_write(&mut l, w.clone());
        }
        l
    }
    fn last(&self) -> Op {
        self.view_log()
            .last()
            .map_or(self.snapshot.len() as Op, |e| e.op)
    }
    fn holds(&self, op: Op, sum: u64) -> bool {
        self.snapshot.get(op as usize - 1) == Some(&sum)
            || (!self.damaged.contains(&op)
                && self.durable.iter().any(|e| e.op == op && e.checksum == sum))
    }
    fn persist(&mut self, w: Write) {
        if let Write::Replace(after, _) = &w {
            self.damaged.retain(|n| *n <= *after);
        }
        apply_write(&mut self.durable, w);
    }
    fn flush(&mut self) {
        for w in std::mem::take(&mut self.pending) {
            self.persist(w);
        }
    }
}

struct Node {
    replica: Option<Replica>,
    disk: Disk,
    down_until: u64,
    /// A spare is not started until a membership change adds it.
    started: bool,
    /// Lost for good (a member to replace).
    dead: bool,
}

impl Node {
    /// The membership this member last knew.
    fn membership(&self, n: u8) -> Membership {
        match &self.replica {
            Some(r) => r.membership().clone(),
            None => self
                .disk
                .membership
                .clone()
                .unwrap_or_else(|| Membership::initial(n)),
        }
    }
}

/// The newest membership any started member knows.
fn newest(nodes: &[Node], n: u8) -> Membership {
    let joining = |x: &Node| match &x.replica {
        Some(r) => r.joining(),
        None => x.disk.membership.is_none() && x.disk.provisional.is_some(),
    };
    let mut best = Membership::initial(n);
    for x in nodes.iter().filter(|x| x.started && !joining(x)) {
        let m = x.membership(n);
        if m.epoch > best.epoch {
            best = m;
        }
    }
    best
}

#[allow(dead_code)]
pub struct Outcome {
    pub committed: u64,
    pub crashes: u32,
    pub view_changes: u64,
    pub repairs: u32,
    pub checkpoints: u32,
    /// Commits a primary learned while its own log was durable through less
    /// than the new commit position (a backup quorum committed them).
    pub ahead_of_primary: u64,
    /// Milliseconds in which some ready primary held a valid read lease.
    pub lease_ms: u64,
    /// The final epoch, and membership requests sent through a backup.
    pub epochs: u64,
    pub reconfig_msgs: u64,
}

/// What one simulated run varies.
#[derive(Clone)]
pub struct Params {
    pub n: u8,
    pub fault_ms: u64,
    /// The primary's chance per millisecond to complete its pending log
    /// writes (backups: 500 per mille). A low value models a slow primary
    /// disk: entries commit on a quorum of backups first.
    pub primary_flush_per_mille: u64,
    pub timing: Timing,
    /// Each member's clock runs at a fixed rate drawn from `1 ± drift_ppm`
    /// parts per million of real time (the lease rule assumes at most
    /// [`MAX_CLOCK_DRIFT_PPM`]).
    pub drift_ppm: u64,
    /// Negative control: members ignore their promises to the primary.
    pub ignore_promises: bool,
    /// Membership changes while faults last: add two members, remove two,
    /// then replace a member that is lost for good.
    pub reconfig: bool,
}

impl Params {
    pub fn new(n: u8, fault_ms: u64) -> Self {
        Self {
            n,
            fault_ms,
            primary_flush_per_mille: 500,
            timing: Timing {
                heartbeat_ms: 20,
                election_ms: 150,
            },
            drift_ppm: 0,
            ignore_promises: false,
            reconfig: false,
        }
    }
}

/// Runs one seed; returns an error describing the first violation.
pub fn run(seed: u64, n: u8, fault_ms: u64) -> Result<Outcome, String> {
    run_params(seed, &Params::new(n, fault_ms))
}

/// Like [`run`], with a slow primary disk (see [`Params`]).
pub fn run_with(
    seed: u64,
    n: u8,
    fault_ms: u64,
    primary_flush_per_mille: u64,
) -> Result<Outcome, String> {
    run_params(
        seed,
        &Params {
            primary_flush_per_mille,
            ..Params::new(n, fault_ms)
        },
    )
}

pub fn run_params(seed: u64, p: &Params) -> Result<Outcome, String> {
    let (n, fault_ms, primary_flush_per_mille) = (p.n, p.fault_ms, p.primary_flush_per_mille);
    let mut rng = Rng(seed ^ 0x5eed);
    // Clocks and closed streams draw from their own generator, so runs
    // without drift keep the same fault schedule per seed.
    let mut clock_rng = Rng(seed ^ 0xc10c);
    // Spares that membership changes add.
    let slots = if p.reconfig { n + 3 } else { n };
    let rates: Vec<i128> = (0..slots)
        .map(|_| {
            let d = p.drift_ppm as i128;
            1_000_000 + clock_rng.below(2 * p.drift_ppm + 1) as i128 - d
        })
        .collect();
    // Each member's clock: real milliseconds scaled by its rate.
    let local = |i: usize, now: u64| -> u64 { (now as i128 * rates[i] / 1_000_000) as u64 };
    let timing = p.timing.clone();
    let mut nodes: Vec<Node> = (0..slots)
        .map(|i| {
            let replica = (i < n).then(|| {
                let mut r = Replica::new(
                    i,
                    n,
                    Recovered::default(),
                    timing.clone(),
                    seed.wrapping_mul(31) + u64::from(i) * 97,
                    0,
                );
                r.ignore_promises = p.ignore_promises;
                r
            });
            Node {
                replica,
                disk: Disk::default(),
                down_until: if i < n { 0 } else { u64::MAX },
                started: i < n,
                dead: false,
            }
        })
        .collect();
    // Membership changes: the member set being asked for, the steps taken.
    let mut target: BTreeSet<ReplicaId> = (0..n).collect();
    let mut step = 0;
    let mut reconfig_msgs = 0u64;
    // Lease checks: when each member, as primary, first held its whole log
    // committed in its current view (reads are served only from then on).
    let mut ready: Vec<Option<u64>> = vec![None; usize::from(slots)];
    let mut lease_ms = 0u64;
    let mut net: BTreeMap<(u64, u64), (ReplicaId, ReplicaId, Msg)> = BTreeMap::new();
    let mut seq = 0u64;
    let mut committed: BTreeMap<Op, u64> = BTreeMap::new();
    let mut commits: Vec<Op> = vec![0; usize::from(slots)];
    let mut partition: Option<(Vec<bool>, u64)> = None;
    let mut out = Vec::new();
    let mut body = 0u64;
    let mut crashes = 0;
    let mut repairs = 0;
    let mut checkpoints = 0;
    AHEAD.with(|a| a.set(0));
    let mut max_view = 0;
    let total = fault_ms + 6_000;
    let mut committed_at_heal = 0usize;
    for now in 0..total {
        let faults = now < fault_ms;
        // Faults: crashes, restarts, partitions.
        if faults && rng.chance(4) {
            let i = rng.below(u64::from(slots)) as usize;
            let up = nodes.iter().filter(|x| x.replica.is_some()).count();
            // With changing membership: every membership a member holds
            // keeps a quorum up.
            let survivable = if p.reconfig {
                let memberships: Vec<Membership> = nodes
                    .iter()
                    .filter(|x| x.started && !x.dead)
                    .map(|x| x.membership(n))
                    .collect();
                memberships.iter().all(|m| {
                    m.members
                        .iter()
                        .filter(|j| {
                            usize::from(**j) != i && nodes[usize::from(**j)].replica.is_some()
                        })
                        .count()
                        >= m.quorum()
                })
            } else {
                up > usize::from(n) / 2
            };
            if nodes[i].replica.is_some() && survivable {
                nodes[i].replica = None;
                trace(format!(
                    "t={now} r{i} CRASH durable to {:?} pending {}",
                    nodes[i].disk.durable.last().map(|e| e.op),
                    nodes[i].disk.pending.len()
                ));
                crashes += 1;
                // The process's streams close: most peers notice at once.
                for (j, node) in nodes.iter_mut().enumerate() {
                    if let Some(r) = node.replica.as_mut()
                        && clock_rng.chance(700)
                    {
                        r.peer_closed(i as u8, local(j, now));
                    }
                }
                // Unflushed writes: a random prefix survives; an append may
                // tear (a prefix of its entries), a replacement is atomic.
                let d = &mut nodes[i].disk;
                let keep = rng.below(d.pending.len() as u64 + 1) as usize;
                let pending = std::mem::take(&mut d.pending);
                for (k, w) in pending.into_iter().enumerate() {
                    if k < keep {
                        d.persist(w);
                    } else if k == keep {
                        if let Write::Append(es) = w {
                            let cut = rng.below(es.len() as u64 + 1) as usize;
                            d.durable.extend(es.into_iter().take(cut));
                        }
                        break;
                    }
                }
                nodes[i].down_until = now + 20 + rng.below(300);
            }
        }
        for i in 0..usize::from(slots) {
            if nodes[i].replica.is_none()
                && nodes[i].started
                && !nodes[i].dead
                && now >= nodes[i].down_until
            {
                let d = &nodes[i].disk;
                let mut rec = Recovered {
                    view: d.view,
                    last_normal: d.last_normal,
                    commit: d
                        .commit
                        .min(d.durable.last().map_or(d.snapshot.len() as Op, |e| e.op)),
                    base: d.snapshot.len() as Op,
                    base_checksum: d.snapshot.last().copied().unwrap_or(0),
                    entries: d.durable.clone(),
                    damaged: None,
                    membership: d.membership.clone().or_else(|| d.provisional.clone()),
                    joining: d.membership.is_none() && d.provisional.is_some(),
                };
                // Positive liveness trials preserve an independent durable
                // copy of each corrupted entry. Destroying the last copy of
                // a committed value is beyond the group's failure tolerance.
                // Damage persists across crashes until a repair is flushed.
                if faults && rec.entries.len() > 3 && rng.chance(250) {
                    let k = if rec.entries.last().is_some_and(|e| e.op == rec.commit)
                        && rng.chance(300)
                    {
                        rec.entries.len() - 1
                    } else {
                        1 + rng.below(rec.entries.len() as u64 - 2) as usize
                    };
                    let entry = &rec.entries[k];
                    // Only a member that still takes part can repair it.
                    let newest = newest(&nodes, n);
                    if nodes.iter().enumerate().any(|(j, x)| {
                        j != i
                            && !x.dead
                            && newest.contains(j as u8)
                            && x.disk.holds(entry.op, entry.checksum)
                    }) {
                        nodes[i].disk.damaged.insert(entry.op);
                        repairs += 1;
                    }
                }
                let bad = &nodes[i].disk.damaged;
                if let (Some(&first), Some(&last)) = (bad.first(), bad.last()) {
                    rec.damaged = Some((first, last));
                    rec.entries.retain(|entry| !bad.contains(&entry.op));
                }
                let mut r =
                    Replica::new(i as u8, n, rec, timing.clone(), rng.next(), local(i, now));
                r.ignore_promises = p.ignore_promises;
                trace(format!(
                    "t={now} r{i} RESTART op {} commit {} status {:?} damaged {:?}",
                    r.op(),
                    r.commit(),
                    r.status(),
                    r.damaged
                ));
                commits[i] = r.commit();
                nodes[i].replica = Some(r);
            }
        }
        if faults && partition.is_none() && rng.chance(2) {
            let side: Vec<bool> = (0..slots).map(|_| rng.chance(500)).collect();
            partition = Some((side, now + 50 + rng.below(400)));
        }
        if partition
            .as_ref()
            .is_some_and(|(_, until)| now >= *until || !faults)
        {
            partition = None;
            // Sometimes the network regroups at once instead of healing: a
            // member that was reachable is cut off just as others, who had
            // already given up on the primary, reach it.
            if faults && clock_rng.chance(300) {
                let side: Vec<bool> = (0..slots).map(|_| clock_rng.chance(500)).collect();
                partition = Some((side, now + 50 + clock_rng.below(400)));
            }
        }
        if now == fault_ms {
            committed_at_heal = committed.len();
        }
        // Membership changes, under the same faults: 3 (or 5) members, two
        // more, two fewer, then one lost for good and replaced. Each step
        // starts once the group has reached the previous one; until then
        // the primary is asked again every 50 ms (some requests go through
        // a backup, some are lost with the reconfiguration entry).
        if p.reconfig {
            let current = newest(&nodes, n);
            let reached = current.members.iter().copied().collect::<BTreeSet<_>>() == target;
            let at = |k: u64| now >= fault_ms * k / 5;
            if reached && step < 3 && at(step + 1) {
                let mut next = target.clone();
                match step {
                    0 => {
                        // Add two spares, started as joiners.
                        next.insert(n);
                        next.insert(n + 1);
                    }
                    1 => {
                        // Remove two members, the primary among them half
                        // of the time.
                        let primary = (0..slots).find(|i| {
                            nodes[usize::from(*i)]
                                .replica
                                .as_ref()
                                .is_some_and(|r| r.is_primary())
                        });
                        let mut gone: Vec<ReplicaId> = next.iter().copied().collect();
                        let first = match primary {
                            Some(p) if clock_rng.chance(500) => p,
                            _ => gone[clock_rng.below(gone.len() as u64) as usize],
                        };
                        next.remove(&first);
                        gone.retain(|g| *g != first);
                        next.remove(&gone[clock_rng.below(gone.len() as u64) as usize]);
                    }
                    _ => {
                        // Lose a member for good, if the rest keep a
                        // quorum, and replace it with the last spare.
                        let members: Vec<ReplicaId> = next.iter().copied().collect();
                        let lost = members[clock_rng.below(members.len() as u64) as usize];
                        let up = members
                            .iter()
                            .filter(|j| **j != lost && nodes[usize::from(**j)].replica.is_some())
                            .count();
                        // Otherwise try again next millisecond. Losing a
                        // member for good while another holds a damaged
                        // entry could destroy its last copy, which is
                        // beyond what a group survives.
                        if up >= current.quorum()
                            && current
                                .members
                                .iter()
                                .all(|j| nodes[usize::from(*j)].disk.damaged.is_empty())
                        {
                            let node = &mut nodes[usize::from(lost)];
                            node.replica = None;
                            node.dead = true;
                            node.disk.pending.clear();
                            next.remove(&lost);
                            next.insert(n + 2);
                        }
                    }
                }
                if next != target {
                    target = next;
                    step += 1;
                    for id in &target {
                        let node = &mut nodes[usize::from(*id)];
                        if !node.started {
                            node.started = true;
                            node.down_until = now;
                            node.disk.provisional = Some(Membership {
                                epoch: 0,
                                members: target.iter().copied().collect(),
                                entry: None,
                                previous: Vec::new(),
                            });
                        }
                    }
                }
            }
            if !reached && now % 50 == 0 {
                let members: Vec<(ReplicaId, String)> =
                    target.iter().map(|id| (*id, format!("m{id}"))).collect();
                let primary = (0..slots).find(|i| {
                    nodes[usize::from(*i)]
                        .replica
                        .as_ref()
                        .is_some_and(|r| r.is_primary())
                });
                if let Some(pr) = primary {
                    let via = (0..slots).find(|i| {
                        *i != pr
                            && nodes[usize::from(*i)]
                                .replica
                                .as_ref()
                                .is_some_and(|r| r.membership().contains(*i) && !r.retired())
                    });
                    if let Some(via) = via.filter(|_| clock_rng.chance(300)) {
                        // Through a backup, as `Msg::Reconfigure`.
                        seq += 1;
                        reconfig_msgs += 1;
                        net.insert(
                            (now + 1, seq),
                            (
                                via,
                                pr,
                                Msg::Reconfigure {
                                    body: encode_reconfig(current.epoch + 1, &members),
                                },
                            ),
                        );
                    } else if let Some(r) = nodes[usize::from(pr)].replica.as_mut() {
                        let _ = r.reconfigure(&members, &mut out);
                        apply(
                            pr,
                            &mut nodes,
                            &mut out,
                            &mut net,
                            &mut seq,
                            &mut rng,
                            now,
                            faults,
                            &mut committed,
                            &mut commits,
                        )?;
                    }
                }
            }
        }
        // Deliver messages due now.
        let due: Vec<(u64, u64)> = net.range(..(now + 1, 0)).map(|(k, _)| *k).collect();
        for k in due {
            let Some((from, to, msg)) = net.remove(&k) else {
                continue;
            };
            let cut = partition
                .as_ref()
                .is_some_and(|(side, _)| side[usize::from(from)] != side[usize::from(to)]);
            if cut {
                continue;
            }
            if let Some(r) = nodes[usize::from(to)].replica.as_mut() {
                r.on_message(from, msg, local(usize::from(to), now), &mut out);
                apply(
                    to,
                    &mut nodes,
                    &mut out,
                    &mut net,
                    &mut seq,
                    &mut rng,
                    now,
                    faults,
                    &mut committed,
                    &mut commits,
                )?;
            }
        }
        // A checkpoint installation is atomic with its suffix. Delayed,
        // dropped and damaged copies leave the receiver on the old generation.
        // The process tests exercise byte transfer and filesystem publication.
        if now % 113 == 0 {
            for node in &mut nodes {
                let Some(r) = node.replica.as_ref() else {
                    continue;
                };
                if !r.is_primary() || r.commit() < r.base() + 64 {
                    continue;
                }
                let base = r.commit();
                let checksum = r.checksum_at(base).unwrap();
                let mut state = node.disk.snapshot.as_ref().clone();
                for e in r.entries().take_while(|e| e.op <= base) {
                    if e.op != state.len() as u64 + 1 || committed.get(&e.op) != Some(&e.checksum) {
                        return Err(format!(
                            "invalid checkpoint history at op {} (seed {seed})",
                            e.op
                        ));
                    }
                    state.push(e.checksum);
                }
                node.disk.flush();
                node.disk.commit = node.disk.commit.max(base);
                assert!(
                    node.replica
                        .as_mut()
                        .unwrap()
                        .compact_through(base, checksum)
                );
                node.disk.snapshot = Arc::new(state);
                node.disk.durable.retain(|e| e.op > base);
                checkpoints += 1;
            }
        }
        if now % 47 == 0 {
            for i in 0..usize::from(slots) {
                let Some(donor) = nodes[i].replica.as_ref() else {
                    continue;
                };
                let view = donor.view();
                let donor_commit = donor.commit();
                let image = nodes[i].disk.snapshot.clone();
                let Some(&hash) = image.last() else { continue };
                let base = image.len() as Op;
                for j in 0..usize::from(slots) {
                    if i == j
                        || partition
                            .as_ref()
                            .is_some_and(|(side, _)| side[i] != side[j])
                        || faults && rng.chance(250)
                    {
                        continue;
                    }
                    let Some(rx) = nodes[j].replica.as_ref() else {
                        continue;
                    };
                    if !rx.accepts_checkpoint(i as u8, view, base, donor_commit) {
                        continue;
                    }
                    let mut suffix = rx.checkpoint_suffix(base, hash);
                    let mut after = suffix.last().map_or(base, |e| e.op);
                    if after < rx.commit() {
                        if donor_commit < rx.commit() {
                            continue;
                        }
                        let extra = nodes[i]
                            .replica
                            .as_ref()
                            .unwrap()
                            .committed_page(after + 1, rx.commit());
                        suffix.extend(extra);
                        after = suffix.last().map_or(base, |e| e.op);
                        suffix.extend(
                            rx.checkpoint_suffix(after, suffix.last().map_or(hash, |e| e.checksum)),
                        );
                    }
                    if !rx.checkpoint_matches(base, hash, &suffix) {
                        continue;
                    }
                    for (k, &sum) in image.iter().enumerate() {
                        if committed.get(&(k as Op + 1)) != Some(&sum) {
                            return Err(format!("disagreeing checkpoint at op {}", k + 1));
                        }
                    }
                    let node = &mut nodes[j];
                    let old_commit = node.replica.as_ref().unwrap().commit();
                    let new_disk = suffix.clone();
                    if !node.replica.as_mut().unwrap().installed_checkpoint(
                        i as u8,
                        view,
                        base,
                        hash,
                        donor_commit,
                        suffix,
                        local(j, now),
                        &mut out,
                    ) {
                        continue;
                    }
                    node.disk.pending.clear();
                    node.disk.damaged.clear();
                    trace(format!(
                        "t={now} r{j} INSTALL image base {base} from r{i} suffix to {:?}",
                        new_disk.last().map(|e| e.op)
                    ));
                    node.disk.durable = new_disk;
                    node.disk.snapshot = image.clone();
                    node.disk.commit = node.disk.commit.max(old_commit).max(base);
                    commits[j] = old_commit.max(base);
                    checkpoints += 1;
                    apply(
                        j as u8,
                        &mut nodes,
                        &mut out,
                        &mut net,
                        &mut seq,
                        &mut rng,
                        now,
                        faults,
                        &mut committed,
                        &mut commits,
                    )?;
                }
            }
        }
        for i in 0..usize::from(slots) {
            // Ticks, flushes, proposals.
            if let Some(r) = nodes[i].replica.as_mut() {
                r.on_tick(local(i, now), &mut out);
                max_view = max_view.max(r.view());
            }
            apply(
                i as u8,
                &mut nodes,
                &mut out,
                &mut net,
                &mut seq,
                &mut rng,
                now,
                faults,
                &mut committed,
                &mut commits,
            )?;
            let flush = if nodes[i].replica.as_ref().is_some_and(|r| r.is_primary()) {
                primary_flush_per_mille
            } else {
                500
            };
            if rng.chance(flush) && !nodes[i].disk.pending.is_empty() {
                let d = &mut nodes[i].disk;
                d.flush();
                let last = d.last();
                if let Some(r) = nodes[i].replica.as_mut() {
                    r.on_durable(last, &mut out);
                }
                apply(
                    i as u8,
                    &mut nodes,
                    &mut out,
                    &mut net,
                    &mut seq,
                    &mut rng,
                    now,
                    faults,
                    &mut committed,
                    &mut commits,
                )?;
            }
            let primary = nodes[i].replica.as_ref().is_some_and(|r| r.is_primary());
            // With membership changes the group also goes idle for a while
            // now and then: only heartbeats then keep backups in a view.
            let idle = p.reconfig && (now / 700) % 3 == 2;
            if primary && !idle && rng.chance(300) {
                body += 1;
                let b: Payload = Arc::from(body.to_le_bytes().to_vec());
                if let Some(r) = nodes[i].replica.as_mut() {
                    r.propose(b, &mut out);
                }
                apply(
                    i as u8,
                    &mut nodes,
                    &mut out,
                    &mut net,
                    &mut seq,
                    &mut rng,
                    now,
                    faults,
                    &mut committed,
                    &mut commits,
                )?;
            }
        }
        // Read leases. A primary serves reads from its own state once its
        // whole log is committed in its view (the host's "ready"), while
        // its lease holds by its own clock. Then no member may be normal in
        // a newer view, and its commit must cover everything committed
        // anywhere: a read there sees every acknowledged write.
        for i in 0..usize::from(slots) {
            let Some(r) = nodes[i].replica.as_ref() else {
                ready[i] = None;
                continue;
            };
            if !r.is_primary() {
                ready[i] = None;
                continue;
            }
            if ready[i] != Some(r.view()) && r.commit() == r.op() {
                ready[i] = Some(r.view());
            }
            if ready[i] != Some(r.view()) || !r.lease_valid(local(i, now)) {
                continue;
            }
            lease_ms += 1;
            for (j, other) in nodes.iter().enumerate() {
                if let Some(o) = other.replica.as_ref()
                    && o.status() == Status::Normal
                    && o.view() > r.view()
                {
                    return Err(format!(
                        "t={now}: replica {i} holds a read lease in view {} while replica {j} is normal in view {} (seed {seed})",
                        r.view(),
                        o.view()
                    ));
                }
            }
            let max = committed.keys().next_back().copied().unwrap_or(0);
            if r.commit() < max {
                return Err(format!(
                    "t={now}: replica {i} would serve a stale read under its lease: commit {} of {max} (seed {seed})",
                    r.commit()
                ));
            }
        }
    }
    // After healing: progress, and every replica holds every committed entry.
    if committed.len() <= committed_at_heal {
        let states: Vec<String> = nodes
            .iter()
            .map(|n| match &n.replica {
                Some(r) => format!(
                    "v{} {:?} op{} c{} d{} dmg={:?}",
                    r.view(),
                    r.status(),
                    r.op(),
                    r.commit(),
                    r.durable(),
                    r.damaged
                ),
                None => "down".into(),
            })
            .collect();
        return Err(format!(
            "no progress after faults stopped (seed {seed}): {} committed; {states:?}",
            committed.len()
        ));
    }
    // With membership changes: the last change completed, removed members
    // retired, and the checks below cover the final members.
    let last = newest(&nodes, n);
    if p.reconfig {
        let members: BTreeSet<ReplicaId> = last.members.iter().copied().collect();
        if members != target || step < 3 {
            return Err(format!(
                "membership change incomplete (seed {seed}): epoch {} has {members:?}, step {step} asked for {target:?}",
                last.epoch
            ));
        }
        for (i, node) in nodes.iter().enumerate() {
            // A retired member stops listening: it stays in the epoch that
            // removed it. A new member removed before it ever learned an
            // epoch with it stays joining (it cannot tell removed from not
            // yet added; the operator stops it).
            if let Some(r) = &node.replica
                && node.started
                && !last.contains(i as u8)
                && !r.retired()
                && !r.joining()
            {
                return Err(format!(
                    "removed member {i} is not retired (epoch {}, seed {seed})",
                    r.membership().epoch
                ));
            }
        }
    }
    for (i, node) in nodes.iter().enumerate() {
        if p.reconfig && !last.contains(i as u8) {
            continue;
        }
        let Some(r) = node.replica.as_ref() else {
            return Err(format!("replica {i} still down"));
        };
        if r.membership().epoch != last.epoch || r.joining() {
            return Err(format!(
                "member {i} is in epoch {} (joining {}), not {}",
                r.membership().epoch,
                r.joining(),
                last.epoch
            ));
        }
        let max = committed.keys().next_back().copied().unwrap_or(0);
        if r.commit() + 50 < max {
            return Err(format!(
                "replica {i} lags: commit {} of {max} (status {:?}, view {})",
                r.commit(),
                r.status(),
                r.view()
            ));
        }
        for (op, sum) in committed.range(..=r.commit()) {
            match r.entry(*op) {
                Some(e) if e.checksum == *sum => {}
                Some(_) => return Err(format!("replica {i} disagrees on committed op {op}")),
                None if node.disk.snapshot.get(*op as usize - 1) == Some(sum) => {}
                None => return Err(format!("replica {i} lost committed op {op}")),
            }
        }
    }
    Ok(Outcome {
        committed: committed.len() as u64,
        crashes,
        view_changes: max_view,
        repairs,
        checkpoints,
        ahead_of_primary: AHEAD.with(|a| a.get()),
        lease_ms,
        epochs: last.epoch,
        reconfig_msgs,
    })
}

thread_local! {
    static AHEAD: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    static TRACE: std::cell::RefCell<Vec<String>> = const { std::cell::RefCell::new(Vec::new()) };
}

fn trace(s: String) {
    TRACE.with(|t| {
        let mut t = t.borrow_mut();
        t.push(s);
        if t.len() > 3000 {
            t.drain(..1000);
        }
    });
}

#[allow(clippy::too_many_arguments)]
fn apply(
    id: ReplicaId,
    nodes: &mut [Node],
    out: &mut Vec<Action>,
    net: &mut BTreeMap<(u64, u64), (ReplicaId, ReplicaId, Msg)>,
    seq: &mut u64,
    rng: &mut Rng,
    now: u64,
    faults: bool,
    committed: &mut BTreeMap<Op, u64>,
    commits: &mut [Op],
) -> Result<(), String> {
    let i = usize::from(id);
    let view = nodes[i]
        .replica
        .as_ref()
        .map(|r| (r.view(), r.status(), r.op(), r.commit()));
    for a in out.drain(..) {
        match &a {
            Action::Send { .. } | Action::Persist { .. } => {}
            other => trace(format!(
                "t={now} r{id} {view:?} {}",
                match other {
                    Action::Replace { after, entries } =>
                        format!("replace after {after} ({} entries)", entries.len()),
                    Action::Committed { commit } => format!("commit {commit}"),
                    Action::SaveView {
                        view,
                        last_normal,
                        commit,
                    } => format!("saveview v={view} ln={last_normal} c={commit}"),
                    Action::Role { primary, view } => format!("role primary={primary} v={view}"),
                    Action::SaveMembership { membership } => format!(
                        "membership epoch {} {:?}",
                        membership.epoch, membership.members
                    ),
                    _ => String::new(),
                }
            )),
        }
        if let Action::Send { to, msg } = &a {
            match msg {
                Msg::StartView {
                    view,
                    op,
                    commit,
                    entries,
                } => trace(format!(
                    "t={now} r{id} -> r{to} StartView v={view} op={op} c={commit} first={:?}",
                    entries.first().map(|e| (e.op, e.view))
                )),
                Msg::DoViewChange {
                    view,
                    last_normal,
                    op,
                    commit,
                    ..
                } => trace(format!(
                    "t={now} r{id} -> r{to} DVC v={view} ln={last_normal} op={op} c={commit}"
                )),
                Msg::NewState {
                    view,
                    commit,
                    after,
                    entries,
                } => trace(format!(
                    "t={now} r{id} -> r{to} NewState v={view} c={commit} after={after} n={}",
                    entries.len()
                )),
                Msg::GetState { view, after } => trace(format!(
                    "t={now} r{id} -> r{to} GetState v={view} after={after}"
                )),
                Msg::PrepareOk { view, op } if std::env::var_os("FUSEE_VSR_ACKS").is_some() => {
                    trace(format!("t={now} r{id} -> r{to} PrepareOk v={view} op={op}"))
                }
                _ => {}
            }
        }
        match a {
            Action::Send { to, msg } => {
                if faults && rng.chance(30) {
                    continue; // dropped
                }
                let copies = if faults && rng.chance(20) { 2 } else { 1 };
                for _ in 0..copies {
                    let delay = 1 + rng.below(if faults { 12 } else { 3 });
                    *seq += 1;
                    net.insert((now + delay, *seq), (id, to, msg.clone()));
                }
            }
            Action::Persist { entries } => {
                if let Ok(Some(w)) = std::env::var("FUSEE_VSR_OP").map(|v| v.parse::<u64>().ok())
                    && let Some(e) = entries.iter().find(|e| e.op == w)
                {
                    trace(format!(
                        "t={now} r{id} PERSIST op {w} sum {:x} view {} ({:?})",
                        e.checksum, e.view, view
                    ));
                }
                let d = &mut nodes[i].disk;
                if let Some(first) = entries.first()
                    && first.op != d.last() + 1
                {
                    return Err(format!(
                        "replica {id} persisted op {} after {}",
                        first.op,
                        d.last()
                    ));
                }
                d.pending.push(Write::Append(entries));
            }
            Action::Replace { after, entries } => {
                if let Ok(Some(w)) = std::env::var("FUSEE_VSR_OP").map(|v| v.parse::<u64>().ok())
                    && let Some(e) = entries.iter().find(|e| e.op == w)
                {
                    trace(format!(
                        "t={now} r{id} REPLACE sets op {w} sum {:x} view {}",
                        e.checksum, e.view
                    ));
                }
                if after < commits[i] && entries.first().is_none_or(|e| e.op > commits[i]) {
                    // Allowed only for repair, which rewrites committed
                    // entries unchanged (checked when they commit again).
                }
                nodes[i].disk.pending.push(Write::Replace(after, entries));
            }
            Action::Committed { commit } => {
                let r = nodes[i].replica.as_ref().ok_or("no replica")?;
                if r.is_primary() && commit > r.durable() {
                    AHEAD.with(|a| a.set(a.get() + 1));
                }
                for op in (commits[i] + 1)..=commit {
                    let e = r.entry(op).ok_or_else(|| {
                        format!("replica {id} committed op {op} it does not hold")
                    })?;
                    match committed.get(&op) {
                        Some(sum) if *sum != e.checksum => {
                            return Err(format!(
                                "replica {id} committed a different entry at op {op} (entry view {}, commit {commit})",
                                e.view
                            ));
                        }
                        Some(_) => {}
                        None => {
                            if std::env::var("FUSEE_VSR_OP")
                                .ok()
                                .and_then(|v| v.parse::<u64>().ok())
                                == Some(op)
                            {
                                trace(format!(
                                    "t={now} r{id} FIRST COMMIT op {op} sum {:x} view {} (replica view {:?})",
                                    e.checksum, e.view, view
                                ));
                            }
                            committed.insert(op, e.checksum);
                        }
                    }
                }
                commits[i] = commits[i].max(commit);
                // The host keeps the commit number as a hint for restarts.
                let d = &mut nodes[i].disk;
                d.commit = d.commit.max(commit);
            }
            Action::SaveView {
                view,
                last_normal,
                commit,
            } => {
                let d = &mut nodes[i].disk;
                // Barrier: the log it describes is durable first.
                d.flush();
                d.view = view;
                d.last_normal = last_normal;
                d.commit = d.commit.max(commit);
            }
            Action::Role { .. } => {}
            Action::SaveMembership { membership } => {
                // A barrier like SaveView: the log it follows is durable.
                if std::env::var_os("FUSEE_VSR_EPOCHS").is_some() {
                    eprintln!(
                        "t={now} r{id} enters epoch {} {:?} (start op {})",
                        membership.epoch,
                        membership.members,
                        membership.start()
                    );
                }
                let d = &mut nodes[i].disk;
                d.flush();
                d.membership = Some(membership);
            }
        }
    }
    // A barrier flush made more of the log durable.
    let last = nodes[i]
        .disk
        .durable
        .last()
        .map_or(nodes[i].disk.snapshot.len() as Op, |e| e.op);
    if nodes[i].disk.pending.is_empty()
        && let Some(r) = nodes[i].replica.as_mut()
        && last > r.durable()
        && last <= r.op()
    {
        let mut more = Vec::new();
        r.on_durable(last, &mut more);
        if !more.is_empty() {
            out.extend(more);
            return apply(
                id, nodes, out, net, seq, rng, now, faults, committed, commits,
            );
        }
    }
    // A restarted replica's commit restarts from its saved hint.
    if let Some(r) = nodes[i].replica.as_ref()
        && r.commit() < commits[i]
    {
        commits[i] = r.commit();
    }
    Ok(())
}

#[test]
fn replication_survives_crashes_partitions_and_damage() {
    let seeds: u64 = std::env::var("FUSEE_VSR_SEEDS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(60);
    let first: u64 = std::env::var("FUSEE_VSR_FIRST")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    let mut total = 0;
    let mut crashes = 0;
    let mut repairs = 0;
    let mut checkpoints = 0;
    for seed in first..first + seeds {
        let n = if seed % 3 == 0 { 5 } else { 3 };
        match run(seed, n, 4_000) {
            Ok(o) => {
                total += o.committed;
                crashes += o.crashes;
                repairs += o.repairs;
                checkpoints += o.checkpoints;
            }
            Err(e) => {
                TRACE.with(|t| {
                    for l in t.borrow().iter().rev().take(80).rev() {
                        eprintln!("{l}");
                    }
                });
                panic!("seed {seed} (n={n}): {e}")
            }
        }
    }
    eprintln!(
        "{seeds} seeds: {total} entries committed, {crashes} crashes, {repairs} damaged-entry repairs, {checkpoints} checkpoint publications or installations"
    );
}

/// Commit on any durable quorum: the primary's own flush is rare, so most
/// entries commit on a quorum of backups before the primary holds them, and
/// the primary releases them at once. Crashes then keep only the primary's
/// shorter durable prefix.
#[test]
fn a_slow_primary_disk_never_loses_entries_a_backup_quorum_committed() {
    let seeds: u64 = std::env::var("FUSEE_VSR_SEEDS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(60);
    let first: u64 = std::env::var("FUSEE_VSR_FIRST")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    let (mut total, mut crashes, mut ahead) = (0, 0, 0);
    for seed in first..first + seeds {
        let n = if seed % 3 == 0 { 5 } else { 3 };
        // A primary flushes 1-in-100 to 1-in-10 milliseconds; backups 1-in-2.
        let primary = 10 + seed % 91;
        match run_with(seed, n, 4_000, primary) {
            Ok(o) => {
                total += o.committed;
                crashes += o.crashes;
                ahead += o.ahead_of_primary;
            }
            Err(e) => {
                TRACE.with(|t| {
                    for l in t.borrow().iter().rev().take(80).rev() {
                        eprintln!("{l}");
                    }
                });
                panic!("seed {seed} (n={n}, primary flush {primary}/1000): {e}")
            }
        }
    }
    assert!(ahead > 0, "no commit ran ahead of the primary's own flush");
    eprintln!(
        "{seeds} seeds with a slow primary disk: {total} entries committed, {ahead} commits ahead of the primary's durable log, {crashes} crashes"
    );
}

fn seeds() -> std::ops::Range<u64> {
    let seeds: u64 = std::env::var("FUSEE_VSR_SEEDS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(60);
    let first: u64 = std::env::var("FUSEE_VSR_FIRST")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    first..first + seeds
}

fn dump_trace() {
    // FUSEE_VSR_TRACE_LINES: how much of the trace to print (default 80).
    let lines = std::env::var("FUSEE_VSR_TRACE_LINES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(80);
    TRACE.with(|t| {
        for l in t.borrow().iter().rev().take(lines).rev() {
            eprintln!("{l}");
        }
    });
}

/// The default failure detection (heartbeat 5 ms, election 40 ms) against
/// message delays of up to 12 ms under faults: elections are frequent while
/// faults last, and the group must stay safe and make progress after.
#[test]
fn fast_failure_detection_stays_safe_and_live() {
    let (mut total, mut views) = (0, 0);
    let range = seeds();
    let count = range.end - range.start;
    for seed in range {
        let n = if seed % 3 == 0 { 5 } else { 3 };
        let p = Params {
            timing: Timing::default(),
            ..Params::new(n, 4_000)
        };
        match run_params(seed, &p) {
            Ok(o) => {
                total += o.committed;
                views += o.view_changes;
            }
            Err(e) => {
                dump_trace();
                panic!("seed {seed} (n={n}, default timing): {e}")
            }
        }
    }
    eprintln!("{count} seeds with default timing: {total} entries committed, {views} views");
}

/// Read leases with every member's clock off by up to 0.9 % (the rule
/// assumes 1 %), the default timing and the simulator's timing, crashes,
/// partitions and lost messages: whenever a ready primary holds a lease,
/// no member is normal in a newer view and nothing committed is missing
/// from the primary.
#[test]
fn read_leases_hold_under_bounded_clock_drift() {
    let (mut total, mut lease) = (0, 0);
    let range = seeds();
    let count = range.end - range.start;
    for seed in range {
        let n = if seed % 3 == 0 { 5 } else { 3 };
        let timing = if seed % 2 == 0 {
            Timing::default()
        } else {
            Params::new(n, 0).timing
        };
        let p = Params {
            timing,
            drift_ppm: 9_000,
            ..Params::new(n, 4_000)
        };
        match run_params(seed, &p) {
            Ok(o) => {
                total += o.committed;
                lease += o.lease_ms;
            }
            Err(e) => {
                dump_trace();
                panic!("seed {seed} (n={n}, drift 0.9 %): {e}")
            }
        }
    }
    assert!(lease > 0, "no primary ever held a lease");
    eprintln!(
        "{count} seeds with clock drift up to 0.9 %: {total} entries committed, {lease} ms under a valid lease"
    );
}

/// Negative controls: the checks above fail when backups ignore their
/// promises, and when clocks drift far beyond the assumed bound.
#[test]
fn read_leases_fail_without_promises_or_beyond_the_drift_bound() {
    let first_failure = |p: &Params| {
        (0..1000u64).find(|seed| {
            let p = Params {
                n: if seed % 3 == 0 { 5 } else { 3 },
                ..p.clone()
            };
            run_params(*seed, &p).is_err_and(|e| e.contains("lease"))
        })
    };
    let no_promises = Params {
        ignore_promises: true,
        ..Params::new(3, 4_000)
    };
    let found = first_failure(&no_promises);
    eprintln!("without promises, first lease violation at seed {found:?}");
    assert!(found.is_some(), "ignoring promises never broke a lease");
    let drift = Params {
        drift_ppm: 900_000,
        ..Params::new(3, 4_000)
    };
    let found = first_failure(&drift);
    eprintln!("with clocks off by up to 90 %, first lease violation at seed {found:?}");
    assert!(
        found.is_some(),
        "drift beyond the bound never broke a lease"
    );
}

/// Membership changes under crashes, partitions, lost messages, damage and
/// clock drift, with read leases checked throughout: 3 → 5 → 3 members (5
/// → 7 → 5 for a third of the seeds), then a member lost for good and
/// replaced. Every change must complete after the faults stop, removed
/// members retire, and every final member holds every committed entry.
#[test]
fn membership_changes_survive_crashes_during_reconfiguration() {
    let (mut total, mut epochs, mut forwarded, mut crashes) = (0, 0, 0, 0);
    let range = seeds();
    let count = range.end - range.start;
    for seed in range {
        let n = if seed % 3 == 0 { 5 } else { 3 };
        let p = Params {
            reconfig: true,
            drift_ppm: 9_000,
            timing: if seed % 2 == 0 {
                Timing::default()
            } else {
                Params::new(n, 0).timing
            },
            ..Params::new(n, 4_000)
        };
        match run_params(seed, &p) {
            Ok(o) => {
                total += o.committed;
                epochs += o.epochs;
                forwarded += o.reconfig_msgs;
                crashes += o.crashes;
            }
            Err(e) => {
                dump_trace();
                panic!("seed {seed} (n={n}, membership changes): {e}")
            }
        }
    }
    eprintln!(
        "{count} seeds with membership changes: {total} entries committed, {epochs} epochs entered, {forwarded} requests through a backup, {crashes} crashes"
    );
}

/// Slow failure detection, for tests about paging and repair, not timeouts.
fn slow() -> Timing {
    Timing {
        heartbeat_ms: 30,
        election_ms: 300,
    }
}

#[test]
fn primary_elect_fetches_multiple_pages_from_the_selected_donor() {
    let mut entries = Vec::new();
    let mut parent = 0;
    for op in 1..=4_101 {
        let entry = Entry::new(op, 0, parent, Arc::from(&b"operation"[..]));
        parent = entry.checksum;
        entries.push(entry);
    }
    let mut donor = Replica::new(
        0,
        3,
        Recovered {
            commit: 4_100,
            entries: entries.clone(),
            view: 1,
            last_normal: 0,
            ..Recovered::default()
        },
        slow(),
        0,
        0,
    );
    let mut candidate = Replica::new(1, 3, Recovered::default(), slow(), 0, 0);
    // Long past its restart: no promise holds it back.
    candidate.promised_until = 0;
    let mut actions = Vec::new();
    candidate.start_view_change(1, 1, &mut actions);
    actions.clear();
    candidate.on_message(
        0,
        Msg::DoViewChange {
            view: 1,
            last_normal: 0,
            op: 4_101,
            commit: 4_100,
            entries: vec![entries[4_100].clone()],
        },
        2,
        &mut actions,
    );
    let request = actions
        .iter()
        .find_map(|action| match action {
            Action::Send {
                to: 0,
                msg: msg @ Msg::GetEntries { .. },
            } => Some(msg.clone()),
            _ => None,
        })
        .expect("missing prefix request");
    actions.clear();
    donor.on_message(1, request, 3, &mut actions);
    let page = actions
        .into_iter()
        .find_map(|action| match action {
            Action::Send {
                to: 1,
                msg: msg @ Msg::Entries { .. },
            } => Some(msg),
            _ => None,
        })
        .unwrap();
    let mut actions = Vec::new();
    candidate.on_message(2, page.clone(), 4, &mut actions);
    assert!(candidate.adopting.as_ref().unwrap().fetched.is_empty());
    candidate.on_message(0, page, 5, &mut actions);
    assert_eq!(candidate.status(), Status::ViewChange);
    assert_eq!(candidate.op(), 0);
    assert_eq!(candidate.adopting.as_ref().unwrap().fetched.len(), 4096);
    // Drop the first follow-up. A heartbeat must ask for the missing range again.
    actions.clear();
    candidate.on_tick(45, &mut actions);
    let next = actions
        .into_iter()
        .find_map(|a| match a {
            Action::Send {
                to: 0,
                msg:
                    msg @ Msg::GetEntries {
                        from: 4097,
                        to: 4100,
                        ..
                    },
            } => Some(msg),
            _ => None,
        })
        .expect("missing catch-up retry");
    let mut actions = Vec::new();
    donor.on_message(1, next, 46, &mut actions);
    let final_page = actions
        .into_iter()
        .find_map(|a| match a {
            Action::Send {
                to: 1,
                msg: msg @ Msg::Entries { .. },
            } => Some(msg),
            _ => None,
        })
        .unwrap();
    candidate.on_message(0, final_page, 47, &mut Vec::new());
    assert!(candidate.is_primary());
    assert_eq!(candidate.op(), 4101);
    assert_eq!(candidate.entry(4101).unwrap().checksum, parent);
}

#[test]
fn large_state_transfer_retries_verified_pages_before_installing_the_log() {
    // Three entries fit per preferred page. A complete state needs three frames.
    let payload: Payload = Arc::from(vec![38u8; 300_000]);
    let mut entries = Vec::new();
    let mut parent = 0;
    for op in 1..=8 {
        let e = Entry::new(op, 1, parent, payload.clone());
        parent = e.checksum;
        entries.push(e);
    }
    let mut donor = Replica::new(
        1,
        3,
        Recovered {
            view: 1,
            last_normal: 1,
            commit: 8,
            entries: entries.clone(),
            ..Recovered::default()
        },
        slow(),
        0,
        0,
    );
    // An already elected primary has these same entries and watermarks.
    donor.status = Status::Normal;
    let mut receiver = Replica::new(
        2,
        3,
        Recovered {
            view: 0,
            last_normal: 0,
            commit: 1,
            entries: entries[..1].to_vec(),
            ..Recovered::default()
        },
        slow(),
        0,
        0,
    );
    let mut actions = Vec::new();
    donor.on_message(2, Msg::GetState { view: 1, after: 1 }, 1, &mut actions);
    let offer = actions
        .into_iter()
        .find_map(|a| match a {
            Action::Send {
                to: 2,
                msg: msg @ Msg::StateOffer { .. },
            } => Some(msg),
            _ => None,
        })
        .unwrap();
    let mut wire = Vec::new();
    encode_msg(&offer, &mut wire);
    let mut actions = Vec::new();
    receiver.on_message(1, decode_msg(&wire).unwrap(), 2, &mut actions);
    assert_eq!(receiver.status(), Status::Recovering);
    // Drop the initial request. Poll the receiver's normal repair timer.
    actions.clear();
    receiver.on_tick(200, &mut actions);
    let mut time = 201;
    let mut pages = 0;
    while receiver.status() == Status::Recovering {
        let req = actions
            .drain(..)
            .find_map(|a| match a {
                Action::Send {
                    to: 1,
                    msg: msg @ Msg::GetEntries { .. },
                } => Some(msg),
                _ => None,
            })
            .expect("requested a continuation");
        donor.on_message(2, req, time, &mut actions);
        let page = actions
            .drain(..)
            .find_map(|a| match a {
                Action::Send {
                    to: 2,
                    msg: msg @ Msg::Entries { .. },
                } => Some(msg),
                _ => None,
            })
            .unwrap();
        let mut raw = Vec::new();
        encode_msg(&page, &mut raw);
        assert!(raw.len() < (1 << 20));
        if pages == 0 {
            let mut damaged = page.clone();
            if let Msg::Entries { entries, .. } = &mut damaged {
                entries[0].parent ^= 1;
            }
            receiver.on_message(1, damaged, time, &mut actions);
            assert!(receiver.transfer.as_ref().unwrap().fetched.is_empty());
        }
        receiver.on_message(1, page, time, &mut actions);
        if receiver.status() == Status::Recovering {
            assert_eq!(receiver.op(), 1);
            assert!(!actions.iter().any(|a| matches!(a, Action::Replace { .. })));
        }
        pages += 1;
        time += 1;
        assert!(pages <= 3);
    }
    assert_eq!(pages, 3);
    assert_eq!(receiver.op(), 8);
    assert_eq!(receiver.entry(8).unwrap().checksum, parent);
    assert!(
        actions
            .iter()
            .any(|a| matches!(a, Action::Replace { after: 1, .. }))
    );
}

#[test]
fn repair_accepts_a_prepared_copy_only_when_an_intact_successor_authenticates_it() {
    let mut chain = Vec::new();
    let mut parent = 0;
    for op in 1..=5 {
        let e = Entry::new(op, 1, parent, Arc::from(op.to_le_bytes()));
        parent = e.checksum;
        chain.push(e);
    }
    let restart = |missing: Op, commit: Op| {
        Replica::new(
            0,
            3,
            Recovered {
                view: 1,
                last_normal: 1,
                commit,
                entries: chain.iter().filter(|e| e.op != missing).cloned().collect(),
                damaged: Some((missing, missing)),
                ..Recovered::default()
            },
            slow(),
            1,
            0,
        )
    };
    // Another damaged replica retained op 3, but never learned its commit.
    let mut receiver = restart(3, 5);
    let mut actions = Vec::new();
    let wrong = Entry::new(3, 1, chain[1].checksum, Arc::from([77]));
    receiver.on_message(
        2,
        Msg::RepairEntries {
            entries: vec![wrong],
            commit: 2,
        },
        1,
        &mut actions,
    );
    assert_eq!(receiver.status(), Status::Recovering);
    assert!(receiver.entry(3).is_none());
    receiver.on_message(
        2,
        Msg::RepairEntries {
            entries: vec![chain[2].clone()],
            commit: 2,
        },
        2,
        &mut actions,
    );
    assert_eq!(receiver.entry(3), Some(&chain[2]));
    assert_eq!(receiver.status(), Status::Normal);
    // Both voting copies of the last committed entry are gone. A surviving
    // older prepare without a successor or a known commit cannot replace it.
    let mut receiver = restart(5, 5);
    actions.clear();
    for now in 1..=500 {
        receiver.on_message(
            2,
            Msg::RepairEntries {
                entries: vec![chain[4].clone()],
                commit: 4,
            },
            now,
            &mut actions,
        );
        receiver.on_tick(now, &mut actions);
    }
    assert_eq!(receiver.status(), Status::Recovering);
    assert_eq!(receiver.commit(), 5);
    assert!(receiver.entry(5).is_none());
    assert!(!actions.iter().any(|a| matches!(
        a,
        Action::Send {
            msg: Msg::PrepareOk { .. } | Msg::DoViewChange { .. },
            ..
        }
    )));
    receiver.on_message(
        2,
        Msg::RepairEntries {
            entries: vec![chain[4].clone()],
            commit: 5,
        },
        501,
        &mut actions,
    );
    assert_eq!(receiver.entry(5), Some(&chain[4]));
}

/// Found by the slow-primary simulation (seed 1417): a replica restarted
/// with a hole at op 3 had acknowledged ops 4 and 5, and a quorum with it
/// committed them. An image whose suffix stops before the hole must not
/// replace its log (it would drop 4 and 5); one that reaches 5 may.
#[test]
fn a_damaged_replica_keeps_acknowledged_entries_after_its_hole_when_installing_an_image() {
    let mut chain = Vec::new();
    let mut parent = 0;
    for op in 1..=5 {
        let e = Entry::new(op, 1, parent, Arc::from(op.to_le_bytes()));
        parent = e.checksum;
        chain.push(e);
    }
    let r = Replica::new(
        0,
        3,
        Recovered {
            view: 1,
            last_normal: 1,
            commit: 1,
            entries: chain.iter().filter(|e| e.op != 3).cloned().collect(),
            damaged: Some((3, 3)),
            ..Recovered::default()
        },
        slow(),
        1,
        0,
    );
    assert!(!r.checkpoint_matches(1, chain[0].checksum, &chain[1..2]));
    assert!(r.checkpoint_matches(1, chain[0].checksum, &chain[1..5]));
}

#[test]
fn checkpoint_donor_cannot_replace_an_acknowledged_prepare_or_lower_the_view() {
    let a = Entry::new(1, 1, 0, Arc::from([1]));
    let b = Entry::new(2, 1, a.checksum, Arc::from([2]));
    let mut r = Replica::new(
        2,
        3,
        Recovered {
            view: 1,
            last_normal: 1,
            commit: 1,
            entries: vec![a.clone(), b.clone()],
            ..Recovered::default()
        },
        slow(),
        1,
        0,
    );
    assert!(!r.accepts_checkpoint(0, 1, 1, 1));
    assert!(r.accepts_checkpoint(1, 1, 1, 1));
    assert!(!r.checkpoint_matches(1, a.checksum, &[]));
    let wrong = Entry::new(2, 1, a.checksum, Arc::from([99]));
    assert!(!r.checkpoint_matches(1, a.checksum, &[wrong]));
    assert!(r.installed_checkpoint(1, 1, 1, a.checksum, 1, vec![b.clone()], 2, &mut Vec::new()));
    assert_eq!(r.durable(), 2);
    assert_eq!(r.entry(2), Some(&b));
    let mut r = Replica::new(
        2,
        3,
        Recovered {
            view: 3,
            last_normal: 1,
            commit: 2,
            entries: vec![a],
            damaged: Some((2, 2)),
            ..Recovered::default()
        },
        slow(),
        1,
        0,
    );
    assert!(!r.accepts_checkpoint(1, 1, 1, 2));
    assert!(r.installed_checkpoint(1, 1, 2, b.checksum, 2, vec![], 3, &mut Vec::new()));
    assert_eq!(r.view(), 3);
    assert_eq!(r.status(), Status::Recovering);
}
