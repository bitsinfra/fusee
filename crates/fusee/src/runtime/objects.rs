//! Keyed objects in the shard runtime (see [`crate::object`]): which calls
//! wait for their key, the state an exclusive handler commits, calls from
//! workflow code, and compaction. The hooks that call these are in
//! `runtime.rs`, where updates are offered, decided and finished.

use super::*;
use crate::object::{CallWaiter, ObjectRun, PendingCall, Snapshot, Waiting};

fn null() -> Payload {
    Arc::from(&b"null"[..])
}

impl<E: Env> Inner<E> {
    /// Answers a call to a key without offering it: one that finished
    /// before the key's last compaction (its kept result), and a one-way
    /// call the key accepted already. Returns whether it did.
    pub(super) fn object_answered(&mut self, run: RunId, id: &str, token: Option<u64>) -> bool {
        let Some(l) = self
            .core
            .get(run)
            .filter(|l| l.policy & policy::OBJECT != 0 && !l.is_done())
        else {
            return false;
        };
        let dedup = self.cfg.sabotage != Sabotage::DuplicateUpdates;
        let known = dedup && l.update(id).is_some();
        let send = crate::object::is_send(id);
        let answer = if known {
            send.then(|| (Status::Ok, null()))
        } else if dedup {
            let kept = self.obj.runs.get(&run).and_then(|o| o.kept(id));
            kept.map(|k| {
                if send {
                    (Status::Ok, null())
                } else {
                    (k.status, k.result.clone())
                }
            })
        } else {
            None
        };
        let Some((status, payload)) = answer else {
            return false;
        };
        if let Some(t) = token {
            let at = self.writer.last_lsn();
            self.core.add_output(
                at,
                Output::Reply {
                    token: t,
                    status,
                    payload,
                },
            );
        }
        true
    }

    /// A key accepted a call: its `Update` record names the workflow
    /// waiting for it, the message that brought it is acknowledged once the
    /// record is durable, a one-way call is answered then, and an exclusive
    /// handler owns the key's writes.
    pub(super) fn object_accept(
        &mut self,
        run: RunId,
        ordinal: u64,
        name: &str,
        id: &Name,
        mut rec: RecordBuf,
    ) -> Result<(), Error> {
        let caller = self.obj.callers.remove(&(run, id.clone()));
        if let (Some(c), Record::Update { caller: rc, .. }) = (&caller, &mut rec) {
            *rc = c.parent;
        }
        let lsn = self.append(&rec)?;
        if let Some(from) = caller.and_then(|c| c.from) {
            let t = self.msg_token(from);
            self.core.add_output(lsn, Output::Ack(t));
        }
        if crate::object::is_send(id) {
            for t in self
                .update_waiters
                .remove(&(run, id.clone()))
                .unwrap_or_default()
            {
                self.core.add_output(
                    lsn,
                    Output::Reply {
                        token: t,
                        status: Status::Ok,
                        payload: null(),
                    },
                );
            }
        }
        self.object_started(run, ordinal, name);
        Ok(())
    }

    /// A key's handler returned: an exclusive one commits its writes with
    /// its result (a failed one commits nothing). Returns the change for its
    /// `UpdateDone` record, sealed (empty for none).
    pub(super) fn object_commit(
        &mut self,
        run: RunId,
        ordinal: u64,
        status: Status,
    ) -> Result<Payload, Error> {
        let Some(o) = self
            .obj
            .runs
            .get_mut(&run)
            .filter(|o| o.owner == Some(ordinal))
        else {
            return Ok(empty());
        };
        o.owner = None;
        let writes = std::mem::take(&mut o.writes);
        if status != Status::Ok || writes.is_empty() {
            return Ok(empty());
        }
        for (k, v) in &writes {
            match v {
                Some(v) => o.state.insert(k.clone(), v.clone()),
                None => o.state.remove(k),
            };
        }
        self.seal(Arc::from(crate::object::encode_delta(&writes)))
    }

    pub(super) fn object_run(&self, run: RunId) -> Result<&ObjectRun, Error> {
        self.obj.runs.get(&run).ok_or_else(|| {
            Error::Rejected(format!("run {run} is not a loaded key of an object type"))
        })
    }

    /// A write from the exclusive handler running (`None` clears the key).
    pub(super) fn object_write(
        &mut self,
        run: RunId,
        key: &str,
        value: Option<Payload>,
    ) -> Result<(), Error> {
        if let Some(v) = &value {
            serde_json::from_slice::<serde::de::IgnoredAny>(v)
                .map_err(|e| Error::Codec(format!("a state value is JSON: {e}")))?;
        }
        self.object_run(run)?;
        let Some(o) = self.obj.runs.get_mut(&run).filter(|o| o.owner.is_some()) else {
            return Err(Error::Rejected(
                "state changes only in an exclusive handler, while it runs".into(),
            ));
        };
        let value = value.filter(|v| &v[..] != b"null");
        o.writes.insert(key.to_owned(), value);
        Ok(())
    }

    /// A state edit, as the writes of the exclusive handler running (the
    /// edit handler): applied to the committed state, which that handler
    /// sees, and committed with its result.
    pub(super) fn object_edit(&mut self, run: RunId, edit: &[u8]) -> Result<Payload, Error> {
        let o = self.object_run(run)?;
        if o.owner.is_none() || !o.writes.is_empty() {
            return Err(Error::Rejected(
                "a state edit runs as the key's exclusive handler, before any other write".into(),
            ));
        }
        let (writes, answer) = crate::object::edit::apply(&o.state, edit)?;
        for (k, v) in writes {
            self.object_write(run, &k, v)?;
        }
        Ok(answer)
    }

    /// A call from workflow code as the `Child` operation it is: the key's
    /// workflow ID, the flags, and the input (its ID is made from the
    /// caller's journal position).
    pub(super) fn object_call(
        &self,
        run: RunId,
        wtype: &str,
        key: &str,
        handler: &str,
        arg: &[u8],
        detached: bool,
    ) -> Result<(String, u8, Payload), Error> {
        crate::object::check_type(wtype)?;
        let slot = self
            .slots
            .get(&run)
            .ok_or_else(|| Error::Rejected(format!("run {run} is not active on this shard")))?;
        let call_id = slot.machine.key(slot.machine.next_seq());
        let input: Payload = Arc::from(crate::object::encode_call(handler, &call_id, arg));
        let mut flags = policy::OBJECT | policy::CLOSE_ABANDON;
        if detached {
            flags |= policy::DETACHED;
        }
        Ok((crate::object::object_id(wtype, key), flags, input))
    }

    /// A start of an object type is a key's run: its policy and input are
    /// the engine's, whatever the caller asked. Also names a one-way call
    /// sent without an ID. Returns whether the start is a key's.
    pub(super) fn object_start(&mut self, req: &mut StartRequest) -> Result<bool, Error> {
        if let Some(u) = req.update.as_mut()
            && u.id == crate::object::SEND_PREFIX
        {
            u.id = format!("{}{}", crate::object::SEND_PREFIX, self.fresh_update_id());
        }
        if self.object_def(&req.wtype).is_none() {
            return Ok(false);
        }
        if req.parent.is_some() {
            return Err(Error::Rejected(format!(
                "'{}' is an object type: call its keys with ctx.object(), not as a child workflow",
                req.wtype
            )));
        }
        if crate::object::key_of(&req.id, &req.wtype).is_none_or(str::is_empty) {
            return Err(Error::Rejected(format!(
                "'{}' is not a key of object type '{}' (an ID '{}/<key>')",
                req.id, req.wtype, req.wtype
            )));
        }
        req.policy = policy::OBJECT | policy::RUNNING_USE_EXISTING | policy::FINISHED_ALLOW;
        req.input = Arc::from(Snapshot::default().encode());
        req.sealed = false;
        req.deadline_ms = 0;
        Ok(true)
    }

    /// A running run holds the ID a start of an object type names: it must
    /// be a key of that type.
    pub(super) fn object_check_held(
        &mut self,
        run: RunId,
        req: &StartRequest,
    ) -> Result<(), Error> {
        self.ensure_loaded(run)?;
        let ok = self
            .core
            .get(run)
            .is_some_and(|l| l.policy & policy::OBJECT != 0 && *l.wtype == *req.wtype);
        if ok {
            return Ok(());
        }
        Err(Error::Rejected(format!(
            "'{}' is held by a run that is not an object of type '{}'",
            req.id, req.wtype
        )))
    }

    /// A key's run left memory: its state goes with it (loading the run
    /// rebuilds it from the journal), unless calls wait for the key.
    pub(super) fn object_parked(&mut self, run: RunId) {
        if self
            .obj
            .runs
            .get(&run)
            .is_some_and(|o| o.waiting.is_empty())
        {
            self.obj.runs.remove(&run);
        }
    }

    pub(super) fn object_def(&self, wtype: &str) -> Option<Arc<ObjectDef>> {
        self.registry.read().ok()?.object(wtype)
    }

    /// Whether `handler` of the key's type is shared (unknown: exclusive).
    pub(super) fn object_shared(&self, run: RunId, handler: &str) -> bool {
        self.core
            .get(run)
            .and_then(|l| self.object_def(&l.wtype))
            .is_some_and(|d| d.is_shared(handler))
    }

    /// An exclusive call of the key is offered, running or recorded and
    /// not finished.
    pub(super) fn object_busy(&self, run: RunId) -> bool {
        if self
            .obj
            .runs
            .get(&run)
            .is_some_and(|o| o.offered.is_some() || o.owner.is_some())
        {
            return true;
        }
        let Some(l) = self.core.get(run) else {
            return false;
        };
        let def = self.object_def(&l.wtype);
        l.updates
            .iter()
            .any(|u| u.result.is_none() && !def.as_ref().is_some_and(|d| d.is_shared(&u.name)))
    }

    /// Queues a call that cannot start yet: an exclusive one while the key
    /// is busy, any while a compaction is due. Returns whether it waits. An
    /// exclusive call that may start is marked offered.
    pub(super) fn object_hold(
        &mut self,
        run: RunId,
        name: &str,
        id: &str,
        payload: &Payload,
    ) -> bool {
        let shared = self.object_shared(run, name);
        let busy =
            !shared && self.cfg.sabotage != Sabotage::UnlockedObjects && self.object_busy(run);
        let o = self.obj.runs.entry(run).or_default();
        if o.waiting.iter().any(|w| *w.id == *id) {
            return true;
        }
        if o.compacting || busy {
            if o.waiting.len() >= crate::object::MAX_WAITING {
                const FULL: &str = "busy: too many calls are waiting for this object";
                let tail = self.writer.last_lsn();
                for t in self
                    .update_waiters
                    .remove(&(run, Name::from(id)))
                    .unwrap_or_default()
                {
                    self.core.add_output(
                        tail,
                        Output::Refuse {
                            token: t,
                            message: Arc::from(FULL.as_bytes()),
                        },
                    );
                }
                self.object_fail_caller(run, id, FULL);
                return true;
            }
            o.waiting.push_back(Waiting {
                name: Name::from(name),
                id: Name::from(id),
                payload: payload.clone(),
                shared,
            });
            return true;
        }
        if !shared {
            o.offered = Some(Name::from(id));
        }
        false
    }

    /// A key's handler starts (accepted now, or recorded): an exclusive one
    /// owns the key's writes until it returns.
    pub(super) fn object_started(&mut self, run: RunId, ordinal: u64, handler: &str) {
        if !self.obj.runs.contains_key(&run) || self.object_shared(run, handler) {
            return;
        }
        if let Some(o) = self.obj.runs.get_mut(&run) {
            o.owner = Some(ordinal);
            o.writes.clear();
        }
    }

    /// A workflow's call that will not be accepted: its caller gets the
    /// error as the call's result.
    pub(super) fn object_fail_caller(&mut self, run: RunId, id: &str, message: &str) {
        let Some(c) = self.obj.callers.remove(&(run, Name::from(id))) else {
            return;
        };
        let tail = self.writer.last_lsn();
        if let Some(parent) = c.parent {
            self.core.add_output(
                tail,
                Output::ChildResult {
                    parent,
                    status: Status::Failed,
                    payload: Arc::from(message.as_bytes()),
                },
            );
        }
        if let Some(from) = c.from {
            let t = self.msg_token(from);
            self.core.add_output(tail, Output::Ack(t));
        }
    }

    /// A key's call finished or was refused: compact the key if it is due,
    /// else start what waits.
    pub(super) fn object_settle(&mut self, run: RunId) {
        if self.obj.runs.get(&run).is_none_or(|o| o.compacting) || self.object_busy(run) {
            return;
        }
        let ops = self.slots.get(&run).map_or(0, |s| s.machine.journal_size());
        if ops >= crate::object::COMPACT_OPS && self.object_quiet(run) {
            if let Some(o) = self.obj.runs.get_mut(&run) {
                o.compacting = true;
            }
            self.obj.compact_due.push(run);
            return;
        }
        if let Err(e) = self.object_release(run) {
            self.fail(e.to_string());
        }
    }

    /// Offers the key's waiting shared calls and its first waiting exclusive
    /// one, if the key is free.
    pub(super) fn object_release(&mut self, run: RunId) -> Result<(), Error> {
        let mut busy = self.object_busy(run);
        loop {
            let Some(o) = self.obj.runs.get_mut(&run) else {
                return Ok(());
            };
            if o.compacting {
                return Ok(());
            }
            let pos = if busy {
                o.waiting.iter().position(|w| w.shared)
            } else if o.waiting.is_empty() {
                None
            } else {
                Some(0)
            };
            let Some(w) = pos.and_then(|p| o.waiting.remove(p)) else {
                return Ok(());
            };
            if !w.shared {
                o.offered = Some(w.id.clone());
                busy = true;
            }
            self.offer_now(run, &w.name, &w.id, w.payload)?;
        }
    }

    /// Nothing of the key runs or is about to: its code waits for nothing,
    /// every accepted call returned and no call was handed to the code.
    pub(super) fn object_quiet(&self, run: RunId) -> bool {
        let Some(slot) = self.slots.get(&run) else {
            return false;
        };
        let Some(l) = self.core.get(run) else {
            return false;
        };
        l.policy & policy::OBJECT != 0
            && !l.is_done()
            && !self.stuck.contains_key(&run)
            && slot.machine.is_idle()
            && l.updates.iter().all(|u| u.result.is_some())
            && self
                .obj
                .runs
                .get(&run)
                .is_some_and(|o| o.offered.is_none() && o.owner.is_none())
            && !self.offered.values().any(|(r, ..)| *r == run)
    }

    /// A quiet key with nothing waiting may be parked.
    pub(super) fn object_idle(&self, run: RunId) -> bool {
        self.object_quiet(run)
            && self
                .obj
                .runs
                .get(&run)
                .is_some_and(|o| o.waiting.is_empty() && !o.compacting)
    }

    /// The run may be parked: it waits only on time, signals or children,
    /// or it is a key with nothing to do.
    pub(super) fn parkable(&self, run: RunId) -> bool {
        self.slots
            .get(&run)
            .is_some_and(|s| s.machine.is_suspended())
            || self.object_idle(run)
    }

    /// Continues a quiet key's run as new, with its committed state and
    /// recent calls as the new run's input. Waiting calls move to the new
    /// run. Returns the new run, or `None` if the key is not quiet.
    pub(super) fn object_compact(&mut self, run: RunId) -> Result<Option<RunId>, Error> {
        if !self.object_quiet(run) {
            return Ok(None);
        }
        let now = self.now();
        let mut o = self.obj.runs.remove(&run).unwrap_or_default();
        let mut snap = Snapshot {
            state: std::mem::take(&mut o.state),
            calls: std::mem::take(&mut o.calls),
        };
        let finished: Vec<(Name, Status, Payload)> = self
            .core
            .get(run)
            .map(|l| {
                l.updates
                    .iter()
                    .filter(|u| !crate::object::is_auto_id(&u.id))
                    .filter_map(|u| {
                        let (s, p, _) = u.result.clone()?;
                        Some((u.id.clone(), s, p))
                    })
                    .collect()
            })
            .unwrap_or_default();
        for (id, status, p) in finished {
            let result = if status == Status::Ok {
                self.open(p)?
            } else {
                p
            };
            snap.calls.push_back(crate::object::Kept {
                id,
                status,
                result,
                at_ms: now,
            });
        }
        snap.prune(now);
        let ids: Vec<Name> = o.waiting.iter().map(|w| w.id.clone()).collect();
        let mut waiters = Vec::new();
        let mut callers = Vec::new();
        for id in &ids {
            if let Some(t) = self.update_waiters.remove(&(run, id.clone())) {
                waiters.push((id.clone(), t));
            }
            if let Some(c) = self.obj.callers.remove(&(run, id.clone())) {
                callers.push((id.clone(), c));
            }
        }
        let new = self.continue_as_new(run, Arc::from(snap.encode()), None)?;
        for (id, t) in waiters {
            self.update_waiters.insert((new, id), t);
        }
        for (id, c) in callers {
            self.obj.callers.insert((new, id), c);
        }
        let n = self.obj.runs.entry(new).or_default();
        n.waiting = o.waiting;
        self.metrics.object_compactions += 1;
        Ok(Some(new))
    }

    /// A key's run got a slot: its state as of the start of its journal,
    /// `input` (its start's input).
    pub(super) fn object_on_slot(&mut self, run: RunId, input: Payload) {
        let snap = self
            .open(input)
            .ok()
            .and_then(|i| Snapshot::decode(&i).ok())
            .unwrap_or_default();
        self.obj.runs.entry(run).or_default().reset(snap);
    }

    /// The key's run ended or stopped: calls waiting for it are refused.
    pub(super) fn object_ended(&mut self, run: RunId, message: &str) {
        let Some(o) = self.obj.runs.remove(&run) else {
            return;
        };
        let tail = self.writer.last_lsn();
        for w in o.waiting {
            for t in self
                .update_waiters
                .remove(&(run, w.id.clone()))
                .unwrap_or_default()
            {
                self.core.add_output(
                    tail,
                    Output::Refuse {
                        token: t,
                        message: Arc::from(message.as_bytes()),
                    },
                );
            }
            self.object_fail_caller(run, &w.id, message);
        }
        let callers: Vec<Name> = self
            .obj
            .callers
            .keys()
            .filter(|k| k.0 == run)
            .map(|k| k.1.clone())
            .collect();
        for id in callers {
            self.object_fail_caller(run, &id, message);
        }
    }

    /// What `inspect` shows of a key: its committed state from the journal
    /// alone, its running calls and the ones waiting for it.
    pub(super) fn object_detail(&self, run: RunId, journal: &[RecordBuf]) -> Option<ObjectDetail> {
        if !crate::object::is_object_journal(journal) {
            return None;
        }
        let open = |p: Payload| {
            self.open(p)
                .map_err(|e| crate::error::Error::Codec(e.to_string()))
        };
        let (state, running) = crate::object::view(journal, &open).unwrap_or_default();
        let version = crate::object::edit::version(&state);
        let state = serde_json::from_slice(&crate::object::state_json(&state))
            .unwrap_or(serde_json::Value::Null);
        let waiting = self
            .obj
            .runs
            .get(&run)
            .map(|o| {
                o.waiting
                    .iter()
                    .map(|w| PendingCall {
                        handler: w.name.to_string(),
                        id: w.id.to_string(),
                    })
                    .collect()
            })
            .unwrap_or_default();
        Some(ObjectDetail {
            state,
            version,
            running,
            waiting,
        })
    }
}

impl<E: Env + 'static> ShardRuntime<E> {
    /// A workflow's call to a key (a `Child` record flagged
    /// [`policy::OBJECT`]): creates the key's run if it has none, then
    /// offers the call, which the key accepts in its turn. The message is
    /// acknowledged once the call's `Update` record (which names the caller)
    /// is durable, and resent until then, so the call is accepted once. A
    /// call the key already has is answered from its record.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn object_call_message(
        &self,
        parent: ParentRef,
        wtype: String,
        id: String,
        flags: u8,
        input: Payload,
        token: Option<u64>,
        from: Option<(u32, u64)>,
    ) -> Result<(), Error> {
        if let Some(t) = token {
            self.inner.borrow_mut().msg_tokens.remove(&t);
        }
        let caller = (flags & policy::DETACHED == 0).then_some(parent);
        let fail = |message: String| -> Result<(), Error> {
            let mut i = self.inner.borrow_mut();
            let tail = i.writer.last_lsn();
            if let Some(p) = caller {
                i.core.add_output(
                    tail,
                    Output::ChildResult {
                        parent: p,
                        status: Status::Failed,
                        payload: Arc::from(message.as_bytes()),
                    },
                );
            }
            if let Some(f) = from {
                i.ack_at_tail(f);
            }
            i.on_commit();
            Ok(())
        };
        if self.inner.borrow().object_def(&wtype).is_none() {
            // No code for the type here yet (an engine still registering
            // its objects): the sender sends the call again.
            return Ok(());
        }
        let call = {
            let i = self.inner.borrow();
            i.open(input).and_then(|p| {
                crate::object::decode_call(&p).map_err(|e| Error::Codec(e.to_string()))
            })
        };
        let (handler, call_id, arg) = match call {
            Ok(c) => c,
            Err(e) => return fail(e.to_string()),
        };
        let req = StartRequest::new(&id, &wtype, Arc::from(&b"null"[..]), IdPolicy::default());
        let run = match self.start_locked(req) {
            Ok(StartOutcome::Created { run } | StartOutcome::Existing { run, .. }) => run,
            Err(Error::Busy) => return Ok(()),
            Err(e) => return fail(e.to_string()),
        };
        {
            let mut i = self.inner.borrow_mut();
            i.ensure_loaded(run)?;
            let tail = i.writer.last_lsn();
            let recorded = i
                .core
                .get(run)
                .and_then(|l| l.update(&call_id).map(|(_, s)| s.result.clone()));
            let kept = i
                .obj
                .runs
                .get(&run)
                .and_then(|o| o.kept(&call_id))
                .map(|k| (k.status, k.result.clone()));
            match (recorded, kept) {
                (Some(result), _) => {
                    // Accepted before: its `UpdateDone` answers the caller.
                    if let (Some(p), Some((status, payload, lsn))) = (caller, result) {
                        i.core.add_output(
                            lsn,
                            Output::ChildResult {
                                parent: p,
                                status,
                                payload,
                            },
                        );
                    }
                    if let Some(f) = from {
                        i.ack_at_tail(f);
                    }
                }
                (None, Some((status, plain))) => {
                    // Finished before the key's last compaction.
                    if let Some(p) = caller {
                        let payload = if status == Status::Ok {
                            i.seal(plain)?
                        } else {
                            plain
                        };
                        i.core.add_output(
                            tail,
                            Output::ChildResult {
                                parent: p,
                                status,
                                payload,
                            },
                        );
                    }
                    if let Some(f) = from {
                        i.ack_at_tail(f);
                    }
                }
                (None, None) => {
                    i.obj.callers.insert(
                        (run, Name::from(call_id.as_str())),
                        CallWaiter {
                            parent: caller,
                            from,
                        },
                    );
                    i.offer_update(run, &handler, &call_id, arg, None)?;
                }
            }
            i.on_commit();
        }
        self.instantiate_pending();
        Ok(())
    }

    /// Compacts the keys whose compaction came due (see [`crate::object`]),
    /// at a point of the tick where no code is being polled.
    pub(super) fn compact_objects(&self) -> bool {
        let due = std::mem::take(&mut self.inner.borrow_mut().obj.compact_due);
        if due.is_empty() {
            return false;
        }
        {
            let mut i = self.inner.borrow_mut();
            for run in due {
                let r = match i.object_compact(run) {
                    Ok(Some(new)) => i.object_release(new),
                    Ok(None) => {
                        if let Some(o) = i.obj.runs.get_mut(&run) {
                            o.compacting = false;
                        }
                        i.object_release(run)
                    }
                    Err(e) => Err(e),
                };
                if let Err(e) = r {
                    i.fail(e.to_string());
                }
            }
            i.on_commit();
        }
        self.instantiate_pending();
        true
    }
}
