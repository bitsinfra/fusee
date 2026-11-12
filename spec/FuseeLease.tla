----------------------------- MODULE FuseeLease -----------------------------
(***************************************************************************)
(* Read leases of Fusee's replication (crates/fusee-core/src/vsr.rs).     *)
(*                                                                         *)
(* A primary pings its backups; a backup normal in the primary's view     *)
(* answers and promises not to take part in any view change until its     *)
(* own clock has advanced Promise past the moment it received the ping.   *)
(* The primary may serve reads from its own state while its clock is      *)
(* less than Lease past the stamp (its clock when it sent the ping) of     *)
(* the oldest answer among a quorum (itself included).                    *)
(*                                                                         *)
(* Time: real time advances one tick at a time, and each replica's clock  *)
(* advances by MinRate..MaxRate per tick, independently: the bounded      *)
(* clock-rate assumption. Messages are delayed arbitrarily (a message     *)
(* never delivered is lost; a newer ping or answer replaces an older one  *)
(* in flight). Replicas crash; a restarted replica cannot remember its   *)
(* promises and abstains for Promise from its restart.                    *)
(*                                                                         *)
(* Replication itself is abstracted to what reads observe: a primary      *)
(* commits a write once a quorum normal in its view holds it, and a view  *)
(* change installs the highest state of its quorum (FuseeReplication.tla  *)
(* checks that this never loses a committed write).                       *)
(*                                                                         *)
(* Safety: while a primary holds a lease, its state has every committed   *)
(* write and no replica is normal in a newer view. With                   *)
(* Lease <= Promise * MinRate / MaxRate both hold; dropping the promise   *)
(* rule (PromiseRule = FALSE) or ignoring drift (Lease = Promise) breaks  *)
(* them.                                                                  *)
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets

CONSTANTS Replicas, Order, MaxView, MaxWrites, MaxTime, MaxCrashes,
          Promise, Lease, MinRate, MaxRate, PromiseRule

ASSUME Cardinality(Replicas) = Len(Order)
ASSUME MinRate >= 1 /\ MaxRate >= MinRate

Primary(v) == Order[(v % Len(Order)) + 1]

Quorums == {Q \in SUBSET Replicas : 2 * Cardinality(Q) > Cardinality(Replicas)}

\* A message slot: live, with the view and stamp of the ping it belongs to.
None == [view |-> 0, stamp |-> 0, live |-> FALSE]

VARIABLES
    now,       \* real time, in ticks
    clock,     \* clock[r]: r's own clock
    view,      \* view[r]
    normal,    \* normal[r]: r is normal in view[r] (not changing views)
    promise,   \* promise[r]: r takes part in no view change while clock[r] < promise[r]
    acks,      \* acks[p][b]: newest ping stamp b echoed to p in p's view (0: none)
    pingMsg,   \* pingMsg[p][b]: the newest ping from p to b in flight
    pongMsg,   \* pongMsg[b][p]: the newest answer from b to p in flight
    data,      \* data[r]: writes reflected in r's state
    committed, \* writes committed so far (history)
    crashes

vars == <<now, clock, view, normal, promise, acks, pingMsg, pongMsg, data,
          committed, crashes>>

NoAcks == [b \in Replicas |-> 0]

Init ==
    /\ now = 0
    /\ clock = [r \in Replicas |-> 1]
    /\ view = [r \in Replicas |-> 0]
    /\ normal = [r \in Replicas |-> TRUE]
    /\ promise = [r \in Replicas |-> 0]
    /\ acks = [p \in Replicas |-> NoAcks]
    /\ pingMsg = [p \in Replicas |-> [b \in Replicas |-> None]]
    /\ pongMsg = [b \in Replicas |-> [p \in Replicas |-> None]]
    /\ data = [r \in Replicas |-> 0]
    /\ committed = 0
    /\ crashes = 0

IsPrimary(p) == normal[p] /\ Primary(view[p]) = p

MayVote(r) == ~PromiseRule \/ clock[r] >= promise[r]

Max(a, b) == IF a > b THEN a ELSE b

\* The lease: a quorum, the primary included, whose other members echoed a
\* ping stamped less than Lease ago on the primary's clock.
LeaseValid(p) ==
    /\ IsPrimary(p)
    /\ \E Q \in Quorums :
        /\ p \in Q
        /\ \A b \in Q \ {p} : acks[p][b] > 0 /\ clock[p] < acks[p][b] + Lease

Tick ==
    /\ now < MaxTime
    /\ now' = now + 1
    /\ \E d \in [Replicas -> MinRate..MaxRate] :
        clock' = [r \in Replicas |-> clock[r] + d[r]]
    /\ UNCHANGED <<view, normal, promise, acks, pingMsg, pongMsg, data,
                   committed, crashes>>

\* A heartbeat: a ping to every backup, stamped with the primary's clock.
SendPings(p) ==
    /\ IsPrimary(p)
    /\ pingMsg' = [pingMsg EXCEPT ![p] = [b \in Replicas |->
                      IF b = p THEN None
                      ELSE [view |-> view[p], stamp |-> clock[p], live |-> TRUE]]]
    /\ UNCHANGED <<now, clock, view, normal, promise, acks, pongMsg, data,
                   committed, crashes>>

\* A ping arrives; a backup normal in its view promises and answers. The
\* answer is delivered at once: a delayed answer carries the same stamp, so
\* its delay only shortens the lease it grants (and pongMsg stays empty).
RecvPing(p, b) ==
    LET m == pingMsg[p][b] IN
    /\ m.live
    /\ pingMsg' = [pingMsg EXCEPT ![p][b] = None]
    /\ IF normal[b] /\ view[b] = m.view /\ Primary(m.view) = p
       THEN /\ promise' = [promise EXCEPT ![b] = Max(@, clock[b] + Promise)]
            /\ acks' = IF IsPrimary(p) /\ view[p] = m.view
                       THEN [acks EXCEPT ![p][b] = Max(@, m.stamp)]
                       ELSE acks
       ELSE UNCHANGED <<promise, acks>>
    /\ UNCHANGED <<now, clock, view, normal, pongMsg, data, committed, crashes>>

\* A write commits once a quorum normal in the primary's view holds it.
Write(p, Q) ==
    /\ IsPrimary(p)
    /\ committed < MaxWrites
    /\ p \in Q
    /\ \A q \in Q : normal[q] /\ view[q] = view[p]
    /\ committed' = committed + 1
    /\ data' = [r \in Replicas |-> IF r \in Q THEN committed + 1 ELSE data[r]]
    /\ UNCHANGED <<now, clock, view, normal, promise, acks, pingMsg, pongMsg,
                   crashes>>

\* A quorum, each member free of promises, forms view v.
ViewChange(Q, v) ==
    /\ v <= MaxView
    /\ Primary(v) \in Q
    /\ \A q \in Q : view[q] < v /\ MayVote(q)
    /\ LET best == CHOOSE x \in {data[q] : q \in Q} : \A q \in Q : x >= data[q] IN
       /\ view' = [r \in Replicas |-> IF r \in Q THEN v ELSE view[r]]
       /\ normal' = [r \in Replicas |-> IF r \in Q THEN TRUE ELSE normal[r]]
       /\ data' = [r \in Replicas |-> IF r \in Q THEN best ELSE data[r]]
       /\ acks' = [r \in Replicas |-> IF r \in Q THEN NoAcks ELSE acks[r]]
    /\ UNCHANGED <<now, clock, promise, pingMsg, pongMsg, committed, crashes>>

\* A member starts a view change it may never finish: a primary stepping
\* down when it hears from no quorum (allowed at any time), or a backup
\* free of promises timing out.
LeaveView(r) ==
    /\ normal[r]
    /\ MayVote(r) \/ IsPrimary(r)
    /\ normal' = [normal EXCEPT ![r] = FALSE]
    /\ acks' = [acks EXCEPT ![r] = NoAcks]
    /\ UNCHANGED <<now, clock, view, promise, pingMsg, pongMsg, data, committed,
                   crashes>>

\* A crash forgets promises: the restarted member abstains for Promise.
Crash(r) ==
    /\ crashes < MaxCrashes
    /\ crashes' = crashes + 1
    /\ normal' = [normal EXCEPT ![r] = FALSE]
    /\ acks' = [acks EXCEPT ![r] = NoAcks]
    /\ promise' = [promise EXCEPT ![r] = IF PromiseRule THEN clock[r] + Promise ELSE 0]
    /\ UNCHANGED <<now, clock, view, pingMsg, pongMsg, data, committed>>

Next ==
    \/ Tick
    \/ \E p \in Replicas : SendPings(p)
    \/ \E p, b \in Replicas : RecvPing(p, b)
    \/ \E p \in Replicas, Q \in Quorums : Write(p, Q)
    \/ \E Q \in Quorums, v \in 1..MaxView : ViewChange(Q, v)
    \/ \E r \in Replicas : LeaveView(r) \/ Crash(r)

Spec == Init /\ [][Next]_vars

\* A read served under the lease sees every committed write.
LeaseReadsAreCurrent ==
    \A p \in Replicas : LeaseValid(p) => data[p] >= committed

\* No newer view is normal anywhere while a primary holds a lease.
LeaseExcludesNewerViews ==
    \A p \in Replicas : LeaseValid(p) =>
        \A r \in Replicas : ~(normal[r] /\ view[r] > view[p])

=============================================================================
