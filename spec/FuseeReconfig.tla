---------------------------- MODULE FuseeReconfig ----------------------------
(***************************************************************************)
(* Membership change in Fusee's replication (crates/fusee-core/src/vsr.rs) *)
(* on top of the protocol of FuseeReplication.tla.                        *)
(*                                                                         *)
(* Epoch 0 has members C0, epoch 1 members C1; views below E1 belong to   *)
(* epoch 0, the rest to epoch 1, and each epoch has its own primaries.     *)
(* A primary of epoch 0 may propose one reconfiguration entry (rc); it    *)
(* proposes nothing after it (the seal rule). Members of C1 that hold the *)
(* committed rc entry form epoch 1's first view from a quorum of C1 (the  *)
(* hold rule); a member of C1 without it (a new member, or one that       *)
(* lagged) first copies the committed prefix through rc from any replica  *)
(* that holds it, or takes the committed state of an epoch-1 primary.     *)
(* Members of C0 not in C1 retire. Crashes keep the durable prefix, as in *)
(* FuseeReplication.tla.                                                  *)
(*                                                                         *)
(* Safety: a committed entry never changes, and a quorum of the newest    *)
(* epoch that has a normal member holds every committed entry durably.    *)
(* Dropping the seal rule (SealRule = FALSE) or the hold rule             *)
(* (HoldRule = FALSE) breaks it.                                          *)
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets

CONSTANTS Replicas, C0, C1, P0(_), P1(_), E1, MaxOp, MaxView, MaxCrashes,
          SealRule, HoldRule

ASSUME C0 \subseteq Replicas /\ C1 \subseteq Replicas /\ 0 < E1 /\ E1 <= MaxView

EpochOf(v) == IF v >= E1 THEN 1 ELSE 0
Config(e) == IF e = 0 THEN C0 ELSE C1
Primary(v) == IF v >= E1 THEN P1(v) ELSE P0(v)
Quorums(e) == {Q \in SUBSET Config(e) : 2 * Cardinality(Q) > Cardinality(Config(e))}

VARIABLES view, normal, lastNormal, log, durable, acked, commit, committed, crashes

vars == <<view, normal, lastNormal, log, durable, acked, commit, committed, crashes>>

Init ==
    /\ view = [r \in Replicas |-> 0]
    \* New members (not in C0) start outside the group.
    /\ normal = [r \in Replicas |-> r \in C0]
    /\ lastNormal = [r \in Replicas |-> 0]
    /\ log = [r \in Replicas |-> <<>>]
    /\ durable = [r \in Replicas |-> 0]
    /\ acked = [r \in Replicas |-> 0]
    /\ commit = [r \in Replicas |-> 0]
    /\ committed = <<>>
    /\ crashes = 0

Max(S) == CHOOSE m \in S : \A s \in S : m >= s
Min2(a, b) == IF a < b THEN a ELSE b

Epoch(r) == EpochOf(view[r])
IsPrimary(p) == normal[p] /\ Primary(view[p]) = p /\ p \in Config(Epoch(p))
Sealed(r) == \E i \in 1..Len(log[r]) : log[r][i].rc
\* r knows the reconfiguration committed and holds it durably: the rc
\* entry is in its committed, durable prefix. (The implementation votes
\* only after its view save, a barrier that makes its whole log durable.)
Held(r) == IF commit[r] < durable[r] THEN commit[r] ELSE durable[r]
KnowsRc(r) == \E i \in 1..Held(r) : log[r][i].rc
RcIndex(r) == CHOOSE i \in 1..Held(r) : log[r][i].rc

Propose(p) ==
    /\ IsPrimary(p)
    /\ Len(log[p]) < MaxOp
    /\ SealRule => ~Sealed(p)
    /\ log' = [log EXCEPT ![p] = Append(@, [view |-> view[p], rc |-> FALSE])]
    /\ UNCHANGED <<view, normal, lastNormal, durable, acked, commit, committed, crashes>>

ProposeRc(p) ==
    /\ IsPrimary(p) /\ Epoch(p) = 0
    /\ Len(log[p]) < MaxOp
    /\ ~Sealed(p)
    /\ log' = [log EXCEPT ![p] = Append(@, [view |-> view[p], rc |-> TRUE])]
    /\ UNCHANGED <<view, normal, lastNormal, durable, acked, commit, committed, crashes>>

Replicate(b, p) ==
    /\ b /= p
    /\ IsPrimary(p)
    /\ normal[b] /\ view[b] = view[p]
    /\ Len(log[b]) < Len(log[p])
    /\ SubSeq(log[p], 1, Len(log[b])) = log[b]
    /\ log' = [log EXCEPT ![b] = Append(@, log[p][Len(log[b]) + 1])]
    /\ UNCHANGED <<view, normal, lastNormal, durable, acked, commit, committed, crashes>>

Flush(r) ==
    /\ durable[r] < Len(log[r])
    /\ durable' = [durable EXCEPT ![r] = Len(log[r])]
    /\ UNCHANGED <<view, normal, lastNormal, log, acked, commit, committed, crashes>>

Ack(r) ==
    /\ normal[r]
    /\ acked' = [acked EXCEPT ![r] = durable[r]]
    /\ UNCHANGED <<view, normal, lastNormal, log, durable, commit, committed, crashes>>

Commit(p, k) ==
    /\ IsPrimary(p)
    /\ k > commit[p]
    /\ k <= Len(log[p])
    /\ \E Q \in Quorums(Epoch(p)) :
        \A q \in Q : /\ view[q] = view[p] /\ normal[q]
                     /\ (IF q = p THEN durable[p] ELSE acked[q]) >= k
    /\ commit' = [commit EXCEPT ![p] = k]
    /\ committed' = [i \in 1..Max({Len(committed), k}) |->
                        IF i <= Len(committed) THEN committed[i] ELSE log[p][i]]
    /\ UNCHANGED <<view, normal, lastNormal, log, durable, acked, crashes>>

LearnCommit(b, p) ==
    /\ IsPrimary(p)
    /\ normal[b] /\ view[b] = view[p]
    /\ commit[b] < commit[p]
    /\ Len(log[b]) >= commit[p]
    /\ SubSeq(log[b], 1, commit[p]) = SubSeq(log[p], 1, commit[p])
    /\ commit' = [commit EXCEPT ![b] = commit[p]]
    /\ UNCHANGED <<view, normal, lastNormal, log, durable, acked, committed, crashes>>

\* Voting in view v: within one epoch, or into epoch 1 once it may start
\* (the voter knows the rc entry committed; without the hold rule, once
\* any replica does).
MayVoteIn(q, v) ==
    IF EpochOf(v) = 0
    THEN Epoch(q) = 0
    ELSE /\ q \in C1
         /\ IF HoldRule THEN KnowsRc(q) ELSE \E r \in Replicas : KnowsRc(r)

\* A view change inside one epoch, among that epoch's members (as in
\* FuseeReplication.tla). Epoch 1's first view is ViewChange(Q, E1): the
\* hold rule makes every voter hold the committed rc entry.
ViewChange(Q, v) ==
    /\ v <= MaxView
    /\ Q \in Quorums(EpochOf(v))
    /\ Primary(v) \in Q
    /\ \A q \in Q : view[q] < v /\ MayVoteIn(q, v)
    /\ LET dlog(q) == SubSeq(log[q], 1, durable[q])
           best == CHOOSE b \in Q :
                     \A q \in Q : \/ lastNormal[b] > lastNormal[q]
                                  \/ /\ lastNormal[b] = lastNormal[q]
                                     /\ durable[b] >= durable[q]
           newLog == dlog(best)
           maxCommit == Max({commit[q] : q \in Q})
       IN
       /\ view' = [r \in Replicas |-> IF r \in Q THEN v ELSE view[r]]
       /\ normal' = [r \in Replicas |-> IF r \in Q THEN TRUE ELSE normal[r]]
       /\ lastNormal' = [r \in Replicas |-> IF r \in Q THEN v ELSE lastNormal[r]]
       /\ log' = [r \in Replicas |-> IF r \in Q THEN newLog ELSE log[r]]
       /\ durable' = [r \in Replicas |-> IF r \in Q THEN Len(newLog) ELSE durable[r]]
       /\ acked' = [r \in Replicas |-> IF r \in Q THEN 0 ELSE acked[r]]
       /\ commit' = [r \in Replicas |-> IF r \in Q THEN Min2(maxCommit, Len(newLog)) ELSE commit[r]]
    /\ UNCHANGED <<committed, crashes>>

\* A member of C1 whose log lacks the rc entry copies the committed prefix
\* through it from a replica that knows it committed, replacing what
\* follows its own commit, and enters epoch 1 (not yet normal: it votes
\* from now on, or follows a primary).
CatchUp(j, d) ==
    /\ j \in C1 /\ KnowsRc(d)
    /\ ~\E i \in 1..Len(log[j]) : log[j][i].rc
    /\ LET k == RcIndex(d) IN
       /\ log' = [log EXCEPT ![j] = SubSeq(log[d], 1, k)]
       \* Its committed prefix stays durable; the copy once it flushes.
       /\ durable' = [durable EXCEPT ![j] = Min2(durable[j], commit[j])]
       /\ commit' = [commit EXCEPT ![j] = k]
    /\ view' = [view EXCEPT ![j] = Max({view[j], E1})]
    /\ normal' = [normal EXCEPT ![j] = FALSE]
    /\ UNCHANGED <<lastNormal, acked, committed, crashes>>

\* A member of epoch 1 behind its primary takes the primary's committed
\* state and becomes normal in its view (the state transfer).
Transfer(b, p) ==
    /\ b /= p /\ b \in C1
    /\ IsPrimary(p) /\ Epoch(p) = 1
    /\ view[b] <= view[p]
    /\ ~(normal[b] /\ view[b] = view[p])
    /\ log' = [log EXCEPT ![b] = SubSeq(log[p], 1, commit[p])]
    /\ durable' = [durable EXCEPT ![b] = commit[p]]
    /\ commit' = [commit EXCEPT ![b] = commit[p]]
    /\ view' = [view EXCEPT ![b] = view[p]]
    /\ normal' = [normal EXCEPT ![b] = TRUE]
    /\ lastNormal' = [lastNormal EXCEPT ![b] = view[p]]
    /\ acked' = [acked EXCEPT ![b] = 0]
    /\ UNCHANGED <<committed, crashes>>

\* A crash keeps the durable prefix.
Crash(r) ==
    /\ crashes < MaxCrashes
    /\ log' = [log EXCEPT ![r] = SubSeq(@, 1, durable[r])]
    /\ normal' = [normal EXCEPT ![r] = FALSE]
    /\ acked' = [acked EXCEPT ![r] = 0]
    /\ commit' = [commit EXCEPT ![r] = Min2(@, durable[r])]
    /\ crashes' = crashes + 1
    /\ UNCHANGED <<view, lastNormal, durable, committed>>

Next ==
    \/ \E p \in Replicas : Propose(p) \/ ProposeRc(p)
    \/ \E b, p \in Replicas : Replicate(b, p) \/ LearnCommit(b, p) \/ Transfer(b, p)
    \/ \E r \in Replicas : Flush(r) \/ Ack(r) \/ Crash(r)
    \/ \E p \in Replicas, k \in 1..MaxOp : Commit(p, k)
    \/ \E v \in 1..MaxView : \E Q \in Quorums(EpochOf(v)) : ViewChange(Q, v)
    \/ \E j, d \in Replicas : CatchUp(j, d)

Spec == Init /\ [][Next]_vars

\* Members still taking part: in C0 during epoch 0, in C1 during epoch 1.
Active(r) == r \in Config(Epoch(r))

CommittedNeverChanges ==
    \A r \in Replicas : Active(r) =>
        /\ commit[r] <= Len(log[r])
        /\ \A i \in 1..commit[r] : i <= Len(committed) /\ log[r][i] = committed[i]

\* The newest epoch with a normal member keeps a quorum holding every
\* committed entry durably, so no later view change in it can lose one.
Newest == IF \E r \in Replicas : normal[r] /\ Epoch(r) = 1 /\ r \in C1 THEN 1 ELSE 0

CommittedSurvives ==
    \A i \in 1..Len(committed) :
        \E Q \in Quorums(Newest) : \A q \in Q :
            /\ durable[q] >= i
            /\ log[q][i] = committed[i]

=============================================================================
