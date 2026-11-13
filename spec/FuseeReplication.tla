-------------------------- MODULE FuseeReplication --------------------------
(***************************************************************************)
(* The safety argument of Fusee's replication (crates/fusee-core/src/vsr). *)
(*                                                                         *)
(* Replicas hold logs of entries <<op, view>>. In view v the primary is    *)
(* the replica chosen by v; it appends entries, and backups copy them.    *)
(* Each replica flushes a prefix of its log (durable). An op is committed *)
(* when a quorum holds it durably in the current view. Outputs are        *)
(* released at the commit position.                                       *)
(*                                                                         *)
(* A view change by a quorum Q picks, among Q's durable logs, the one with *)
(* the highest (lastNormal, length), keeps each member's committed prefix *)
(* and installs the chosen log on every member of Q atomically (the       *)
(* implementation makes the replacement atomic and saves lastNormal only  *)
(* after it is durable). A crash keeps only the durable prefix; a crashed *)
(* primary never leads its view again.                                    *)
(*                                                                         *)
(* Safety: a committed entry is never lost or changed.                     *)
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets

CONSTANTS Replicas, Primary(_), MaxOp, MaxView, MaxCrashes

ASSUME Cardinality(Replicas) \in {3, 5}

Quorums == {Q \in SUBSET Replicas : 2 * Cardinality(Q) > Cardinality(Replicas)}

VARIABLES
    view,        \* view[r]
    normal,      \* normal[r]: TRUE when r is normal in view[r]
    lastNormal,  \* lastNormal[r]
    log,         \* log[r]: sequence of [view |-> v]
    durable,     \* durable[r]: length of the durable prefix
    acked,       \* acked[r]: in view view[r], the durable length r reported
    commit,      \* commit[r]
    committed,   \* history: the entries ever committed, op -> entry
    crashes

vars == <<view, normal, lastNormal, log, durable, acked, commit, committed, crashes>>

Init ==
    /\ view = [r \in Replicas |-> 0]
    /\ normal = [r \in Replicas |-> TRUE]
    /\ lastNormal = [r \in Replicas |-> 0]
    /\ log = [r \in Replicas |-> <<>>]
    /\ durable = [r \in Replicas |-> 0]
    /\ acked = [r \in Replicas |-> 0]
    /\ commit = [r \in Replicas |-> 0]
    /\ committed = <<>>
    /\ crashes = 0

IsPrimary(r) == normal[r] /\ Primary(view[r]) = r

Max(S) == CHOOSE m \in S : \A s \in S : m >= s

Min2(a, b) == IF a < b THEN a ELSE b

\* The primary appends a new entry.
Propose(p) ==
    /\ IsPrimary(p)
    /\ Len(log[p]) < MaxOp
    /\ log' = [log EXCEPT ![p] = Append(@, [view |-> view[p]])]
    /\ UNCHANGED <<view, normal, lastNormal, durable, acked, commit, committed, crashes>>

\* A backup in the primary's view copies one more entry of the primary's log.
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

\* A normal replica reports its durable length to its view's primary.
Ack(r) ==
    /\ normal[r]
    /\ acked' = [acked EXCEPT ![r] = durable[r]]
    /\ UNCHANGED <<view, normal, lastNormal, log, durable, commit, committed, crashes>>

\* The primary commits op k once a quorum in its view holds it durably.
Commit(p, k) ==
    /\ IsPrimary(p)
    /\ k > commit[p]
    /\ k <= Len(log[p])
    /\ \E Q \in Quorums :
        \A q \in Q : /\ view[q] = view[p] /\ normal[q]
                     /\ (IF q = p THEN durable[p] ELSE acked[q]) >= k
    /\ commit' = [commit EXCEPT ![p] = k]
    /\ committed' = [i \in 1..Max({Len(committed), k}) |->
                        IF i <= Len(committed) THEN committed[i] ELSE log[p][i]]
    /\ UNCHANGED <<view, normal, lastNormal, log, durable, acked, crashes>>

\* A backup learns the commit position from the primary.
LearnCommit(b, p) ==
    /\ IsPrimary(p)
    /\ normal[b] /\ view[b] = view[p]
    /\ commit[b] < commit[p]
    /\ Len(log[b]) >= commit[p]
    /\ commit' = [commit EXCEPT ![b] = commit[p]]
    /\ UNCHANGED <<view, normal, lastNormal, log, durable, acked, committed, crashes>>

\* A quorum Q changes to view v. Logs are judged by their durable prefix
\* (what a member could still vouch for after a crash).
ViewChange(Q, v) ==
    /\ v <= MaxView
    /\ \A q \in Q : view[q] < v
    /\ Primary(v) \in Q
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

\* A crash keeps the durable prefix. The replica rejoins as not normal
\* (a restarted primary never leads its view again; a restarted backup
\* must catch up before it is trusted).
Crash(r) ==
    /\ crashes < MaxCrashes
    /\ log' = [log EXCEPT ![r] = SubSeq(@, 1, durable[r])]
    /\ normal' = [normal EXCEPT ![r] = FALSE]
    /\ acked' = [acked EXCEPT ![r] = 0]
    /\ commit' = [commit EXCEPT ![r] = Min2(@, durable[r])]
    /\ crashes' = crashes + 1
    /\ UNCHANGED <<view, lastNormal, durable, committed>>

Next ==
    \/ \E p \in Replicas : Propose(p)
    \/ \E b, p \in Replicas : Replicate(b, p)
    \/ \E r \in Replicas : Flush(r) \/ Ack(r) \/ Crash(r)
    \/ \E p \in Replicas, k \in 1..MaxOp : Commit(p, k)
    \/ \E b, p \in Replicas : LearnCommit(b, p)
    \/ \E Q \in Quorums, v \in 1..MaxView : ViewChange(Q, v)

Spec == Init /\ [][Next]_vars

\* Every replica's committed prefix is the committed history.
CommittedNeverChanges ==
    \A r \in Replicas :
        /\ commit[r] <= Len(log[r])
        /\ \A i \in 1..commit[r] : i <= Len(committed) /\ log[r][i] = committed[i]

\* Some quorum always still holds every committed entry durably, so no
\* future view change can lose it.
CommittedSurvives ==
    \A i \in 1..Len(committed) :
        \E Q \in Quorums : \A q \in Q :
            /\ durable[q] >= i
            /\ log[q][i] = committed[i]

=============================================================================
