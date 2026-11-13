------------------------------ MODULE FuseeLog ------------------------------
EXTENDS Naturals, FiniteSets, Sequences

(***************************************************************************)
(* The acceptance rule of log format 3 (crates/fusee-core/src/wal.rs).     *)
(*                                                                         *)
(* A writer appends batches with up to K writes in flight. The device may  *)
(* complete them in any order; the writer acknowledges a batch only when   *)
(* it and every batch before it completed. Each batch carries:             *)
(*   - its sequence number;                                                *)
(*   - the identity of the batch before it (the chained checksum);         *)
(*   - acked: the highest sequence the writer had acknowledged when it     *)
(*     submitted the batch (acked_through).                                *)
(* A batch's identity is <<life, seq>>: every open of the log is a new     *)
(* life, and the life is part of every batch it writes, so two lives never *)
(* write identical bytes and a checksum never matches a stale batch.       *)
(*                                                                         *)
(* A crash keeps, loses or tears each write in flight. Recovery accepts    *)
(* batches from the start while each has the expected sequence and names  *)
(* the accepted batch before it; torn bytes never validate (CRC). A valid  *)
(* batch after the end whose acked reaches past the end proves that an     *)
(* acknowledged batch was lost: recovery refuses to open. The next life    *)
(* resumes in place, overwriting from the slot after the end, so stale     *)
(* batches of older lives stay on disk beyond and between its writes.      *)
(*                                                                         *)
(* Slots are indexed by sequence. That is the worst case for stale bytes:  *)
(* a stale batch with the expected sequence always sits where recovery     *)
(* looks next, so only the chain can reject it. Recycled segments are not  *)
(* modeled: their checksums are seeded with a salt that is new for every   *)
(* segment life, so their old bytes never validate.                        *)
(***************************************************************************)

CONSTANTS MaxSeq, K, MaxLives,
          ChainCheck,   \* FALSE: accept by sequence only (negative control)
          RelativeAck   \* FALSE: any valid later batch means damage (control)

Genesis == <<0, 0>>
\* Every slot value has one shape so TLC can compare them.
Empty == [kind |-> "empty", life |-> 0, seq |-> 0, prev |-> Genesis, acked |-> 0]
Torn == [kind |-> "torn", life |-> 0, seq |-> 0, prev |-> Genesis, acked |-> 0]

VARIABLES
    disk,       \* slot -> Empty, Torn or a batch
    life,       \* the current writer's life
    start,      \* first sequence written by the current life
    next,       \* next sequence to submit
    last,       \* identity of the newest submitted batch
    inflight,   \* batches submitted and not completed
    acked,      \* highest sequence acknowledged by the current life
    ackedIds,   \* every batch identity ever acknowledged (ghost)
    lost,       \* recovery accepted a log missing an acknowledged batch (ghost)
    refused,    \* recovery reported damage (ghost)
    mixed       \* recovery accepted an older life's batch after a newer one's (ghost)

vars == <<disk, life, start, next, last, inflight, acked, ackedIds, lost,
          refused, mixed>>

Slots == 1..MaxSeq
Id(b) == <<b.life, b.seq>>
IsBatch(x) == x.kind = "batch"

Init ==
    /\ disk = [s \in Slots |-> Empty]
    /\ life = 1
    /\ start = 1
    /\ next = 1
    /\ last = Genesis
    /\ inflight = {}
    /\ acked = 0
    /\ ackedIds = {}
    /\ lost = FALSE
    /\ refused = FALSE
    /\ mixed = FALSE

Submit ==
    /\ next <= MaxSeq
    /\ Cardinality(inflight) < K
    /\ LET b == [kind |-> "batch", life |-> life, seq |-> next, prev |-> last,
                 acked |-> acked]
       IN /\ inflight' = inflight \cup {b}
          /\ last' = Id(b)
    /\ next' = next + 1
    /\ UNCHANGED <<disk, life, start, acked, ackedIds, lost, refused, mixed>>

\* The highest sequence s such that every batch of this life up to s is on
\* disk; batches before the life's start were recovered and are durable.
AckedThrough(d) ==
    LET ok(s) == \A t \in start..s : IsBatch(d[t]) /\ d[t].life = life
        cands == {s \in (start - 1)..(next - 1) : s < start \/ ok(s)}
    IN CHOOSE s \in cands : \A u \in cands : u <= s

Complete ==
    \E b \in inflight :
        LET d == [disk EXCEPT ![b.seq] = b]
            a == AckedThrough(d)
        IN /\ disk' = d
           /\ inflight' = inflight \ {b}
           /\ acked' = IF a > acked THEN a ELSE acked
           /\ ackedIds' = ackedIds \cup {Id(d[s]) : s \in (acked + 1)..a}
           /\ UNCHANGED <<life, start, next, last, lost, refused, mixed>>

\* Whether slot s continues the chain accepted through slot s - 1.
Valid(d, s) ==
    /\ IsBatch(d[s])
    /\ d[s].seq = s
    /\ \/ ~ChainCheck
       \/ d[s].prev = IF s = 1 THEN Genesis ELSE Id(d[s - 1])

\* End of the accepted chain.
ChainEnd(d) ==
    LET good(e) == \A s \in 1..e : Valid(d, s)
    IN CHOOSE e \in 0..MaxSeq : good(e) /\ (e = MaxSeq \/ ~Valid(d, e + 1))

\* A valid-looking batch after the end proves an acknowledged batch was lost.
Damaged(d, e) ==
    \E s \in Slots :
        /\ s > e + 1
        /\ IsBatch(d[s])
        /\ IF RelativeAck THEN d[s].acked >= e + 1 ELSE TRUE

\* Each write in flight lands, is lost (the slot keeps what it held) or tears.
Outcomes == [inflight -> {"land", "lose", "tear"}]
AfterCrash(o) ==
    [s \in Slots |->
        IF \E b \in inflight : b.seq = s
        THEN LET b == CHOOSE x \in inflight : x.seq = s
             IN CASE o[b] = "land" -> b
                  [] o[b] = "lose" -> disk[s]
                  [] OTHER -> Torn
        ELSE disk[s]]

CrashAndRecover ==
    /\ life < MaxLives
    /\ \E o \in Outcomes :
        LET d == AfterCrash(o)
            e == ChainEnd(d)
        IN /\ disk' = d
           /\ refused' = (refused \/ Damaged(d, e))
           /\ lost' = (lost \/ \E a \in ackedIds :
                            a[2] > e \/ ~IsBatch(d[a[2]]) \/ Id(d[a[2]]) # a)
           /\ mixed' = (mixed \/ \E s \in 2..e : d[s].life < d[s - 1].life)
           \* Resume in place after the accepted chain.
           /\ life' = life + 1
           /\ start' = e + 1
           /\ next' = e + 1
           /\ last' = IF e = 0 THEN Genesis ELSE Id(d[e])
           /\ inflight' = {}
           /\ acked' = e
           \* Everything recovery accepted is durable once it is synced.
           /\ ackedIds' = ackedIds \cup {Id(d[s]) : s \in 1..e}

Next == Submit \/ Complete \/ CrashAndRecover

Spec == Init /\ [][Next]_vars

\* An acknowledged batch is never missing from what recovery accepts.
NoLostAck == ~lost
\* A crash alone never looks like damage.
NoFalseDamage == ~refused
\* Recovery never splices a stale batch of an older life after a newer one.
NoStaleSplice == ~mixed
=============================================================================
