---------------------------- MODULE FuseeOutputs ----------------------------
(***************************************************************************)
(* The output-release rule of a single Fusee shard (local durability).    *)
(*                                                                         *)
(* Records are appended to the log in memory and become durable in        *)
(* batches. Workflow code runs ahead of the disk, but three things wait:   *)
(*  - a step function runs only once every earlier record of its run is   *)
(*    durable (its gate);                                                  *)
(*  - an output (acknowledgment, result, message) leaves the process only *)
(*    once the record behind it is durable;                                *)
(*  - a crash loses everything that was not durable.                       *)
(*                                                                         *)
(* Safety: nothing outside the process (a released output, or an external *)
(* call a step made) depends on a record a crash took back.               *)
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets

CONSTANTS MaxRecords, MaxCrashes

VARIABLES
    log,        \* records in memory, 1..Len(log); record i is the value i
    durable,    \* records 1..durable are on disk
    released,   \* records whose outputs left the process
    effects,    \* records an external call was made after (step inputs)
    crashes

vars == <<log, durable, released, effects, crashes>>

Init ==
    /\ log = <<>>
    /\ durable = 0
    /\ released = {}
    /\ effects = {}
    /\ crashes = 0

\* Code appends a record (a step result, a timer, a start...).
AppendRecord ==
    /\ Len(log) < MaxRecords
    /\ log' = Append(log, Len(log) + 1)
    /\ UNCHANGED <<durable, released, effects, crashes>>

\* One flush covers every record appended so far, or any prefix of them
\* (a flush in flight while more records arrive).
Flush ==
    /\ durable < Len(log)
    /\ \E k \in (durable + 1)..Len(log) : durable' = k
    /\ UNCHANGED <<log, released, effects, crashes>>

\* An output for record i leaves once i is durable.
Release ==
    /\ \E i \in 1..durable :
        /\ i \notin released
        /\ released' = released \cup {i}
    /\ UNCHANGED <<log, durable, effects, crashes>>

\* A step function runs on inputs through record i once i is durable
\* (the gate), and may call an external service.
RunStep ==
    /\ \E i \in 1..durable :
        /\ i \notin effects
        /\ effects' = effects \cup {i}
    /\ UNCHANGED <<log, durable, released, crashes>>

\* A crash: the page cache is lost.
Crash ==
    /\ crashes < MaxCrashes
    /\ log' = SubSeq(log, 1, durable)
    /\ crashes' = crashes + 1
    /\ UNCHANGED <<durable, released, effects>>

Next == AppendRecord \/ Flush \/ Release \/ RunStep \/ Crash

Spec == Init /\ [][Next]_vars

TypeOK ==
    /\ durable \in 0..MaxRecords
    /\ durable <= Len(log)
    /\ released \subseteq 1..MaxRecords
    /\ effects \subseteq 1..MaxRecords

\* Every released output and every external call is backed by a record
\* that is still in the log, after any number of crashes.
NothingLeavesOnStateACrashCanTakeBack ==
    /\ \A i \in released : i <= Len(log)
    /\ \A i \in effects : i <= Len(log)

=============================================================================
