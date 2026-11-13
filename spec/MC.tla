---- MODULE MC ----
EXTENDS FuseeReplication
CONSTANTS r1, r2, r3
\* Round-robin primaries over three replicas.
PrimaryOf(v) == CASE v % 3 = 0 -> r1 [] v % 3 = 1 -> r2 [] OTHER -> r3

\* Witness (MC-witness.cfg, expected to be violated): a primary never
\* commits past its own durable prefix. Commit is on any durable quorum,
\* so a counterexample is a primary releasing entries only its backups
\* hold durably; MC.cfg checks that the safety invariants hold there too.
PrimaryCommitsOnlyWhatItHolds ==
    \A p \in Replicas : IsPrimary(p) => commit[p] <= durable[p]
====
