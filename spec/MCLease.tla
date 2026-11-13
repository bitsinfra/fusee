---- MODULE MCLease ----
EXTENDS FuseeLease
CONSTANTS r1, r2, r3
\* Round-robin primaries over three replicas.
RoundRobin == <<r1, r2, r3>>
====
