---- MODULE MCReconfig ----
EXTENDS FuseeReconfig
CONSTANTS r1, r2, r3, r4
\* Epoch 0: {r1, r2, r3}, primaries round-robin; epoch 1 replaces r3 with
\* the new member r4 (its first view is led by r1, a continuing member).
Old == {r1, r2, r3}
New == {r1, r2, r4}
OldPrimary(v) == CASE v % 3 = 0 -> r1 [] v % 3 = 1 -> r2 [] OTHER -> r3
NewPrimary(v) == CASE (v - 2) % 3 = 0 -> r1 [] (v - 2) % 3 = 1 -> r2 [] OTHER -> r4
\* Epoch 1 grows the group to four: {r1, r2, r3, r4}.
Grown == {r1, r2, r3, r4}
GrownPrimary(v) == CASE (v - 2) % 4 = 0 -> r1 [] (v - 2) % 4 = 1 -> r2
                   [] (v - 2) % 4 = 2 -> r3 [] OTHER -> r4
====
