--------------------------- MODULE FuseeCheckpoint ---------------------------
EXTENDS Naturals, FiniteSets

(***************************************************************************)
(* One replacement of an existing durable generation. The old image and    *)
(* suffix include every acknowledged position. A candidate image can come  *)
(* in pages from another member, but its digest and anchor are fixed by an  *)
(* eligible donor. The last suffix position includes both learned commits  *)
(* and prepares this member already acknowledged in the current view.      *)
(* A crash may retain either directory entry until the parent is synced.   *)
(***************************************************************************)
CONSTANT Pages, AckedThrough
VARIABLES page, good, donor, verified, suffix, filesSynced, pointer,
          pointerSynced, oldPresent
vars == <<page, good, donor, verified, suffix, filesSynced, pointer,
          pointerSynced, oldPresent>>

Init == /\ page = 0 /\ good = TRUE /\ donor = FALSE /\ verified = FALSE
        /\ suffix = 0 /\ filesSynced = FALSE /\ pointer = "old"
        /\ pointerSynced = FALSE /\ oldPresent = TRUE

Receive ==
    /\ pointer = "old" /\ ~verified /\ page < Pages
    /\ page' = page + 1
    /\ good' \in {FALSE, good}
    /\ UNCHANGED <<donor, verified, suffix, filesSynced, pointer,
                   pointerSynced, oldPresent>>

ChangeDonor ==
    /\ pointer = "old" /\ donor' = ~donor
    /\ page' = 0 /\ good' = TRUE /\ verified' = FALSE /\ suffix' = 0
    /\ filesSynced' = FALSE
    /\ UNCHANGED <<pointer, pointerSynced, oldPresent>>

Verify ==
    /\ donor /\ page = Pages /\ good /\ pointer = "old"
    /\ verified' = TRUE
    /\ UNCHANGED <<page,good,donor,suffix,filesSynced,pointer,pointerSynced,oldPresent>>

ReceiveSuffix ==
    /\ pointer = "old" /\ verified /\ suffix < AckedThrough
    /\ suffix' = suffix + 1
    /\ UNCHANGED <<page,good,donor,verified,filesSynced,pointer,pointerSynced,oldPresent>>

SyncFiles ==
    /\ pointer = "old" /\ donor /\ verified /\ suffix = AckedThrough
    /\ filesSynced' = TRUE
    /\ UNCHANGED <<page,good,donor,verified,suffix,pointer,pointerSynced,oldPresent>>

RenamePointer ==
    /\ pointer = "old" /\ filesSynced /\ donor
    /\ pointer' = "new"
    /\ UNCHANGED <<page,good,donor,verified,suffix,filesSynced,pointerSynced,oldPresent>>

SyncDirectory ==
    /\ pointer = "new" /\ pointerSynced' = TRUE
    /\ UNCHANGED <<page,good,donor,verified,suffix,filesSynced,pointer,oldPresent>>

Retire ==
    /\ pointerSynced /\ oldPresent' = FALSE
    /\ UNCHANGED <<page,good,donor,verified,suffix,filesSynced,pointer,pointerSynced>>

Crash ==
    /\ pointer' \in IF pointerSynced THEN {"new"} ELSE {"old",pointer}
    /\ pointerSynced' = (pointer' = "new")
    /\ oldPresent' = oldPresent
    /\ IF pointer' = "old"
       THEN /\ page' = 0 /\ good' = TRUE /\ donor' = FALSE
            /\ verified' = FALSE /\ suffix' = 0 /\ filesSynced' = FALSE
       ELSE UNCHANGED <<page,good,donor,verified,suffix,filesSynced>>

Next == Receive \/ ChangeDonor \/ Verify \/ ReceiveSuffix \/ SyncFiles \/
        RenamePointer \/ SyncDirectory \/ Retire \/ Crash
Spec == Init /\ [][Next]_vars
CanRecover == IF pointer = "old" THEN oldPresent
              ELSE donor /\ verified /\ good /\ page = Pages /\ filesSynced /\ suffix = AckedThrough
PrefixReclaimSafe == ~oldPresent => pointerSynced /\ pointer = "new"
=============================================================================
