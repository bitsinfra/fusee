# Guarantees

Stated exactly as they are tested (see [testing.md](testing.md)).

1. **An acknowledged start, signal or step result is never lost** within the
   durability mode's failure model. "Acknowledged" means the call returned
   (the promise resolved, the `await` completed).
2. **A recorded step result never changes**, and code after it sees the same
   value on every run.
3. **A step's function runs at least once.** It runs again only if the
   process failed before that step's result became durable. `ctx.key()`
   returns a stable workflow-and-step key; passing it to a service that
   accepts idempotency keys makes the step's effect exactly once. A step
   marked [read-only](features.md#read-only-steps) may also run again with
   different inputs, if the process failed before the records ahead of it
   were durable; mark only steps that change nothing outside the process.
4. **Nothing leaves the process based on state a crash could take back.**
   Acknowledgments, results returned to callers, refusals (for example "the
   workflow has already finished"), messages to other workflows and the
   inputs a step function sees are all released only after the records
   behind them are durable. The one exception is declared by the code: a
   read-only step sees its inputs at once, because it has no effect for a
   crash to contradict.

Fusee does not weaken these guarantees anywhere. Speculating through
external calls with rollback is not part of Fusee.

## Failure models

| Mode | Survives | Does not survive |
| --- | --- | --- |
| `local` | process crash (SIGKILL, panic, OOM kill); power loss, if the drive honors flushes | loss of the machine or its disk; a drive that acknowledges flushes it has not done |
| server | application crash; failover to a standby application | loss of the server's disk |
| `replicated` | loss of a minority of replicas | loss of a majority |
| `memory` | nothing | anything |

**Drives.** Consumer drives without power-loss protection, and macOS
without `F_FULLFSYNC`, can acknowledge data that is not on stable media. On
macOS Fusee uses `F_FULLFSYNC`, the only flush macOS guarantees reaches the
media; the `fsync` flush option exists for development and is labeled
non-durable everywhere it appears. `fusee disk-check <dir>` shows the flush
latency of the disk under a directory.

**Damaged logs.** Recovery distinguishes a torn, never-acknowledged write at
the end of the log (discarded) from damage to acknowledged records (refused
with an error naming the record, never silently truncated). In replicated
mode a replica repairs damaged records from its peers.
