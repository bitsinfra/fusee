# Examples

Five small applications, each in Python, TypeScript, Rust, Go and Java. They
print the same results in every language, and `scripts/check_examples.sh`
runs all twenty-five in CI.

| | Python | TypeScript | Rust | Go | Java |
| --- | --- | --- | --- | --- | --- |
| Payment saga: reserve, charge, wait for shipping; compensations undo on failure | [py](python/payment_saga.py) | [ts](typescript/payment-saga.mjs) | [rs](../crates/fusee/examples/payment_saga.rs) | [go](go/payment_saga/main.go) | [java](java/PaymentSaga.java) |
| Agent loop: model and tool calls, each a step | [py](python/agent_loop.py) | [ts](typescript/agent-loop.mjs) | [rs](../crates/fusee/examples/agent_loop.rs) | [go](go/agent_loop/main.go) | [java](java/AgentLoop.java) |
| Scheduled job: a schedule starts a report each second (a cron expression in a real job) | [py](python/scheduled_job.py) | [ts](typescript/scheduled-job.mjs) | [rs](../crates/fusee/examples/scheduled_job.rs) | [go](go/scheduled_job/main.go) | [java](java/ScheduledJob.java) |
| Human approval: ask, wait up to three days for a decision, act on it | [py](python/human_approval.py) | [ts](typescript/human-approval.mjs) | [rs](../crates/fusee/examples/human_approval.rs) | [go](go/human_approval/main.go) | [java](java/HumanApproval.java) |
| Cart object: one object per user, calls that take turns, a repeated call that runs once | [py](python/cart_object.py) | [ts](typescript/cart-object.mjs) | [rs](../crates/fusee/examples/cart_object.rs) | [go](go/cart_object/main.go) | [java](java/CartObject.java) |

Each keeps its data in `./data/<example>` under the directory it runs
from, and its workflow IDs attach to an earlier run (`use-existing`), so
running one again, or killing it midway and running it again, continues
where it was.

Run one:

```sh
PYTHONPATH=bindings/python/python python3 examples/python/payment_saga.py
cargo run --release -p fusee --example payment_saga
(cd examples/go && go run ./payment_saga)
java -cp bindings/java/target/classes examples/java/PaymentSaga.java
```

The TypeScript examples import `fusee`: run them from a project where it is
installed (`npm install fusee`), as `scripts/check_examples.sh` arranges.
`invoked_worker.py` and `invoked-worker.mjs` show a worker the server
starts on demand ([server mode](../docs/server-mode.md)).
