# Changelog

All notable changes are recorded here. Fusee follows semantic versioning. A
release that changes an on-disk format either converts an existing data
directory with a command or refuses to open it with a clear message.

## 1.0.2

- The TypeScript SDK declarations compile in strict consumer projects with library checks enabled.
- The TypeScript and Python starter tests use the test engine’s virtual clock to finish timed workflows without waiting in real time.
- Opening an engine rejects shard counts outside 1–1024. The HTTP API returns a 400 response for malformed call arguments and list filters before dispatching them to the engine.

## 1.0.1

- Clean shutdown checkpoints the last writes and lets busy workflows resume on the next open.
- Backup and recovery keep log segments in order when a spare is used during background preparation. New backups write their manifest before uploading objects so an interrupted first upload can be retried.
- The TypeScript starter and examples use the published SDK, `@bitsinfra/fusee`.

## 1.0.0

The first stable release.

- **Engine.** Durable execution embedded in the application: workflows are
  ordinary async code, every step's result is journaled to a per-shard log,
  one durable write covers every record that arrived meanwhile, and the next
  step starts as soon as its input is durable. Only work that leaves the
  process waits, and only for the records it depends on.
- **SDKs.** Rust, TypeScript (Node.js and Bun), Python, Go and Java, with the
  same operations, argument order and options in each (`api/spec.json`,
  [API at a glance](docs/api.md)). Go and Java embed the engine through the
  C interface in `bindings/c`, which other languages can use too.
- **Durability modes.** `local` (a durable flush on this machine), server
  mode (`fusee server`: the server holds the log and applications lease
  shards), `replicated` (a Viewstamped Replication style protocol with
  protocol-aware recovery, read leases, membership change and hot standby)
  and `memory` for tests.
- **Workflow features.** Retries with backoff, step and workflow timeouts,
  durable sleeps and timers, signals (with start and with timeouts), child
  workflows, `all` and `race`, cancellation with compensation, queries,
  updates and update-with-start, step heartbeats, async completion by token
  or callback URL, continue-as-new, patching and versioning, terminate and
  reset, schedules (cron with time zones, overlap rules and backfill),
  search attributes and filtered listing, concurrency and rate limits with
  priority and fairness, debounce, batching, durable streams, keyed objects,
  read-only steps, payload encryption, large-payload offload, replay testing
  (`fusee export` and a `Replayer`), and workflow ID and child close policies.
- **Running it.** Worker threads in TypeScript, workers in several processes,
  invoked workers that scale to zero (`fusee server --invoke`), TLS 1.3 and
  token authentication, continuous backup and point-in-time restore,
  `fusee reshard`, `fusee disk-check`, a built-in web UI and HTTP API,
  Prometheus metrics and OpenTelemetry traces, a Helm chart, a systemd unit
  and Terraform and monitoring examples (`deploy/`).
- **Testing your workflows.** A test kit for Rust, TypeScript and Python
  with virtual time, step mocks, crash injection and strict replay
  ([Testing your workflows](docs/testing-workflows.md)), and
  `fusee_sim::Scenario` to run
  application workflows under the deterministic simulator.
- **Verification.** A deterministic simulator of the runtime with fault
  injection and checkers, a replication simulator, SIGKILL crash tests on
  real files, cross-SDK conformance (identical journals in every SDK), fuzz
  targets for every decoder, and TLA+ models of the output-release rule and
  the replication protocol. See [How Fusee is tested](docs/testing.md).
